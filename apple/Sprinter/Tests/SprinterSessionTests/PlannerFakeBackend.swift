import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE3.2's planner view-model tests.
///
/// It supports BOTH surfaces the planner touches, without a daemon or network:
///
/// - The **session channel** — it vends a fresh live ``SessionEvent`` feed on each
///   `sessionEvents` call (modelling BE1's per-(re)start feed), keeping the latest
///   continuation so a test can `emit`/`finish`/`fail` the current feed, so the
///   reused ``SessionViewModel``'s transcript builds live.
/// - **`createWorkstreamFromPlan`** — it records every ``WorkstreamPlan`` it
///   observes on an inspectable stream (so a test can assert the exact plan was
///   submitted) and resolves the configured `materializeResult`: the new
///   ``WorkstreamId`` on `.success`, or a thrown ``ContractError`` on `.failure`
///   (a `.planRejected` drives the rejection path).
final class PlannerFakeBackend: Backend {
  private let knownSession: SessionId
  private let materializeResult: Result<WorkstreamId, ContractError>

  /// When `true`, `createWorkstreamFromPlan` suspends after recording the plan until
  /// ``releaseGate()`` — so a test can hold one materialize in flight and prove a
  /// concurrent second call is a no-op (the re-entrancy guard).
  private let gated: Bool

  /// Lock-guarded mutable feed state — the latest vended feed's continuation and
  /// the count of feeds vended (a restart re-subscribes, so this increments).
  private let feed = Locked(FeedState())

  /// The gate continuation (set while a gated call is suspended) + whether it has
  /// been opened, so ``releaseGate()`` before the call suspends still lets it through.
  private let gate = Locked(GateState())

  /// How many times `createWorkstreamFromPlan` was actually invoked — a re-entrant
  /// `materialize` must NOT increment this (its guard returns before the port call).
  private let submissions = Locked(0)

  /// Every ``WorkstreamPlan`` submitted through `createWorkstreamFromPlan`, in order.
  let submittedPlans: AsyncStream<WorkstreamPlan>
  private let submittedPlansContinuation: AsyncStream<WorkstreamPlan>.Continuation

  init(
    knownSession: SessionId, materializeResult: Result<WorkstreamId, ContractError>,
    gated: Bool = false
  ) {
    self.knownSession = knownSession
    self.materializeResult = materializeResult
    self.gated = gated
    (submittedPlans, submittedPlansContinuation) = AsyncStream.makeStream()
    // Prepare the first feed eagerly so its buffer is live before the model's feed
    // task subscribes — an `emit` right after `start()` is buffered, not lost.
    let (stream, continuation) = AsyncThrowingStream<SessionEvent, any Error>.makeStream()
    feed.withLock {
      $0.pending = stream
      $0.continuation = continuation
    }
  }

  /// The number of live feeds vended so far (one per `start`; a restart adds one).
  var feedCount: Int { feed.withLock { $0.count } }

  /// How many times `createWorkstreamFromPlan` reached the port (see ``submissions``).
  var submissionCount: Int { submissions.withLock { $0 } }

  /// Releases a gated `createWorkstreamFromPlan` so it resolves its result. Safe to
  /// call before the call suspends — it marks the gate open and the call sails through.
  func releaseGate() {
    let waiter = gate.withLock { state -> CheckedContinuation<Void, Never>? in
      state.isOpen = true
      defer { state.continuation = nil }
      return state.continuation
    }
    waiter?.resume()
  }

  /// Raises one event on the current live feed.
  func emit(_ event: SessionEvent) {
    feed.withLock { _ = $0.continuation?.yield(event) }
  }

  /// Ends the current live feed cleanly (the session ended).
  func finish() {
    feed.withLock { $0.continuation?.finish() }
  }

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    submissions.withLock { $0 += 1 }
    submittedPlansContinuation.yield(plan)
    if gated {
      await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
        let alreadyOpen = gate.withLock { state -> Bool in
          if state.isOpen { return true }
          state.continuation = cont
          return false
        }
        if alreadyOpen { cont.resume() }
      }
    }
    switch materializeResult {
    case .success(let id):
      return id
    case .failure(let error):
      throw error
    }
  }

  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    guard sessionId == knownSession else {
      return AsyncThrowingStream {
        $0.finish(throwing: ContractError.sessionNotFound(id: sessionId))
      }
    }
    return feed.withLock { state in
      state.count += 1
      // Vend the eagerly-prepared feed on the first subscribe; a restart
      // (re-subscribe) gets a FRESH stream, modelling BE1's per-(re)start feed.
      if let pending = state.pending {
        state.pending = nil
        return pending
      }
      let (stream, continuation) = AsyncThrowingStream<SessionEvent, any Error>.makeStream()
      state.continuation = continuation
      return stream
    }
  }

  func sessionSend(sessionId: SessionId, input: SessionInput) async throws {
    try requireKnown(sessionId)
  }

  func interrupt(sessionId: SessionId) async throws {
    try requireKnown(sessionId)
  }

  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {
    try requireKnown(sessionId)
  }

  func close() async {
    feed.withLock { $0.continuation?.finish() }
    submittedPlansContinuation.finish()
  }

  private func requireKnown(_ sessionId: SessionId) throws {
    guard sessionId == knownSession else { throw ContractError.sessionNotFound(id: sessionId) }
  }

  private struct FeedState {
    /// The eagerly-prepared first feed, vended on the first `sessionEvents` call.
    var pending: AsyncThrowingStream<SessionEvent, any Error>?
    /// The latest vended feed's continuation, targeted by `emit`/`finish`.
    var continuation: AsyncThrowingStream<SessionEvent, any Error>.Continuation?
    var count = 0
  }

  private struct GateState {
    /// The suspended gated call's continuation, resumed by ``releaseGate()``.
    var continuation: CheckedContinuation<Void, Never>?
    /// Set once the gate is opened, so a release racing ahead of the suspend still lets it through.
    var isOpen = false
  }

  // MARK: - Unused board surface (the planner uses only session + createWorkstreamFromPlan)

  func snapshot() async throws -> Snapshot {
    throw ContractError.planRejected(reason: "unsupported in PlannerFakeBackend")
  }

  func control(workstreamId: WorkstreamId, action: ControlAction) async throws {
    throw ContractError.workstreamNotFound(id: workstreamId)
  }

  func retryIssue(issueId: IssueId) async throws {
    throw ContractError.issueNotFound(id: issueId)
  }

  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    AsyncThrowingStream { $0.finish() }
  }
}

/// A minimal lock-guarded box, so the fake's mutable feed state is safely shared
/// across the (nonisolated) `Backend` calls without opting out of concurrency
/// checking beyond this one audited boundary.
private final class Locked<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var value: Value

  init(_ value: Value) {
    self.value = value
  }

  func withLock<Result>(_ body: (inout Value) -> Result) -> Result {
    lock.lock()
    defer { lock.unlock() }
    return body(&value)
  }
}
