import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE4.1's inspector tests.
///
/// It drives BOTH sides the inspector pairs, without a daemon or network:
/// - the **session channel** — a fresh live ``SessionEvent`` feed per
///   `sessionEvents` call (BE1's single-consumer feed re-subscribed on a restart),
///   whose current stream a test emits/finishes onto;
/// - the **work graph** — a fixed baseline ``Snapshot`` served by `snapshot` plus an
///   `events` subscription a test pushes ``WorkGraphEvent`` deltas onto, so a real
///   ``WorkGraphResync`` folds them into fresh snapshots.
///
/// A `sessionEvents` for an unknown session id fails with
/// ``ContractError/sessionNotFound(id:)``, so the not-found path is exercised too.
final class InspectorFakeBackend: Backend {
  private let knownSession: SessionId
  private let baseline: Snapshot

  private let session = Locked(SessionFeed())
  private let workGraph = Locked(WorkGraphFeed())

  init(knownSession: SessionId, snapshot: Snapshot) {
    self.knownSession = knownSession
    self.baseline = snapshot
    // Prepare the first session feed eagerly so an `emit` right after `start()` is
    // buffered, not lost, before the model's feed task subscribes.
    let (stream, continuation) = AsyncThrowingStream<SessionEvent, any Error>.makeStream()
    session.withLock {
      $0.pending = stream
      $0.continuation = continuation
    }
    let (events, eventsContinuation) = AsyncThrowingStream<WorkGraphEvent, any Error>.makeStream()
    workGraph.withLock {
      $0.stream = events
      $0.continuation = eventsContinuation
    }
  }

  // MARK: - Session feed control

  /// The number of live session feeds vended so far (one per `start`; a restart
  /// adds one).
  var sessionFeedCount: Int { session.withLock { $0.count } }

  /// Raises one event on the current live session feed.
  func emit(_ event: SessionEvent) {
    session.withLock { _ = $0.continuation?.yield(event) }
  }

  /// Ends the current live session feed cleanly (the session ended).
  func finishSession() {
    session.withLock { $0.continuation?.finish() }
  }

  // MARK: - Work-graph feed control

  /// Emits one live delta onto the `events` subscription.
  func emit(_ event: WorkGraphEvent) {
    workGraph.withLock { _ = $0.continuation?.yield(event) }
  }

  // MARK: - Backend

  func snapshot() async throws -> Snapshot { baseline }

  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    workGraph.withLock { state in
      // Vend the prepared stream once; a re-subscribe (a fresh WorkGraphResync
      // attempt) gets a fresh stream so the feed stays single-consumer per attempt.
      if let stream = state.stream {
        state.stream = nil
        return stream
      }
      let (stream, continuation) = AsyncThrowingStream<WorkGraphEvent, any Error>.makeStream()
      state.continuation = continuation
      return stream
    }
  }

  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    guard sessionId == knownSession else {
      return AsyncThrowingStream {
        $0.finish(throwing: ContractError.sessionNotFound(id: sessionId))
      }
    }
    return session.withLock { state in
      state.count += 1
      if let pending = state.pending {
        state.pending = nil
        return pending
      }
      let (stream, continuation) = AsyncThrowingStream<SessionEvent, any Error>.makeStream()
      state.continuation = continuation
      return stream
    }
  }

  func close() async {
    session.withLock { $0.continuation?.finish() }
    workGraph.withLock { $0.continuation?.finish() }
  }

  // MARK: - Unused write / input surface (the inspector reads two feeds)

  func sessionSend(sessionId: SessionId, input: SessionInput) async throws {
    try requireKnown(sessionId)
  }

  func interrupt(sessionId: SessionId) async throws {
    try requireKnown(sessionId)
  }

  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {
    try requireKnown(sessionId)
  }

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw ContractError.planRejected(reason: "unsupported in InspectorFakeBackend")
  }

  func control(workstreamId: WorkstreamId, action: ControlAction) async throws {
    throw ContractError.workstreamNotFound(id: workstreamId)
  }

  func retryIssue(issueId: IssueId) async throws {
    throw ContractError.issueNotFound(id: issueId)
  }

  private func requireKnown(_ sessionId: SessionId) throws {
    guard sessionId == knownSession else { throw ContractError.sessionNotFound(id: sessionId) }
  }

  private struct SessionFeed {
    var pending: AsyncThrowingStream<SessionEvent, any Error>?
    var continuation: AsyncThrowingStream<SessionEvent, any Error>.Continuation?
    var count = 0
  }

  private struct WorkGraphFeed {
    var stream: AsyncThrowingStream<WorkGraphEvent, any Error>?
    var continuation: AsyncThrowingStream<WorkGraphEvent, any Error>.Continuation?
  }
}

/// A minimal lock-guarded box so the fake's mutable feed state is safely shared
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
