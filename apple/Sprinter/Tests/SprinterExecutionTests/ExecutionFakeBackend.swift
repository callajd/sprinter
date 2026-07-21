import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE3.1's execution view-model tests.
///
/// It vends a **fresh** live ``ExecutionEvent`` feed on each `executionEvents` call
/// (modelling BE1's single-consumer feed re-subscribed on a restart), keeping the
/// latest stream's continuation so a test can `emit`/`finish`/`fail` the current
/// feed. It records every `executionSend`/`interrupt`/`answerUiRequest` it observes
/// on inspectable streams so a test can assert the exact input/response was driven
/// back — all without a daemon or network. Every call for an execution id other than
/// the known one throws ``ContractError/executionNotFound(id:)``, so the not-found
/// path is exercised too.
final class ExecutionFakeBackend: Backend {
  private let knownExecution: ExecutionId

  /// Lock-guarded mutable feed state — the latest vended feed's continuation and
  /// the count of feeds vended (a restart re-subscribes, so this increments).
  private let feed = Locked(FeedState())

  /// Every `executionSend` observed, in call order.
  let sent: AsyncStream<ExecutionInput>
  private let sentContinuation: AsyncStream<ExecutionInput>.Continuation
  /// Every `interrupt` observed, in call order.
  let interrupted: AsyncStream<ExecutionId>
  private let interruptedContinuation: AsyncStream<ExecutionId>.Continuation
  /// Every `answerUiRequest` observed, in call order.
  let answered: AsyncStream<UiResponse>
  private let answeredContinuation: AsyncStream<UiResponse>.Continuation

  init(knownExecution: ExecutionId) {
    self.knownExecution = knownExecution
    (sent, sentContinuation) = AsyncStream.makeStream()
    (interrupted, interruptedContinuation) = AsyncStream.makeStream()
    (answered, answeredContinuation) = AsyncStream.makeStream()
    // Prepare the first feed eagerly so its buffer is live before the model's feed
    // task subscribes — an `emit` right after `start()` is buffered, not lost.
    let (stream, continuation) = AsyncThrowingStream<ExecutionEvent, any Error>.makeStream()
    feed.withLock {
      $0.pending = stream
      $0.continuation = continuation
    }
  }

  /// The number of live feeds vended so far (one per `start`; a restart adds one).
  var feedCount: Int { feed.withLock { $0.count } }

  /// Raises one event on the current live feed.
  func emit(_ event: ExecutionEvent) {
    feed.withLock { _ = $0.continuation?.yield(event) }
  }

  /// Ends the current live feed cleanly (the execution ended).
  func finish() {
    feed.withLock { $0.continuation?.finish() }
  }

  /// Drops the current live feed with an error (a transport drop / failure `Exit`).
  func fail(_ error: any Error) {
    feed.withLock { $0.continuation?.finish(throwing: error) }
  }

  func executionEvents(executionId: ExecutionId) -> AsyncThrowingStream<ExecutionEvent, any Error> {
    guard executionId == knownExecution else {
      return AsyncThrowingStream {
        $0.finish(throwing: ContractError.executionNotFound(id: executionId))
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
      let (stream, continuation) = AsyncThrowingStream<ExecutionEvent, any Error>.makeStream()
      state.continuation = continuation
      return stream
    }
  }

  func executionSend(executionId: ExecutionId, input: ExecutionInput) async throws {
    try requireKnown(executionId)
    sentContinuation.yield(input)
  }

  func interrupt(executionId: ExecutionId) async throws {
    try requireKnown(executionId)
    interruptedContinuation.yield(executionId)
  }

  func answerUiRequest(executionId: ExecutionId, response: UiResponse) async throws {
    try requireKnown(executionId)
    answeredContinuation.yield(response)
  }

  func close() async {
    feed.withLock { $0.continuation?.finish() }
    sentContinuation.finish()
    interruptedContinuation.finish()
    answeredContinuation.finish()
  }

  private func requireKnown(_ executionId: ExecutionId) throws {
    guard executionId == knownExecution else {
      throw ContractError.executionNotFound(id: executionId)
    }
  }

  private struct FeedState {
    /// The eagerly-prepared first feed, vended on the first `executionEvents` call.
    var pending: AsyncThrowingStream<ExecutionEvent, any Error>?
    /// The latest vended feed's continuation, targeted by `emit`/`finish`/`fail`.
    var continuation: AsyncThrowingStream<ExecutionEvent, any Error>.Continuation?
    var count = 0
  }

  // MARK: - Unused board / write surface (the execution model only uses the execution channel)

  func snapshot() async throws -> Snapshot {
    throw ContractError.planRejected(reason: "unsupported in ExecutionFakeBackend")
  }

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw ContractError.planRejected(reason: "unsupported in ExecutionFakeBackend")
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
