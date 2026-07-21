import Foundation
import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE4.1's inspector tests.
///
/// It drives BOTH sides the inspector pairs, without a daemon or network:
/// - the **execution channel** — a fresh live ``ExecutionEvent`` feed per
///   `executionEvents` call (BE1's single-consumer feed re-subscribed on a restart),
///   whose current stream a test emits/finishes onto;
/// - the **work graph** — a fixed baseline ``Snapshot`` served by `snapshot` plus an
///   `events` subscription a test pushes ``WorkGraphEvent`` deltas onto, so a real
///   ``WorkGraphResync`` folds them into fresh snapshots.
///
/// A `executionEvents` for an unknown execution id fails with
/// ``ContractError/executionNotFound(id:)``, so the not-found path is exercised too.
final class InspectorFakeBackend: Backend {
  private let knownExecution: ExecutionId
  private let baseline: Snapshot

  private let execution = Locked(ExecutionFeed())
  private let workGraph = Locked(WorkGraphFeed())

  init(knownExecution: ExecutionId, snapshot: Snapshot) {
    self.knownExecution = knownExecution
    self.baseline = snapshot
    // Prepare the first execution feed eagerly so an `emit` right after `start()` is
    // buffered, not lost, before the model's feed task subscribes.
    let (stream, continuation) = AsyncThrowingStream<ExecutionEvent, any Error>.makeStream()
    execution.withLock {
      $0.pending = stream
      $0.continuation = continuation
    }
    let (events, eventsContinuation) = AsyncThrowingStream<WorkGraphEvent, any Error>.makeStream()
    workGraph.withLock {
      $0.stream = events
      $0.continuation = eventsContinuation
    }
  }

  // MARK: - Execution feed control

  /// The number of live execution feeds vended so far (one per `start`; a restart
  /// adds one).
  var executionFeedCount: Int { execution.withLock { $0.count } }

  /// Raises one event on the current live execution feed.
  func emit(_ event: ExecutionEvent) {
    execution.withLock { _ = $0.continuation?.yield(event) }
  }

  /// Ends the current live execution feed cleanly (the execution ended).
  func finishExecution() {
    execution.withLock { $0.continuation?.finish() }
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

  func executionEvents(executionId: ExecutionId) -> AsyncThrowingStream<ExecutionEvent, any Error> {
    guard executionId == knownExecution else {
      return AsyncThrowingStream {
        $0.finish(throwing: ContractError.executionNotFound(id: executionId))
      }
    }
    return execution.withLock { state in
      state.count += 1
      if let pending = state.pending {
        state.pending = nil
        return pending
      }
      let (stream, continuation) = AsyncThrowingStream<ExecutionEvent, any Error>.makeStream()
      state.continuation = continuation
      return stream
    }
  }

  func close() async {
    execution.withLock { $0.continuation?.finish() }
    workGraph.withLock { $0.continuation?.finish() }
  }

  // MARK: - Unused write / input surface (the inspector reads two feeds)

  func executionSend(executionId: ExecutionId, input: ExecutionInput) async throws {
    try requireKnown(executionId)
  }

  func interrupt(executionId: ExecutionId) async throws {
    try requireKnown(executionId)
  }

  func answerUiRequest(executionId: ExecutionId, response: UiResponse) async throws {
    try requireKnown(executionId)
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

  private func requireKnown(_ executionId: ExecutionId) throws {
    guard executionId == knownExecution else {
      throw ContractError.executionNotFound(id: executionId)
    }
  }

  private struct ExecutionFeed {
    var pending: AsyncThrowingStream<ExecutionEvent, any Error>?
    var continuation: AsyncThrowingStream<ExecutionEvent, any Error>.Continuation?
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
