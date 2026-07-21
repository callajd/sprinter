import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE2.1's board tests.
///
/// It serves a fixed baseline ``Snapshot`` and lets the test push live
/// ``WorkGraphEvent`` deltas on demand, so a real ``WorkGraphResync`` can be driven
/// end to end without a daemon or network. Only the feed surface the board uses
/// (`snapshot` + `events`) is scripted; the write/execution methods are unused here
/// and throw, since the board is a read-only projection.
final class ScriptedBackend: Backend {
  private let baseline: Snapshot
  private let eventStream: AsyncThrowingStream<WorkGraphEvent, any Error>
  private let continuation: AsyncThrowingStream<WorkGraphEvent, any Error>.Continuation

  init(snapshot: Snapshot) {
    self.baseline = snapshot
    (self.eventStream, self.continuation) = AsyncThrowingStream.makeStream()
  }

  /// Emits one live delta onto the `events` subscription.
  func emit(_ event: WorkGraphEvent) {
    continuation.yield(event)
  }

  func snapshot() async throws -> Snapshot { baseline }

  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> { eventStream }

  func close() async { continuation.finish() }

  // MARK: - Unused write / execution surface (the board only reads)

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw ContractError.planRejected(reason: "unsupported in ScriptedBackend")
  }

  func control(workstreamId: WorkstreamId, action: ControlAction) async throws {
    throw ContractError.workstreamNotFound(id: workstreamId)
  }

  func retryIssue(issueId: IssueId) async throws {
    throw ContractError.issueNotFound(id: issueId)
  }

  func executionEvents(executionId: ExecutionId) -> AsyncThrowingStream<ExecutionEvent, any Error> {
    AsyncThrowingStream { $0.finish() }
  }

  func executionSend(executionId: ExecutionId, input: ExecutionInput) async throws {
    throw ContractError.executionNotFound(id: executionId)
  }

  func interrupt(executionId: ExecutionId) async throws {
    throw ContractError.executionNotFound(id: executionId)
  }

  func answerUiRequest(executionId: ExecutionId, response: UiResponse) async throws {
    throw ContractError.executionNotFound(id: executionId)
  }
}
