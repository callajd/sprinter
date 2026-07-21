import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE2.2's inbox tests.
///
/// It serves one live ``ExecutionEvent`` feed per execution declared at init (so the
/// feed stays single-consumer — a second `executionEvents` call for the same execution
/// returns the same stream), lets the test push `UiRequestRaised` prompts on
/// demand, and records every `answerUiRequest` it observes on the ``answered``
/// stream so a test can assert the exact neutral ``UiResponse`` was driven back —
/// all without a daemon or network. Only the execution surface the inbox uses is
/// scripted; the board/write methods are unused here and throw.
///
/// All stored state is immutable (`let` continuations/streams), so the fake is
/// `Sendable` without opting out of checking — matching the board's `ScriptedBackend`.
final class InboxFakeBackend: Backend {
  /// One observed `answerUiRequest` call.
  struct Answered: Equatable, Sendable {
    let executionId: ExecutionId
    let response: UiResponse
  }

  private let feeds: [ExecutionId: AsyncThrowingStream<ExecutionEvent, any Error>]
  private let continuations:
    [ExecutionId: AsyncThrowingStream<ExecutionEvent, any Error>.Continuation]

  /// Every `answerUiRequest` the inbox drove, in call order.
  let answered: AsyncStream<Answered>
  private let answeredContinuation: AsyncStream<Answered>.Continuation

  init(executionIds: [ExecutionId]) {
    var feeds: [ExecutionId: AsyncThrowingStream<ExecutionEvent, any Error>] = [:]
    var continuations: [ExecutionId: AsyncThrowingStream<ExecutionEvent, any Error>.Continuation] =
      [:]
    for id in executionIds {
      let (stream, continuation) = AsyncThrowingStream<ExecutionEvent, any Error>.makeStream()
      feeds[id] = stream
      continuations[id] = continuation
    }
    self.feeds = feeds
    self.continuations = continuations
    (answered, answeredContinuation) = AsyncStream.makeStream()
  }

  /// Raises one event on an execution's live feed.
  func emit(_ event: ExecutionEvent, on executionId: ExecutionId) {
    continuations[executionId]?.yield(event)
  }

  func executionEvents(executionId: ExecutionId) -> AsyncThrowingStream<ExecutionEvent, any Error> {
    feeds[executionId] ?? AsyncThrowingStream { $0.finish() }
  }

  func answerUiRequest(executionId: ExecutionId, response: UiResponse) async throws {
    answeredContinuation.yield(Answered(executionId: executionId, response: response))
  }

  func close() async {
    for continuation in continuations.values {
      continuation.finish()
    }
    answeredContinuation.finish()
  }

  // MARK: - Unused board / write surface (the inbox only reads the execution feed)

  func snapshot() async throws -> Snapshot {
    throw ContractError.planRejected(reason: "unsupported in InboxFakeBackend")
  }

  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw ContractError.planRejected(reason: "unsupported in InboxFakeBackend")
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

  func executionSend(executionId: ExecutionId, input: ExecutionInput) async throws {
    throw ContractError.executionNotFound(id: executionId)
  }

  func interrupt(executionId: ExecutionId) async throws {
    throw ContractError.executionNotFound(id: executionId)
  }
}
