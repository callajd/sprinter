import SprinterBackend
import SprinterContract

/// A deterministic, offline fake ``Backend`` for BE2.2's inbox tests.
///
/// It serves one live ``SessionEvent`` feed per session declared at init (so the
/// feed stays single-consumer — a second `sessionEvents` call for the same session
/// returns the same stream), lets the test push `UiRequestRaised` prompts on
/// demand, and records every `answerUiRequest` it observes on the ``answered``
/// stream so a test can assert the exact neutral ``UiResponse`` was driven back —
/// all without a daemon or network. Only the session surface the inbox uses is
/// scripted; the board/write methods are unused here and throw.
///
/// All stored state is immutable (`let` continuations/streams), so the fake is
/// `Sendable` without opting out of checking — matching the board's `ScriptedBackend`.
final class InboxFakeBackend: Backend {
  /// One observed `answerUiRequest` call.
  struct Answered: Equatable, Sendable {
    let sessionId: SessionId
    let response: UiResponse
  }

  private let feeds: [SessionId: AsyncThrowingStream<SessionEvent, any Error>]
  private let continuations: [SessionId: AsyncThrowingStream<SessionEvent, any Error>.Continuation]

  /// Every `answerUiRequest` the inbox drove, in call order.
  let answered: AsyncStream<Answered>
  private let answeredContinuation: AsyncStream<Answered>.Continuation

  init(sessionIds: [SessionId]) {
    var feeds: [SessionId: AsyncThrowingStream<SessionEvent, any Error>] = [:]
    var continuations: [SessionId: AsyncThrowingStream<SessionEvent, any Error>.Continuation] = [:]
    for id in sessionIds {
      let (stream, continuation) = AsyncThrowingStream<SessionEvent, any Error>.makeStream()
      feeds[id] = stream
      continuations[id] = continuation
    }
    self.feeds = feeds
    self.continuations = continuations
    (answered, answeredContinuation) = AsyncStream.makeStream()
  }

  /// Raises one event on a session's live feed.
  func emit(_ event: SessionEvent, on sessionId: SessionId) {
    continuations[sessionId]?.yield(event)
  }

  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    feeds[sessionId] ?? AsyncThrowingStream { $0.finish() }
  }

  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {
    answeredContinuation.yield(Answered(sessionId: sessionId, response: response))
  }

  func close() async {
    for continuation in continuations.values {
      continuation.finish()
    }
    answeredContinuation.finish()
  }

  // MARK: - Unused board / write surface (the inbox only reads the session feed)

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

  func sessionSend(sessionId: SessionId, input: SessionInput) async throws {
    throw ContractError.sessionNotFound(id: sessionId)
  }

  func interrupt(sessionId: SessionId) async throws {
    throw ContractError.sessionNotFound(id: sessionId)
  }
}
