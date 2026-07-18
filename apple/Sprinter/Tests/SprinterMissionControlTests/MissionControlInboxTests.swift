import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Mission Control inbox view model")
@MainActor
struct MissionControlInboxTests {
  private static let sessionA = SessionId(rawValue: "session-a")
  private static let sessionB = SessionId(rawValue: "session-b")

  /// A `UiRequestRaised` surfacing on a tracked session's feed appears in the inbox,
  /// carrying its session id and the prompt/kind/options to render — driven by a
  /// fake `Backend`, no daemon or network.
  @Test("a raised UI request surfaces as an inbox entry")
  func raisedRequestSurfaces() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.sessionA)
    #expect(inbox.entries.isEmpty)
    #expect(!inbox.hasWaitingAgents)

    backend.emit(
      .uiRequestRaised(
        id: "req-1", kind: .select, prompt: "Pick a branch", options: ["main", "dev"]),
      on: Self.sessionA)

    #expect(await waitUntil(inbox) { $0.count == 1 })
    let entry = try #require(inbox.entries.first)
    #expect(entry.sessionId == Self.sessionA)
    #expect(entry.requestId == "req-1")
    #expect(entry.kind == .select)
    #expect(entry.prompt == "Pick a branch")
    #expect(entry.options == ["main", "dev"])
    #expect(inbox.hasWaitingAgents)

    inbox.stop()
    await backend.close()
  }

  /// Answering an inbox entry drives `answerUiRequest` through the session channel
  /// with the neutral `UiResponse` keyed to the request id (the fake observes it),
  /// and the resolved entry leaves the inbox.
  @Test("answering an entry drives answerUiRequest and clears it")
  func answerDrivesRoundTripAndClears() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA])
    let inbox = MissionControlInbox(backend: backend)
    var observed = backend.answered.makeAsyncIterator()
    inbox.track(Self.sessionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Merge the PR?", options: nil),
      on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    let entry = try #require(inbox.entries.first)

    try await inbox.answer(entry, with: .confirmed(confirmed: true))

    // The fake observed exactly the neutral UiResponse, keyed to the request id.
    let answered = try #require(await observed.next())
    #expect(answered.sessionId == Self.sessionA)
    #expect(
      answered.response == UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true)))

    // The resolved entry left the inbox.
    #expect(await waitUntil(inbox) { $0.isEmpty })
    #expect(!inbox.hasWaitingAgents)

    inbox.stop()
    await backend.close()
  }

  /// Pending requests raised on distinct sessions aggregate into one inbox, each
  /// entry retaining its own session id (ordered deterministically by session id).
  @Test("multiple sessions' pending requests aggregate into one inbox")
  func multipleSessionsAggregate() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.sessionB)
    inbox.track(Self.sessionA)

    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .input, prompt: "Name the branch", options: nil),
      on: Self.sessionB)
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "Approve?", options: nil),
      on: Self.sessionA)

    #expect(await waitUntil(inbox) { $0.count == 2 })
    // Deterministic order: session-a before session-b.
    #expect(inbox.entries.map(\.sessionId) == [Self.sessionA, Self.sessionB])
    #expect(inbox.entries.map(\.requestId) == ["req-a", "req-b"])

    inbox.stop()
    await backend.close()
  }

  /// `track` is idempotent while already tracking (a second call is a no-op, never a
  /// cancel-and-respin of the single-consumer feed), and `stop` tears every session
  /// down. Run in isolation — it must be green on its own, not by suite scheduling.
  @Test("track is idempotent; stop tears down every session")
  func trackIdempotentAndStop() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA])
    let inbox = MissionControlInbox(backend: backend)

    inbox.track(Self.sessionA)
    // A second track of the same session is a NO-OP — it must not respin the feed.
    inbox.track(Self.sessionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Ship it?", options: nil),
      on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })

    // The prompt surfaced exactly once — a respun feed would have duplicated it.
    #expect(inbox.entries.count == 1)

    inbox.stop()
    #expect(inbox.entries.isEmpty)
    inbox.stop()  // idempotent

    await backend.close()
  }

  /// `untrack` drops a single session (and its feed) from the inbox, leaving the
  /// other tracked sessions' entries intact.
  @Test("untrack removes one session's entries, leaving the others")
  func untrackRemovesOneSession() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.sessionA)
    inbox.track(Self.sessionB)
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionB)
    #expect(await waitUntil(inbox) { $0.count == 2 })

    // Untrack A: its feed is torn down and its entry drops; only B remains.
    inbox.untrack(Self.sessionA)
    #expect(inbox.entries.map(\.sessionId) == [Self.sessionB])
    #expect(inbox.entries.first?.requestId == "req-b")

    inbox.stop()
    await backend.close()
  }

  /// Identical request ids on different sessions get DISTINCT composite entry ids —
  /// the `sessionId ⨝ requestId` key disambiguates (request ids are unique only
  /// within a session).
  @Test("identical request ids across sessions get distinct composite entry ids")
  func compositeIdDisambiguatesAcrossSessions() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.sessionA)
    inbox.track(Self.sessionB)
    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionB)
    #expect(await waitUntil(inbox) { $0.count == 2 })

    #expect(inbox.entries.allSatisfy { $0.requestId == "req-1" })
    #expect(Set(inbox.entries.map(\.id)).count == 2)

    inbox.stop()
    await backend.close()
  }

  /// Polls the main-actor inbox until `predicate` holds over its entries, yielding
  /// between checks so each session's feed-ingestion task can run. Returns `false`
  /// if the bound is exhausted.
  private func waitUntil(
    _ inbox: MissionControlInbox,
    _ predicate: ([InboxEntry]) -> Bool
  ) async -> Bool {
    for _ in 0..<100_000 {
      if predicate(inbox.entries) { return true }
      await Task.yield()
    }
    return false
  }
}
