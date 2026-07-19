import Foundation
import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Mission Control inbox view model")
@MainActor
struct MissionControlInboxTests {
  private static let sessionA = SessionId(rawValue: "session-a")
  private static let sessionB = SessionId(rawValue: "session-b")

  /// A deterministic, strictly-increasing clock so wait-time ordering is stable in
  /// tests (each stamp is a distinct, later `Date`) rather than depending on the
  /// wall clock's resolution.
  private final class SteppingClock {
    private var tick = 0
    func now() -> Date {
      tick += 1
      return Date(timeIntervalSinceReferenceDate: TimeInterval(tick))
    }
  }

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
  /// entry retaining its own session id, ordered **longest-waiting-first** by the
  /// client-side arrival stamp — the request seen first sorts first regardless of
  /// session id.
  @Test("multiple sessions' pending requests aggregate, ordered by wait time")
  func multipleSessionsAggregate() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.sessionB)
    inbox.track(Self.sessionA)

    // session-B's request is raised (and observed) FIRST, so it has waited longer and
    // must sort ahead of session-A's — even though A < B by session id.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .input, prompt: "Name the branch", options: nil),
      on: Self.sessionB)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "Approve?", options: nil),
      on: Self.sessionA)

    #expect(await waitUntil(inbox) { $0.count == 2 })
    // Longest-waiting-first: req-b (seen first) before req-a.
    #expect(inbox.entries.map(\.sessionId) == [Self.sessionB, Self.sessionA])
    #expect(inbox.entries.map(\.requestId) == ["req-b", "req-a"])
    // The stamps reflect arrival order (earlier = longer wait).
    #expect(inbox.entries[0].waitingSince < inbox.entries[1].waitingSince)

    inbox.stop()
    await backend.close()
  }

  /// Two requests on the SAME session order by wait time within the session: the one
  /// raised first sorts ahead of the one raised later.
  @Test("same-session requests order by wait time")
  func sameSessionOrdersByWaitTime() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.sessionA)

    backend.emit(
      .uiRequestRaised(id: "first", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    backend.emit(
      .uiRequestRaised(id: "second", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 2 })

    #expect(inbox.entries.map(\.requestId) == ["first", "second"])

    inbox.stop()
    await backend.close()
  }

  /// `isOutstanding` is the no-longer-outstanding signal: `true` while a request is
  /// pending, `false` once it is answered (or otherwise leaves the outstanding set),
  /// and the client-side arrival stamp is PRUNED so a re-raised request is stamped
  /// afresh rather than inheriting its old wait.
  @Test("isOutstanding flips false on resolution and the arrival stamp is pruned")
  func isOutstandingFlipsAndArrivalResets() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.sessionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Merge?", options: nil),
      on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    let entry = try #require(inbox.entries.first)
    let firstStamp = entry.waitingSince
    #expect(inbox.isOutstanding(entry.id))

    // Answer it — it leaves the outstanding set, the entry clears, and the signal
    // flips to no-longer-outstanding.
    try await inbox.answer(entry, with: .confirmed(confirmed: true))
    #expect(await waitUntil(inbox) { $0.isEmpty })
    #expect(!inbox.isOutstanding(entry.id))

    // The SAME request id raised again is stamped afresh (a strictly-later stamp),
    // not the stale wait — the arrival map was pruned on resolution.
    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Merge again?", options: nil),
      on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    let reraised = try #require(inbox.entries.first)
    #expect(reraised.id == entry.id)
    #expect(reraised.waitingSince > firstStamp)

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

  /// The pure active-session extractor pulls each issue's live-agent session out of a
  /// projected board tree (and only those — a done issue with no agent contributes
  /// nothing).
  @Test("activeSessionIds extracts each issue's live-agent session")
  func activeSessionIdsExtracts() {
    let board = BoardProjection.project(BoardFixtures.snapshot)
    #expect(MissionControlInbox.activeSessionIds(in: board) == [SessionId(rawValue: "sess-a")])
  }

  /// `syncTrackedSessions` is the live-tracking diff: syncing to a new active set
  /// tracks newly-active sessions and untracks ones no longer active (their entries
  /// drop) — the CE3.1-F4 behaviour, not a point-in-time snapshot.
  @Test("syncTrackedSessions tracks newly-active and untracks no-longer-active sessions")
  func syncTrackedSessionsDiffs() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let inbox = MissionControlInbox(backend: backend)

    inbox.syncTrackedSessions(to: [Self.sessionA])
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.map(\.sessionId) == [Self.sessionA] })

    // Re-sync to {B}: A is untracked (its entry drops) and B is tracked.
    inbox.syncTrackedSessions(to: [Self.sessionB])
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionB)
    #expect(await waitUntil(inbox) { $0.map(\.sessionId) == [Self.sessionB] })

    inbox.stop()
    await backend.close()
  }

  /// `trackActiveSessions(of:)` follows the board LIVE (CE3.1-F4): a session that
  /// becomes active while the inbox is open is tracked, and one that goes inactive is
  /// untracked — reacting to board changes, not the `onAppear` snapshot.
  @Test("trackActiveSessions follows the board live as sessions activate/deactivate")
  func trackActiveSessionsIsLive() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let board = MissionControlBoard()
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job", issue: "iss", status: .running, session: "session-a")],
        sessions: []))
    let inbox = MissionControlInbox(backend: backend)
    inbox.trackActiveSessions(of: board)

    // session-a is the board's live agent → tracked; its prompt surfaces.
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.map(\.sessionId) == [Self.sessionA] })

    // The board changes WHILE the inbox is open: session-a goes inactive, session-b
    // becomes the live agent.
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job2", issue: "iss", status: .running, session: "session-b")],
        sessions: []))

    // Live re-sync: session-a is untracked (its entry drops) and session-b tracked.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionB)
    #expect(await waitUntil(inbox) { $0.map(\.sessionId) == [Self.sessionB] })

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
