import Foundation
import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Mission Control inbox view model")
@MainActor
struct MissionControlInboxTests {
  private static let executionA = ExecutionId(rawValue: "execution-a")
  private static let executionB = ExecutionId(rawValue: "execution-b")

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

  /// A `UiRequestRaised` surfacing on a tracked execution's feed appears in the inbox,
  /// carrying its execution id and the prompt/kind/options to render — driven by a
  /// fake `Backend`, no daemon or network.
  @Test("a raised UI request surfaces as an inbox entry")
  func raisedRequestSurfaces() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.executionA)
    #expect(inbox.entries.isEmpty)
    #expect(!inbox.hasWaitingAgents)

    backend.emit(
      .uiRequestRaised(
        id: "req-1", kind: .select, prompt: "Pick a branch", options: ["main", "dev"]),
      on: Self.executionA)

    #expect(await waitUntil(inbox) { $0.count == 1 })
    let entry = try #require(inbox.entries.first)
    #expect(entry.executionId == Self.executionA)
    #expect(entry.requestId == "req-1")
    #expect(entry.kind == .select)
    #expect(entry.prompt == "Pick a branch")
    #expect(entry.options == ["main", "dev"])
    #expect(inbox.hasWaitingAgents)

    inbox.stop()
    await backend.close()
  }

  /// Answering an inbox entry drives `answerUiRequest` through the execution channel
  /// with the neutral `UiResponse` keyed to the request id (the fake observes it),
  /// and the resolved entry leaves the inbox.
  @Test("answering an entry drives answerUiRequest and clears it")
  func answerDrivesRoundTripAndClears() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA])
    let inbox = MissionControlInbox(backend: backend)
    var observed = backend.answered.makeAsyncIterator()
    inbox.track(Self.executionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Merge the PR?", options: nil),
      on: Self.executionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    let entry = try #require(inbox.entries.first)

    try await inbox.answer(entry, with: .confirmed(confirmed: true))

    // The fake observed exactly the neutral UiResponse, keyed to the request id.
    let answered = try #require(await observed.next())
    #expect(answered.executionId == Self.executionA)
    #expect(
      answered.response == UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true)))

    // The resolved entry left the inbox.
    #expect(await waitUntil(inbox) { $0.isEmpty })
    #expect(!inbox.hasWaitingAgents)

    inbox.stop()
    await backend.close()
  }

  /// Pending requests raised on distinct executions aggregate into one inbox, each
  /// entry retaining its own execution id, ordered **longest-waiting-first** by the
  /// client-side arrival stamp — the request seen first sorts first regardless of
  /// execution id.
  @Test("multiple executions' pending requests aggregate, ordered by wait time")
  func multipleExecutionsAggregate() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.executionB)
    inbox.track(Self.executionA)

    // execution-B's request is raised (and observed) FIRST, so it has waited longer and
    // must sort ahead of execution-A's — even though A < B by execution id.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .input, prompt: "Name the branch", options: nil),
      on: Self.executionB)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "Approve?", options: nil),
      on: Self.executionA)

    #expect(await waitUntil(inbox) { $0.count == 2 })
    // Longest-waiting-first: req-b (seen first) before req-a.
    #expect(inbox.entries.map(\.executionId) == [Self.executionB, Self.executionA])
    #expect(inbox.entries.map(\.requestId) == ["req-b", "req-a"])
    // The stamps reflect arrival order (earlier = longer wait).
    #expect(inbox.entries[0].waitingSince < inbox.entries[1].waitingSince)

    inbox.stop()
    await backend.close()
  }

  /// Two requests on the SAME execution order by wait time within the execution: the one
  /// raised first sorts ahead of the one raised later.
  @Test("same-execution requests order by wait time")
  func sameExecutionOrdersByWaitTime() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.executionA)

    backend.emit(
      .uiRequestRaised(id: "first", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    #expect(await waitUntil(inbox) { $0.count == 1 })
    backend.emit(
      .uiRequestRaised(id: "second", kind: .confirm, prompt: "B?", options: nil),
      on: Self.executionA)
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
    let backend = InboxFakeBackend(executionIds: [Self.executionA])
    let clock = SteppingClock()
    let inbox = MissionControlInbox(backend: backend, now: clock.now)
    inbox.track(Self.executionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Merge?", options: nil),
      on: Self.executionA)
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
      on: Self.executionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })
    let reraised = try #require(inbox.entries.first)
    #expect(reraised.id == entry.id)
    #expect(reraised.waitingSince > firstStamp)

    inbox.stop()
    await backend.close()
  }

  /// `track` is idempotent while already tracking (a second call is a no-op, never a
  /// cancel-and-respin of the single-consumer feed), and `stop` tears every execution
  /// down. Run in isolation — it must be green on its own, not by suite scheduling.
  @Test("track is idempotent; stop tears down every execution")
  func trackIdempotentAndStop() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA])
    let inbox = MissionControlInbox(backend: backend)

    inbox.track(Self.executionA)
    // A second track of the same execution is a NO-OP — it must not respin the feed.
    inbox.track(Self.executionA)

    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "Ship it?", options: nil),
      on: Self.executionA)
    #expect(await waitUntil(inbox) { $0.count == 1 })

    // The prompt surfaced exactly once — a respun feed would have duplicated it.
    #expect(inbox.entries.count == 1)

    inbox.stop()
    #expect(inbox.entries.isEmpty)
    inbox.stop()  // idempotent

    await backend.close()
  }

  /// `untrack` drops a single execution (and its feed) from the inbox, leaving the
  /// other tracked executions' entries intact.
  @Test("untrack removes one execution's entries, leaving the others")
  func untrackRemovesOneExecution() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.executionA)
    inbox.track(Self.executionB)
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.executionB
    )
    #expect(await waitUntil(inbox) { $0.count == 2 })

    // Untrack A: its feed is torn down and its entry drops; only B remains.
    inbox.untrack(Self.executionA)
    #expect(inbox.entries.map(\.executionId) == [Self.executionB])
    #expect(inbox.entries.first?.requestId == "req-b")

    inbox.stop()
    await backend.close()
  }

  /// Identical request ids on different executions get DISTINCT composite entry ids —
  /// the `executionId ⨝ requestId` key disambiguates (request ids are unique only
  /// within an execution).
  @Test("identical request ids across executions get distinct composite entry ids")
  func compositeIdDisambiguatesAcrossExecutions() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let inbox = MissionControlInbox(backend: backend)
    inbox.track(Self.executionA)
    inbox.track(Self.executionB)
    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    backend.emit(
      .uiRequestRaised(id: "req-1", kind: .confirm, prompt: "B?", options: nil), on: Self.executionB
    )
    #expect(await waitUntil(inbox) { $0.count == 2 })

    #expect(inbox.entries.allSatisfy { $0.requestId == "req-1" })
    #expect(Set(inbox.entries.map(\.id)).count == 2)

    inbox.stop()
    await backend.close()
  }

  /// The pure active-execution extractor pulls each issue's live-agent execution out of a
  /// projected board tree (and only those — a done issue with no agent contributes
  /// nothing).
  @Test("activeExecutionIds extracts each issue's live-agent execution")
  func activeExecutionIdsExtracts() {
    let board = BoardProjection.project(BoardFixtures.snapshot)
    #expect(MissionControlInbox.activeExecutionIds(in: board) == [ExecutionId(rawValue: "sess-a")])
  }

  /// `syncTrackedExecutions` is the live-tracking diff: syncing to a new active set
  /// tracks newly-active executions and untracks ones no longer active (their entries
  /// drop) — the CE3.1-F4 behaviour, not a point-in-time snapshot.
  @Test("syncTrackedExecutions tracks newly-active and untracks no-longer-active executions")
  func syncTrackedExecutionsDiffs() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let inbox = MissionControlInbox(backend: backend)

    inbox.syncTrackedExecutions(to: [Self.executionA])
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    #expect(await waitUntil(inbox) { $0.map(\.executionId) == [Self.executionA] })

    // Re-sync to {B}: A is untracked (its entry drops) and B is tracked.
    inbox.syncTrackedExecutions(to: [Self.executionB])
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.executionB
    )
    #expect(await waitUntil(inbox) { $0.map(\.executionId) == [Self.executionB] })

    inbox.stop()
    await backend.close()
  }

  /// `trackActiveExecutions(of:)` follows the board LIVE (CE3.1-F4): an execution that
  /// becomes active while the inbox is open is tracked, and one that goes inactive is
  /// untracked — reacting to board changes, not the `onAppear` snapshot.
  @Test("trackActiveExecutions follows the board live as executions activate/deactivate")
  func trackActiveExecutionsIsLive() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let board = MissionControlBoard()
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job", issue: "iss", status: .running, execution: "execution-a")],
        executions: []))
    let inbox = MissionControlInbox(backend: backend)
    inbox.trackActiveExecutions(of: board)

    // execution-a is the board's live agent → tracked; its prompt surfaces.
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    #expect(await waitUntil(inbox) { $0.map(\.executionId) == [Self.executionA] })

    // The board changes WHILE the inbox is open: execution-a goes inactive, execution-b
    // becomes the live agent.
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job2", issue: "iss", status: .running, execution: "execution-b")],
        executions: []))

    // Live re-sync: execution-a is untracked (its entry drops) and execution-b tracked.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.executionB
    )
    #expect(await waitUntil(inbox) { $0.map(\.executionId) == [Self.executionB] })

    inbox.stop()
    await backend.close()
  }

  /// Polls the main-actor inbox until `predicate` holds over its entries, yielding
  /// between checks so each execution's feed-ingestion task can run. Returns `false`
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
