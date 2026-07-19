import Foundation
import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterMissionControl

/// The inbox's teardown lifecycle: `stop()` must be FINAL — no armed Observation loop
/// may re-arm or re-subscribe a feed after it (CE3.2 cold-review FIX1). Its own suite so
/// the core inbox suite stays under SwiftLint's type/file-length ceilings.
@Suite("Mission Control inbox lifecycle")
@MainActor
struct MissionControlInboxLifecycleTests {
  private static let sessionA = SessionId(rawValue: "session-a")
  private static let sessionB = SessionId(rawValue: "session-b")

  /// `stop()` is FINAL against a board mutation racing the inbox's release: after
  /// `stop()`, a board change that re-fires the armed `trackActiveSessions` `onChange`
  /// must NOT re-track or re-`start()` any session — no feed is re-subscribed, so a
  /// prompt on a would-be-tracked session surfaces nothing. Guards the `isStopped`
  /// short-circuit (the fix for the one-shot `onChange` re-arming after teardown).
  @Test("after stop, a board mutation does not re-track or re-subscribe any session")
  func stopIsFinalAgainstBoardMutation() async throws {
    let backend = InboxFakeBackend(sessionIds: [Self.sessionA, Self.sessionB])
    let board = MissionControlBoard()
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job", issue: "iss", status: .running, session: "session-a")],
        sessions: []))
    let inbox = MissionControlInbox(backend: backend)
    inbox.trackActiveSessions(of: board)

    // session-a is tracked live — confirm a prompt surfaces, so tracking is genuinely on.
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.sessionA)
    #expect(await waitUntil(inbox) { $0.map(\.sessionId) == [Self.sessionA] })

    // Tear the inbox down (a dismissed sheet).
    inbox.stop()
    #expect(inbox.entries.isEmpty)

    // The board changes WHILE the stopped inbox's `onChange` is still armed: session-b
    // becomes the live agent. A pre-fix inbox re-fires `trackActiveSessions` → track(b)
    // → start(b), re-subscribing a feed `stop()` should have made impossible.
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job2", issue: "iss", status: .running, session: "session-b")],
        sessions: []))

    // A prompt on session-b must surface NOTHING: it was never (re-)tracked or started.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.sessionB)
    #expect(await staysEmpty(inbox))

    // Second stop is idempotent.
    inbox.stop()
    #expect(inbox.entries.isEmpty)

    await backend.close()
  }

  /// Polls the main-actor inbox until `predicate` holds over its entries, yielding
  /// between checks so each session's feed-ingestion task can run. Returns `false` if
  /// the bound is exhausted.
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

  /// Yields many times and asserts the inbox's entries stay empty throughout — proof NO
  /// session was (re-)tracked/subscribed after `stop()`: were a feed live, its emitted
  /// prompt would surface within these yields. Returns `false` the moment an entry
  /// appears.
  private func staysEmpty(_ inbox: MissionControlInbox) async -> Bool {
    for _ in 0..<10_000 {
      if !inbox.entries.isEmpty { return false }
      await Task.yield()
    }
    return inbox.entries.isEmpty
  }
}
