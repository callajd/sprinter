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
  private static let executionA = ExecutionId(rawValue: "execution-a")
  private static let executionB = ExecutionId(rawValue: "execution-b")

  /// `stop()` is FINAL against a board mutation racing the inbox's release: after
  /// `stop()`, a board change that re-fires the armed `trackActiveExecutions` `onChange`
  /// must NOT re-track or re-`start()` any execution — no feed is re-subscribed, so a
  /// prompt on a would-be-tracked execution surfaces nothing. Guards the `isStopped`
  /// short-circuit (the fix for the one-shot `onChange` re-arming after teardown).
  @Test("after stop, a board mutation does not re-track or re-subscribe any execution")
  func stopIsFinalAgainstBoardMutation() async throws {
    let backend = InboxFakeBackend(executionIds: [Self.executionA, Self.executionB])
    let board = MissionControlBoard()
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job", issue: "iss", status: .running, execution: "execution-a")],
        executions: []))
    let inbox = MissionControlInbox(backend: backend)
    inbox.trackActiveExecutions(of: board)

    // execution-a is tracked live — confirm a prompt surfaces, so tracking is genuinely on.
    backend.emit(
      .uiRequestRaised(id: "req-a", kind: .confirm, prompt: "A?", options: nil), on: Self.executionA
    )
    #expect(await waitUntil(inbox) { $0.map(\.executionId) == [Self.executionA] })

    // Tear the inbox down (a dismissed sheet).
    inbox.stop()
    #expect(inbox.entries.isEmpty)

    // The board changes WHILE the stopped inbox's `onChange` is still armed: execution-b
    // becomes the live agent. A pre-fix inbox re-fires `trackActiveExecutions` → track(b)
    // → start(b), re-subscribing a feed `stop()` should have made impossible.
    board.apply(
      BoardFixtures.singleEpicSnapshot(
        issueIds: ["iss"],
        jobs: [BoardFixtures.job("job2", issue: "iss", status: .running, execution: "execution-b")],
        executions: []))

    // A prompt on execution-b must surface NOTHING: it was never (re-)tracked or started.
    backend.emit(
      .uiRequestRaised(id: "req-b", kind: .confirm, prompt: "B?", options: nil), on: Self.executionB
    )
    #expect(await staysEmpty(inbox))

    // Second stop is idempotent.
    inbox.stop()
    #expect(inbox.entries.isEmpty)

    await backend.close()
  }

  /// Polls the main-actor inbox until `predicate` holds over its entries, yielding
  /// between checks so each execution's feed-ingestion task can run. Returns `false` if
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
  /// execution was (re-)tracked/subscribed after `stop()`: were a feed live, its emitted
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
