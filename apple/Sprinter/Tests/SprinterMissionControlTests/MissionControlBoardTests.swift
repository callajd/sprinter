import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Mission Control board view model")
@MainActor
struct MissionControlBoardTests {
  /// The board builds from the port-based feed's baseline snapshot, then a live
  /// `events` delta (folded onto the baseline by `WorkGraphResync`) flips a node's
  /// status ongoing → complete and the board reflects it — driven entirely by a
  /// fake `Backend`, no daemon or network.
  @Test("builds from the snapshot, then reflects a live status flip")
  func buildsThenReflectsLiveEvent() async throws {
    let backend = ScriptedBackend(snapshot: BoardFixtures.snapshot)
    let feed = WorkGraphResync(connect: { backend }, backoff: .noDelay)
    let board = MissionControlBoard()
    board.start(feed)

    // Baseline: epic A projects as ongoing, with its live agent surfaced.
    #expect(await waitUntil(board) { $0.first?.epics.first?.status == .ongoing })
    #expect(board.workstreams.first?.epics.first?.issues.first?.hasLiveAgent == true)

    // A live delta advances epic A to done; the board reflects the flip.
    backend.emit(.epicChanged(BoardFixtures.epicADone))
    #expect(await waitUntil(board) { $0.first?.epics.first?.status == .complete })

    board.stop()
    await backend.close()
  }

  /// `apply` projects a snapshot directly and replaces the prior board — the pure
  /// main-actor core the feed consumer reuses.
  @Test("apply replaces the board with a fresh projection")
  func applyReplacesBoard() {
    let board = MissionControlBoard()
    #expect(board.workstreams.isEmpty)

    board.apply(BoardFixtures.snapshot)
    #expect(board.workstreams.count == 2)
    #expect(board.workstreams.first?.epics.first?.status == .ongoing)

    // A second snapshot fully replaces the first (snapshot-then-live, D4).
    let onlyB = Snapshot(
      workstreams: [BoardFixtures.workstreamB],
      epics: [BoardFixtures.epicB],
      issues: [BoardFixtures.issueB],
      jobs: [],
      sessions: [])
    board.apply(onlyB)
    #expect(board.workstreams.map(\.id) == [WorkstreamId(rawValue: "ws-b")])
    #expect(board.workstreams.first?.status == .complete)
  }

  /// `start` is idempotent while running (a second call is a no-op, never a
  /// cancel-and-respin that would blank the single-consumer feed); `stop` cancels;
  /// and a reconnect via a FRESH feed re-consumes deterministically. Uses distinct
  /// feeds per `start` (WorkGraphResync is single-consumer), so this validates the
  /// contract without relying on cross-test scheduling.
  @Test("start is idempotent; stop then start on a fresh feed re-consumes")
  func startAndStopLifecycle() async throws {
    let backend1 = ScriptedBackend(snapshot: BoardFixtures.snapshot)
    let feed1 = WorkGraphResync(connect: { backend1 }, backoff: .noDelay)
    let board = MissionControlBoard()

    board.start(feed1)
    // A second start while already consuming is a NO-OP — the first driver keeps
    // projecting the single-consumer feed (re-consuming it would blank the board).
    board.start(feed1)
    #expect(await waitUntil(board) { !$0.isEmpty })

    board.stop()
    board.stop()  // idempotent

    // Reconnect: a freshly-constructed feed re-consumes deterministically.
    let backend2 = ScriptedBackend(snapshot: BoardFixtures.snapshot)
    let feed2 = WorkGraphResync(connect: { backend2 }, backoff: .noDelay)
    board.start(feed2)
    #expect(await waitUntil(board) { !$0.isEmpty })

    board.stop()
    await backend1.close()
    await backend2.close()
  }

  /// Polls the main-actor board until `predicate` holds, yielding between checks so
  /// the feed-consumption task can run. Returns `false` if the bound is exhausted.
  private func waitUntil(
    _ board: MissionControlBoard,
    _ predicate: ([BoardWorkstream]) -> Bool
  ) async -> Bool {
    for _ in 0..<100_000 {
      if predicate(board.workstreams) { return true }
      await Task.yield()
    }
    return false
  }
}
