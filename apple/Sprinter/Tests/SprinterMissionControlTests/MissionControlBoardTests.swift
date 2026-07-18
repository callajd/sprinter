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
    let feed = WorkGraphResync(connect: { backend }, retryDelay: .zero)
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

  /// `start` replaces a prior driver and `stop` cancels it; the feed finishing
  /// leaves the board on its last projection.
  @Test("start replaces the driver and stop cancels consumption")
  func startAndStopLifecycle() async throws {
    let backend = ScriptedBackend(snapshot: BoardFixtures.snapshot)
    let feed = WorkGraphResync(connect: { backend }, retryDelay: .zero)
    let board = MissionControlBoard()

    board.start(feed)
    // Calling start again replaces the prior driver (no crash, still projects).
    board.start(feed)
    #expect(await waitUntil(board) { !$0.isEmpty })

    board.stop()
    board.stop()  // idempotent
    await backend.close()
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
