import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterAppSupport

@Suite("App model composition")
@MainActor
struct AppModelTests {
  /// `start()` subscribes the board to the live feed AND dials a backend: the board
  /// populates from the snapshot and the connection resolves to `.connected` with a
  /// backend, all off a fake connect seam.
  @Test("start populates the board and connects a backend")
  func startPopulatesAndConnects() async throws {
    let fake = FakeBackend(snapshot: AppSupportFixtures.snapshot)
    let model = AppModel(daemon: DaemonConnection(connect: { fake }))

    model.start()

    #expect(await waitUntil { model.connectionState == .connected })
    #expect(model.backend != nil)
    #expect(await waitUntil { !model.board.workstreams.isEmpty })

    model.stop()
    #expect(await waitUntil { fake.wasClosed })
  }

  /// A failed connect surfaces as `.failed` with no backend, so the shell can render the
  /// reason rather than crash — the connect step never force-unwraps a connection.
  @Test("a failed connect surfaces as .failed with no backend")
  func failedConnectSurfaces() async throws {
    struct DialError: Error {}
    let model = AppModel(daemon: DaemonConnection(connect: { throw DialError() }))

    model.start()

    #expect(
      await waitUntil {
        if case .failed = model.connectionState { return true }
        return false
      })
    #expect(model.backend == nil)

    model.stop()
  }

  /// `start()` is idempotent: a second call does not re-dial while a connect is
  /// outstanding, and `stop()` resets the state so the app can be restarted.
  @Test("start is idempotent; stop resets")
  func startIsIdempotentStopResets() async throws {
    let fake = FakeBackend(snapshot: AppSupportFixtures.snapshot)
    let model = AppModel(daemon: DaemonConnection(connect: { fake }))

    model.start()
    model.start()  // no-op while connecting/connected
    #expect(await waitUntil { model.connectionState == .connected })

    model.stop()
    #expect(model.backend == nil)
    #expect(model.connectionState == .connecting)
    #expect(await waitUntil { fake.wasClosed })
  }

  /// A daemon RESTART (the session backend drops) is SELF-HEALED: the reconnect loop
  /// observes the drop via the connection's liveness watch and RE-DIALS a fresh backend, so
  /// the shell recovers to `.connected` instead of staying stuck on a dead/`nil` backend
  /// (CE3.1-F2 restart watch-item — the session-channel side of restart safety).
  @Test("a dropped session backend is re-dialed (restart self-heal)")
  func reDialsAfterDrop() async throws {
    let model = AppModel(
      daemon: DaemonConnection(connect: { FakeBackend(snapshot: AppSupportFixtures.snapshot) }),
      reconnectBackoff: .noDelay)

    model.start()
    #expect(await waitUntil { model.connectionState == .connected })
    let first = try #require(model.backend as? FakeBackend)

    // The daemon goes away mid-connection: drop the current session backend.
    first.simulateDrop()

    // The loop re-dials → a fresh, DISTINCT backend, connected again (not stuck on nil).
    #expect(
      await waitUntil {
        guard let current = model.backend as? FakeBackend else { return false }
        return current !== first
      })
    #expect(await waitUntil { model.connectionState == .connected })

    model.stop()
  }

  /// A daemon that is not up at launch does not leave the shell permanently `.failed`: the
  /// reconnect loop retries (with backoff) and reaches `.connected` once a dial succeeds —
  /// so a late-starting daemon is still reached.
  @Test("an initial failed dial is retried until connected")
  func retriesAfterInitialFailure() async throws {
    struct DialError: Error {}
    let dials = DialCounter(failFirst: 1)
    let model = AppModel(
      daemon: DaemonConnection(connect: {
        if dials.shouldFail() { throw DialError() }
        return FakeBackend(snapshot: AppSupportFixtures.snapshot)
      }),
      reconnectBackoff: .noDelay)

    model.start()

    // Despite the first dial throwing, the loop retries and connects a backend.
    #expect(await waitUntil { model.connectionState == .connected })
    #expect(model.backend != nil)

    model.stop()
  }

  /// `makeWorkGraphFeed()` mints a fresh single-consumer feed (for an inspector PR pane)
  /// off the same seam — a distinct engine that also drives to a baseline.
  @Test("makeWorkGraphFeed yields a fresh, drivable feed")
  func makeWorkGraphFeedYieldsFreshFeed() async throws {
    let fake = FakeBackend(snapshot: AppSupportFixtures.snapshot)
    let model = AppModel(daemon: DaemonConnection(connect: { fake }))

    let board = SprinterMissionControlProbe()
    await board.consumeFirst(model.makeWorkGraphFeed())
    #expect(board.sawBaseline)

    await fake.close()
  }
}

/// A minimal probe that consumes one baseline from a fresh feed — asserts
/// `makeWorkGraphFeed()` produced a live, drivable engine without pulling in the board
/// view model's full lifecycle.
@MainActor
final class SprinterMissionControlProbe {
  private(set) var sawBaseline = false

  func consumeFirst(_ feed: WorkGraphResync) async {
    for await _ in await feed.states() {
      sawBaseline = true
      await feed.stop()
      break
    }
  }
}
