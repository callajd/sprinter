import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterSession

/// Lifecycle tests for the session view model — the carried BE2 single-consumer
/// constraint. Each test is self-contained and deterministic (its own fake, its own
/// polling to a fixed point), so it is green run **in isolation**
/// (`swift test --filter SessionLifecycleTests`), never only by parallel-suite
/// scheduling luck.
@Suite("Session view model lifecycle")
@MainActor
struct SessionLifecycleTests {
  private static let session = SessionId(rawValue: "session-a")

  /// Before the first `start`, the model is `.idle`.
  @Test("before start the model is idle")
  func idleBeforeStart() async {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)
    #expect(model.lifecycle == .idle)
    await backend.close()
  }

  /// `start` subscribes exactly one feed and goes `.live`; a **second `start` while
  /// running is a no-op** — it does NOT re-subscribe (single-consumer), so the feed
  /// count stays 1 and an emitted event appears exactly once.
  @Test("start is idempotent — a second start does not re-subscribe")
  func startIsIdempotent() async {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)

    model.start()
    #expect(model.lifecycle == .live)
    #expect(await waitUntil { backend.feedCount == 1 })

    // A second start while running must not respin the single-consumer feed.
    model.start()
    backend.emit(.notice(level: .info, message: "once"))
    #expect(await waitUntil { model.transcript.items.count == 1 })

    // Still one feed, and the event surfaced exactly once — no duplicate feed.
    #expect(backend.feedCount == 1)
    #expect(model.transcript.items.count == 1)

    model.stop()
    await backend.close()
  }

  /// A clean feed end (the session ended) leaves the model `.ended` with no
  /// termination error — distinguishable from a drop.
  @Test("a clean feed end leaves the model ended")
  func cleanEndIsEnded() async {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)

    model.start()
    #expect(await waitUntil { backend.feedCount == 1 })
    backend.finish()

    #expect(await waitUntil { model.lifecycle == .ended })
    #expect(model.terminationError == nil)
    await backend.close()
  }

  /// A feed drop (a transport failure) leaves the model `.dropped` with the cause on
  /// `terminationError` — distinguishable from a clean end.
  @Test("a dropped feed leaves the model dropped with the cause")
  func dropIsDropped() async {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)

    model.start()
    #expect(await waitUntil { backend.feedCount == 1 })
    backend.fail(BackendError.connectionClosed)

    #expect(await waitUntil { model.lifecycle == .dropped })
    #expect(model.terminationError as? BackendError == .connectionClosed)
    await backend.close()
  }

  /// After a `stop`, a `start` subscribes a **fresh** feed (a new subscription), and
  /// events on the new feed flow — the per-(re)start feed.
  @Test("restart subscribes a fresh feed")
  func restartSubscribesFreshFeed() async {
    let backend = SessionFakeBackend(knownSession: Self.session)
    let model = SessionViewModel(backend: backend, sessionId: Self.session)

    model.start()
    #expect(await waitUntil { backend.feedCount == 1 })
    model.stop()
    #expect(model.lifecycle == .ended)

    model.start()
    #expect(await waitUntil { backend.feedCount == 2 })
    backend.emit(.notice(level: .info, message: "after restart"))
    #expect(await waitUntil { model.transcript.items.count == 1 })

    model.stop()
    await backend.close()
  }

  /// Polls until `predicate` holds, yielding between checks so the model's feed task
  /// can run. Returns `false` if the bound is exhausted.
  private func waitUntil(_ predicate: () -> Bool) async -> Bool {
    for _ in 0..<100_000 {
      if predicate() { return true }
      await Task.yield()
    }
    return false
  }
}
