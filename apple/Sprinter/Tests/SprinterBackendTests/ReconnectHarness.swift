import Foundation

@testable import SprinterBackend

/// A `connect` seam for ``WorkGraphResync`` that vends a fresh
/// ``FakeTransport``-backed ``RpcBackend`` per (re)connect attempt and publishes
/// each transport, so a test can script the server side of every attempt offline
/// (no live daemon/network).
final class ReconnectHarness: Sendable {
  /// Each transport the engine connected over, in attempt order.
  let transports: AsyncStream<FakeTransport>
  private let continuation: AsyncStream<FakeTransport>.Continuation

  init() {
    (transports, continuation) = AsyncStream<FakeTransport>.makeStream()
  }

  /// The `connect` closure to hand ``WorkGraphResync``.
  func connect() async throws -> any Backend {
    let transport = FakeTransport()
    continuation.yield(transport)
    return RpcBackend(transport: transport)
  }
}

/// A `Sendable` call counter for gating a test observer deterministically.
actor CallCounter {
  private var count = 0
  func increment() -> Int {
    count += 1
    return count
  }
  /// The current count without mutating it (for asserting a callback did/did not fire).
  var value: Int { count }
}

/// A manually-advanced ``Clock`` for deterministic connection-health tests: `now` moves ONLY
/// when the test calls ``advance(by:)``, so an attempt's established-duration is set exactly
/// by the test with no wall-clock wait and no hang. ``WorkGraphResync`` reads `now` from this
/// clock synchronously at an attempt's teardown to score health, so advancing past
/// `minHealthyDuration` before dropping a connection deterministically marks it healthy, and
/// leaving `now` unchanged marks an instant drop a flap.
///
/// `sleep` is unused by the engine on this clock (the reconnect delay is scheduled through
/// ``ReconnectBackoff``, neutralized to zero in tests), so it is a bounded no-op that only
/// honors cancellation — never a source of an unbounded wait.
final class ManualClock: Clock, @unchecked Sendable {
  struct Instant: InstantProtocol {
    let uptime: Duration
    func advanced(by duration: Duration) -> Instant { Instant(uptime: uptime + duration) }
    func duration(to other: Instant) -> Duration { other.uptime - uptime }
    static func < (lhs: Instant, rhs: Instant) -> Bool { lhs.uptime < rhs.uptime }
  }

  private let lock = NSLock()
  private var current = Instant(uptime: .zero)

  var now: Instant {
    lock.lock()
    defer { lock.unlock() }
    return current
  }

  var minimumResolution: Duration { .zero }

  /// Moves `now` forward by `duration` (the only way this clock advances).
  func advance(by duration: Duration) {
    lock.lock()
    current = current.advanced(by: duration)
    lock.unlock()
  }

  func sleep(until deadline: Instant, tolerance: Duration?) async throws {
    try Task.checkCancellation()
  }
}
