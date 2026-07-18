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
}
