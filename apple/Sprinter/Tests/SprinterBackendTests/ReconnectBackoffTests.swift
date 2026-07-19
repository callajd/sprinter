import Testing

@testable import SprinterBackend

@Suite("Reconnect backoff + jitter schedule")
struct ReconnectBackoffTests {
  /// With identity jitter, `next()` returns the raw ceiling — so the schedule doubles per
  /// consecutive failure and caps at `maximum` (deterministic, no wall-clock).
  @Test("ceilings grow exponentially and cap at the maximum")
  func exponentialCappedSchedule() {
    var backoff = ReconnectBackoff(
      base: .seconds(1), maximum: .seconds(10), multiplier: 2, jitter: { $0 })
    #expect(backoff.next() == .seconds(1))
    #expect(backoff.next() == .seconds(2))
    #expect(backoff.next() == .seconds(4))
    #expect(backoff.next() == .seconds(8))
    #expect(backoff.next() == .seconds(10))  // 16 capped to the maximum.
    #expect(backoff.next() == .seconds(10))  // stays capped.
  }

  @Test("a reset returns the schedule to the base delay")
  func resetReturnsToBase() {
    var backoff = ReconnectBackoff(
      base: .seconds(1), maximum: .seconds(30), multiplier: 2, jitter: { $0 })
    _ = backoff.next()  // 1
    _ = backoff.next()  // 2
    _ = backoff.next()  // 4
    backoff.reset()
    #expect(backoff.next() == .seconds(1))
    #expect(backoff.next() == .seconds(2))
  }

  /// Full jitter picks the actual delay uniformly in `[0, ceiling]`, so every drawn delay
  /// stays within the (growing, capped) window — no synchronized thundering herd.
  @Test("full jitter stays within [0, ceiling] across the widening schedule")
  func fullJitterWithinWindow() {
    var backoff = ReconnectBackoff(base: .seconds(1), maximum: .seconds(8), multiplier: 2)
    for _ in 0..<100 {
      let delay = backoff.next()
      #expect(delay >= .zero)
      #expect(delay <= .seconds(8))
    }
  }

  @Test("the no-delay schedule always yields zero")
  func noDelayIsZero() {
    var backoff = ReconnectBackoff.noDelay
    #expect(backoff.next() == .zero)
    #expect(backoff.next() == .zero)
    backoff.reset()
    #expect(backoff.next() == .zero)
  }
}
