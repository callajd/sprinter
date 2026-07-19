import Foundation

/// The reconnect delay schedule for ``WorkGraphResync``: **exponential backoff with
/// jitter** against a persistently-failing daemon (CE2.2 deliverable), replacing the
/// old constant `retryDelay`.
///
/// Each failed reconnect widens the ceiling geometrically — `base`, `base·multiplier`,
/// `base·multiplier²`, … capped at `maximum` — so a daemon that stays down is retried
/// ever less aggressively instead of hammered on a tight constant loop. **Full jitter**
/// then picks the actual delay uniformly in `[0, ceiling]`, so a fleet of clients that
/// dropped together does not reconnect in a synchronized thundering herd. A successful
/// connection ``reset()``s the schedule, so a healthy connection that later drops
/// reconnects promptly from `base` rather than inheriting a widened delay.
///
/// The jitter source is injected (defaulting to the system RNG) so the schedule is
/// exercised deterministically offline: a test can supply an identity jitter to assert
/// the exact exponential ceilings, or a fixed fraction to assert placement within the
/// window — no wall-clock flakiness.
public struct ReconnectBackoff: Sendable {
  /// The first (un-widened) ceiling.
  public let base: Duration
  /// The ceiling cap — the schedule never widens beyond this.
  public let maximum: Duration
  /// The geometric growth factor applied per consecutive failure.
  public let multiplier: Double
  /// Maps a computed ceiling to the actual delay (full jitter by default).
  private let jitter: @Sendable (Duration) -> Duration
  /// The number of consecutive failures so far (drives the exponent).
  private var failures: Int = 0

  /// Builds a schedule. Defaults: 250 ms base, 30 s cap, doubling, full jitter.
  public init(
    base: Duration = .milliseconds(250),
    maximum: Duration = .seconds(30),
    multiplier: Double = 2,
    jitter: @escaping @Sendable (Duration) -> Duration = ReconnectBackoff.fullJitter
  ) {
    self.base = base
    self.maximum = maximum
    self.multiplier = multiplier
    self.jitter = jitter
  }

  /// The ceiling for the current failure count: `base · multiplier^failures`, capped at
  /// `maximum` (and never negative).
  var ceiling: Duration {
    let widened = base.seconds * pow(multiplier, Double(failures))
    let capped = min(max(widened, 0), maximum.seconds)
    return .seconds(capped)
  }

  /// Returns the next delay (jitter applied to the current ceiling) and widens the
  /// schedule by one step for the following failure.
  public mutating func next() -> Duration {
    let delay = jitter(ceiling)
    failures += 1
    return delay
  }

  /// Resets the schedule to `base` — called on a successful connection so a later drop
  /// reconnects promptly rather than inheriting a widened delay.
  public mutating func reset() {
    failures = 0
  }

  /// Full jitter: a delay drawn uniformly from `[0, ceiling]`.
  public static let fullJitter: @Sendable (Duration) -> Duration = { ceiling in
    let upper = ceiling.seconds
    guard upper > 0 else { return .zero }
    return .seconds(Double.random(in: 0...upper))
  }

  /// A no-delay schedule (zero ceiling, identity jitter) for tests that drive reconnects
  /// in lockstep without waiting on wall-clock time.
  public static let noDelay = ReconnectBackoff(
    base: .zero, maximum: .zero, multiplier: 1, jitter: { $0 })
}

extension Duration {
  /// This duration as fractional seconds (for the geometric ceiling math).
  fileprivate var seconds: Double {
    let (whole, attos) = components
    return Double(whole) + Double(attos) / 1e18
  }
}
