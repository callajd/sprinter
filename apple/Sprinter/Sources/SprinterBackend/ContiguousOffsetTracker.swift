/// Tracks the **contiguous-prefix** durable offset a reconnecting client may safely
/// resume from (the CE2.0-review carried correctness constraint).
///
/// The daemon's LIVE `events` feed can publish OUT of durable-offset order: two
/// concurrent writers (the command handler and the `JobRunner`) both journal, and
/// `event-journal.ts` publishes each delta AFTER its own transaction commits, so a
/// higher offset can reach the client before a lower one that is still in flight. A
/// client that resumed from the **maximum** offset seen would therefore skip an event
/// that is `≤ max` but had not yet arrived (a lost event on reconnect).
///
/// So this tracks the highest `N` such that EVERY offset `≤ N` has been observed — the
/// gapless prefix — and never the max. Offsets that arrive ahead of a gap are held in
/// ``ahead`` until the gap fills, at which point the prefix advances across them. On
/// reconnect the client hands ``contiguous`` back as `sinceOffset`; the daemon replays
/// strictly after it, re-sending any held-ahead offsets (whose re-application is a
/// harmless idempotent upsert). Nothing `≤ contiguous` can be unseen, so nothing is lost.
///
/// Origin: on a fresh (origin-replay) subscription the resume cursor is unknown, so the
/// smallest offset ever observed seeds the prefix — the daemon replays its durable log
/// in order from the origin, so the first offsets arrive gaplessly and the minimum seen
/// IS the origin. On reconnect the prefix is seeded from the resume offset instead.
struct ContiguousOffsetTracker: Sendable, Equatable {
  /// Offsets observed strictly above ``contiguous`` that have not yet joined the
  /// gapless prefix (out-of-order arrivals waiting for their gap to fill). Pruned as
  /// the prefix advances across them, so it holds only the un-filled tail.
  private var ahead: Set<Int> = []

  /// The highest offset such that it and every offset below it (down to the origin)
  /// have been observed — the cursor a reconnect resumes strictly after. `nil` until
  /// the first offset is seen on an origin-replay subscription.
  private(set) var contiguous: Int?

  /// Seeds an origin-replay tracker (no known cursor: the prefix starts at the
  /// smallest offset observed).
  init() {}

  /// Seeds a reconnect tracker resuming strictly after `offset`: the next contiguous
  /// offset expected is `offset + 1`.
  init(resumingAfter offset: Int?) {
    contiguous = offset
  }

  /// Records one applied offset and advances the gapless prefix as far as the newly
  /// observed offsets allow. An offset at or below the current prefix is a replay
  /// duplicate (already applied, already covered) and is ignored.
  mutating func observe(_ offset: Int) {
    if let contiguous, offset <= contiguous {
      return
    }
    ahead.insert(offset)
    advance()
  }

  /// Extends ``contiguous`` across every consecutive offset now present in ``ahead``,
  /// removing them as it goes so ``ahead`` retains only the un-filled tail.
  private mutating func advance() {
    var next: Int
    if let contiguous {
      next = contiguous + 1
    } else {
      // No prefix yet: the run must begin at the smallest offset seen (the origin of
      // the in-order durable replay).
      guard let start = ahead.min() else { return }
      next = start
    }
    while ahead.remove(next) != nil {
      contiguous = next
      next += 1
    }
  }
}
