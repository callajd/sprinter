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
/// **The prefix is seeded from a KNOWN resume point, never inferred from the data.** The
/// tracker must NOT seed from the first arbitrary offset observed: the eager live-tail
/// subscribe can interleave a live event with a HIGHER offset before the ordered replay
/// delivers a LOWER one (5 before 3), so seeding from the first arrival would set the
/// prefix to 5 and then discard the genuinely-unapplied 3 as a "duplicate" — a lost event
/// at the origin boundary, the exact loss class this tracker exists to prevent. Instead:
///
/// - **Origin subscription** (`sinceOffset: nil`, nothing applied yet): the prefix seeds
///   at `0`. Durable offsets are strictly `> 0` (the daemon's journal starts at 1), so `0`
///   means "nothing applied" and the prefix advances only as a gapless run up from `1`. A
///   lower offset arriving after a higher one is still `> 0` (above the prefix), so it is a
///   real un-applied event and is retained/applied — never discarded. If the ordered
///   origin replay has not yet filled the low offsets, the prefix simply stays low and a
///   reconnect resumes from there (the daemon re-sends gaplessly — heavier, never lossy).
/// - **Reconnect** (resuming strictly after a known cursor): the prefix seeds at that
///   last-applied offset.
struct ContiguousOffsetTracker: Sendable, Equatable {
  /// Offsets observed strictly above ``contiguous`` that have not yet joined the
  /// gapless prefix (out-of-order arrivals waiting for their gap to fill). Pruned as
  /// the prefix advances across them, so it holds only the un-filled tail.
  private var ahead: Set<Int> = []

  /// The highest offset such that it and every offset below it (down to the origin)
  /// have been observed — the cursor a reconnect resumes strictly after. Seeded from a
  /// KNOWN point (`0` for an origin subscription, the last-applied offset for a
  /// reconnect), never inferred from the first observed offset.
  private(set) var contiguous: Int

  /// Seeds an origin-replay tracker: nothing has been applied, so the prefix starts at
  /// `0` (durable offsets are `> 0`, so `0` is strictly below every real event). The
  /// prefix then advances ONLY as a gapless run up from `1` — never inferred from the
  /// first arbitrary offset observed, so a lower offset arriving after a higher one is
  /// never mistaken for a duplicate and discarded.
  init() {
    contiguous = 0
  }

  /// Seeds a reconnect tracker resuming strictly after `offset`: the next contiguous
  /// offset expected is `offset + 1`.
  init(resumingAfter offset: Int) {
    contiguous = offset
  }

  /// Records one applied offset and advances the gapless prefix as far as the newly
  /// observed offsets allow. An offset at or below the current prefix is a replay
  /// duplicate (already applied, already covered) and is ignored; an offset ABOVE it is
  /// a genuinely un-applied event (even if it is below the max seen) and is retained.
  mutating func observe(_ offset: Int) {
    guard offset > contiguous else { return }
    ahead.insert(offset)
    advance()
  }

  /// Extends ``contiguous`` across every consecutive offset now present in ``ahead``
  /// starting from the KNOWN prefix (`contiguous + 1`), removing them as it goes so
  /// ``ahead`` retains only the un-filled tail. The run begins at the seeded prefix — it
  /// is never inferred from ``ahead``'s minimum.
  private mutating func advance() {
    var next = contiguous + 1
    while ahead.remove(next) != nil {
      contiguous = next
      next += 1
    }
  }
}
