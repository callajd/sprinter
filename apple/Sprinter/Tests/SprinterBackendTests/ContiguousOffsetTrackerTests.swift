import Testing

@testable import SprinterBackend

@Suite("Contiguous-prefix offset tracking")
struct ContiguousOffsetTrackerTests {
  @Test("an origin tracker seeds the prefix at 0 (nothing applied), then advances in order")
  func inOrderAdvances() throws {
    var tracker = ContiguousOffsetTracker()
    // Origin: nothing applied yet, so the prefix seeds at 0 (durable offsets are > 0).
    #expect(tracker.contiguous == 0)
    try tracker.observe(1)
    #expect(tracker.contiguous == 1)
    try tracker.observe(2)
    #expect(tracker.contiguous == 2)
    try tracker.observe(3)
    #expect(tracker.contiguous == 3)
  }

  /// FIX3 — the offset-tracker loss hole at the origin boundary. A HIGHER offset observed
  /// before a LOWER one at the very start (5 before 3) must NOT seed the prefix from the
  /// first arrival and then discard the later 3 as a duplicate. From an origin start the
  /// prefix is seeded at 0, so 3, 4, 5 are ALL above it → all genuinely un-applied → none
  /// discarded. The prefix legitimately stays at 0 (offsets 1, 2 have not been seen); once
  /// they fill in, the whole held-ahead run (3, 4, 5) joins the prefix — proving nothing
  /// was dropped.
  @Test("a higher offset before a lower one at origin discards nothing (the loss hole)")
  func higherBeforeLowerAtOriginDiscardsNothing() throws {
    var tracker = ContiguousOffsetTracker()
    try tracker.observe(5)
    try tracker.observe(3)  // BELOW the max seen (5) but ABOVE the prefix (0): a real event.
    try tracker.observe(4)
    // The prefix stays at 0: offsets 1 and 2 are genuinely un-seen (this is an origin
    // start), so nothing may be assumed applied below the seed. Crucially, 3/4/5 were NOT
    // discarded — they are held ahead.
    #expect(tracker.contiguous == 0)
    // Filling the true origin run advances the prefix across ALL of 1…5 — 3, 4, 5 were
    // retained the whole time, never lost as duplicates.
    try tracker.observe(1)
    try tracker.observe(2)
    #expect(tracker.contiguous == 5)
  }

  @Test("an offset ahead of a gap does NOT advance the prefix past the gap")
  func gapHoldsThePrefix() throws {
    var tracker = ContiguousOffsetTracker()
    try tracker.observe(1)
    try tracker.observe(2)
    try tracker.observe(4)  // offset 3 is missing.
    // The prefix is 2 (not the max, 4): every offset ≤ 2 is seen, but 3 is not.
    #expect(tracker.contiguous == 2)
  }

  @Test("filling the gap advances the prefix across the held-ahead offsets")
  func fillingGapAdvances() throws {
    var tracker = ContiguousOffsetTracker()
    try tracker.observe(1)
    try tracker.observe(2)
    try tracker.observe(4)
    try tracker.observe(5)
    #expect(tracker.contiguous == 2)  // still blocked on 3.
    try tracker.observe(3)  // the gap fills: 3, 4, 5 all join the prefix.
    #expect(tracker.contiguous == 5)
  }

  @Test("out-of-order arrival within a gapless run still yields the right prefix")
  func outOfOrderWithinRun() throws {
    var tracker = ContiguousOffsetTracker()
    try tracker.observe(2)
    try tracker.observe(1)
    try tracker.observe(3)
    #expect(tracker.contiguous == 3)
  }

  @Test("a duplicate at or below the prefix is ignored")
  func duplicateIgnored() throws {
    var tracker = ContiguousOffsetTracker()
    try tracker.observe(1)
    try tracker.observe(2)
    try tracker.observe(1)  // replay duplicate.
    try tracker.observe(2)
    #expect(tracker.contiguous == 2)
  }

  @Test("a reconnect tracker resumes strictly after the seed cursor")
  func resumeSeedsPrefix() throws {
    var tracker = ContiguousOffsetTracker(resumingAfter: 10)
    #expect(tracker.contiguous == 10)
    try tracker.observe(12)  // gap at 11.
    #expect(tracker.contiguous == 10)
    try tracker.observe(11)
    #expect(tracker.contiguous == 12)
  }

  @Test("a reconnect tracker ignores re-sent offsets at or below its cursor")
  func resumeIgnoresReplayed() throws {
    var tracker = ContiguousOffsetTracker(resumingAfter: 5)
    try tracker.observe(3)  // a re-sent already-applied delta.
    try tracker.observe(5)
    #expect(tracker.contiguous == 5)
    try tracker.observe(6)
    #expect(tracker.contiguous == 6)
  }

  /// FIX B — the bounded-gap guard. A live-tail offset can be PERMANENTLY missing within a
  /// connection (the daemon commits the durable entry before publishing the live delta and
  /// does not guarantee a gapless live tail). Without a bound the prefix would stall behind
  /// that hole while `ahead` accumulated every later offset unbounded and `contiguous`
  /// froze. With the bound, once the un-filled ahead-of-gap window exceeds `gapLimit`,
  /// `observe` throws ``ContiguousOffsetTracker/StalledGap`` — the signal the consumer turns
  /// into a resync from the frozen cursor, rather than freezing forever. Deterministic: a
  /// fixed missing offset (2) with a small `gapLimit`, fed a bounded run of higher offsets.
  @Test("a permanently-missing offset trips the bounded-gap guard instead of growing unbounded")
  func permanentGapStalls() throws {
    var tracker = ContiguousOffsetTracker(gapLimit: 3)
    try tracker.observe(1)  // prefix advances to 1.
    #expect(tracker.contiguous == 1)
    // Offset 2 never arrives; offsets 3…5 pile up ahead of the gap (ahead = {3,4,5}, at the
    // cap of 3) — the prefix stays frozen at 1, but no throw yet.
    try tracker.observe(3)
    try tracker.observe(4)
    try tracker.observe(5)
    #expect(tracker.contiguous == 1)
    // The next held-ahead offset pushes the window past the cap: the gap is deemed un-fillable
    // on the live tail, so the tracker signals a resync rather than growing `ahead` further.
    #expect(throws: ContiguousOffsetTracker.StalledGap.self) {
      try tracker.observe(6)
    }
    // The cursor is still the frozen prefix — a resync resumes strictly after it (1), so the
    // daemon re-sends the missing 2 from the durable log.
    #expect(tracker.contiguous == 1)
  }

  /// The guard must NOT trip while a gap is merely large-but-filling: a run that stays within
  /// the window and then fills advances cleanly, no spurious resync.
  @Test("a large but eventually-filled gap does not trip the guard")
  func fillingLargeGapDoesNotStall() throws {
    var tracker = ContiguousOffsetTracker(gapLimit: 4)
    try tracker.observe(2)
    try tracker.observe(3)
    try tracker.observe(4)  // ahead = {2,3,4}, within the cap; prefix frozen at 0.
    #expect(tracker.contiguous == 0)
    try tracker.observe(1)  // the gap fills: 1…4 all join the prefix, ahead drains.
    #expect(tracker.contiguous == 4)
  }
}
