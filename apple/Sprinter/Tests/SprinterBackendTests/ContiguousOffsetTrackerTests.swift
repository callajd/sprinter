import Testing

@testable import SprinterBackend

@Suite("Contiguous-prefix offset tracking")
struct ContiguousOffsetTrackerTests {
  @Test("an origin tracker seeds the prefix at 0 (nothing applied), then advances in order")
  func inOrderAdvances() {
    var tracker = ContiguousOffsetTracker()
    // Origin: nothing applied yet, so the prefix seeds at 0 (durable offsets are > 0).
    #expect(tracker.contiguous == 0)
    tracker.observe(1)
    #expect(tracker.contiguous == 1)
    tracker.observe(2)
    #expect(tracker.contiguous == 2)
    tracker.observe(3)
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
  func higherBeforeLowerAtOriginDiscardsNothing() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(5)
    tracker.observe(3)  // BELOW the max seen (5) but ABOVE the prefix (0): a real event.
    tracker.observe(4)
    // The prefix stays at 0: offsets 1 and 2 are genuinely un-seen (this is an origin
    // start), so nothing may be assumed applied below the seed. Crucially, 3/4/5 were NOT
    // discarded — they are held ahead.
    #expect(tracker.contiguous == 0)
    // Filling the true origin run advances the prefix across ALL of 1…5 — 3, 4, 5 were
    // retained the whole time, never lost as duplicates.
    tracker.observe(1)
    tracker.observe(2)
    #expect(tracker.contiguous == 5)
  }

  @Test("an offset ahead of a gap does NOT advance the prefix past the gap")
  func gapHoldsThePrefix() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(1)
    tracker.observe(2)
    tracker.observe(4)  // offset 3 is missing.
    // The prefix is 2 (not the max, 4): every offset ≤ 2 is seen, but 3 is not.
    #expect(tracker.contiguous == 2)
  }

  @Test("filling the gap advances the prefix across the held-ahead offsets")
  func fillingGapAdvances() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(1)
    tracker.observe(2)
    tracker.observe(4)
    tracker.observe(5)
    #expect(tracker.contiguous == 2)  // still blocked on 3.
    tracker.observe(3)  // the gap fills: 3, 4, 5 all join the prefix.
    #expect(tracker.contiguous == 5)
  }

  @Test("out-of-order arrival within a gapless run still yields the right prefix")
  func outOfOrderWithinRun() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(2)
    tracker.observe(1)
    tracker.observe(3)
    #expect(tracker.contiguous == 3)
  }

  @Test("a duplicate at or below the prefix is ignored")
  func duplicateIgnored() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(1)
    tracker.observe(2)
    tracker.observe(1)  // replay duplicate.
    tracker.observe(2)
    #expect(tracker.contiguous == 2)
  }

  @Test("a reconnect tracker resumes strictly after the seed cursor")
  func resumeSeedsPrefix() {
    var tracker = ContiguousOffsetTracker(resumingAfter: 10)
    #expect(tracker.contiguous == 10)
    tracker.observe(12)  // gap at 11.
    #expect(tracker.contiguous == 10)
    tracker.observe(11)
    #expect(tracker.contiguous == 12)
  }

  @Test("a reconnect tracker ignores re-sent offsets at or below its cursor")
  func resumeIgnoresReplayed() {
    var tracker = ContiguousOffsetTracker(resumingAfter: 5)
    tracker.observe(3)  // a re-sent already-applied delta.
    tracker.observe(5)
    #expect(tracker.contiguous == 5)
    tracker.observe(6)
    #expect(tracker.contiguous == 6)
  }
}
