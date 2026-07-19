import Testing

@testable import SprinterBackend

@Suite("Contiguous-prefix offset tracking")
struct ContiguousOffsetTrackerTests {
  @Test("in-order arrivals advance the prefix one step at a time")
  func inOrderAdvances() {
    var tracker = ContiguousOffsetTracker()
    #expect(tracker.contiguous == nil)
    tracker.observe(0)
    #expect(tracker.contiguous == 0)
    tracker.observe(1)
    #expect(tracker.contiguous == 1)
    tracker.observe(2)
    #expect(tracker.contiguous == 2)
  }

  @Test("the smallest offset seen seeds the prefix origin")
  func minimumSeeds() {
    var tracker = ContiguousOffsetTracker()
    tracker.observe(5)
    #expect(tracker.contiguous == 5)
    tracker.observe(6)
    #expect(tracker.contiguous == 6)
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
