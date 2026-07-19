import SprinterContract
import Testing

@testable import SprinterSession

/// CE3.2 transcript-projection memoization correctness: the incremental
/// ``TranscriptProjection/Memo`` must equal a from-scratch ``TranscriptProjection``
/// fold for ANY event sequence, so it can avoid the O(N²) re-fold on SwiftUI re-reads
/// without ever diverging from re-folding the whole feed.
@Suite("Transcript projection memo")
struct TranscriptProjectionMemoTests {
  /// The memo equals a from-scratch fold at EVERY prefix of a gnarly feed — including
  /// out-of-order (result-before-call), duplicate id-less notices, and a stale delta
  /// after finalize — so continuing the fold never diverges from re-folding.
  @Test("the memo equals a from-scratch fold at every prefix of a gnarly feed")
  func memoEqualsFullFoldAtEveryPrefix() {
    let events: [SessionEvent] = [
      .turnStarted,
      .messageStarted(messageId: "m1"),
      .messageDelta(messageId: "m1", text: "Hel", reasoning: "th"),
      // Out-of-order: the durable tool RESULT arrives before its CALL.
      .entryAppended(entry: .toolResult(id: "t1", output: .string("out"), isError: false)),
      .messageDelta(messageId: "m1", text: "lo", reasoning: "ink"),
      .entryAppended(entry: .toolCall(id: "t1", name: "read", input: .string("in"))),
      // Duplicate-content id-less notices stay distinct (arrival-sequence keyed).
      .notice(id: nil, level: .info, message: "same"),
      .notice(id: nil, level: .info, message: "same"),
      .messageCompleted(messageId: "m1"),
      // Durable entry finalizes, then a STALE delta must be ignored.
      .entryAppended(entry: .assistantMessage(id: "m1", text: "Final", reasoning: "r")),
      .messageDelta(messageId: "m1", text: " STALE", reasoning: nil),
      .turnCompleted(usage: nil),
      .statusChanged(key: "phase", text: "done")
    ]
    let memo = TranscriptProjection.Memo()
    for count in 0...events.count {
      let prefix = Array(events.prefix(count))
      #expect(memo.project(prefix) == TranscriptProjection.project(prefix))
    }
  }

  /// A re-read at the SAME event count returns the cached projection (the O(N²)→O(N)
  /// win: SwiftUI re-reads don't re-fold), and it equals a from-scratch fold.
  @Test("a re-read at the same event count returns the cached, correct projection")
  func memoCachesAtSameCount() {
    let events: [SessionEvent] = [
      .messageStarted(messageId: "m1"),
      .messageDelta(messageId: "m1", text: "hi", reasoning: nil)
    ]
    let memo = TranscriptProjection.Memo()
    let first = memo.project(events)
    let second = memo.project(events)
    #expect(first == second)
    #expect(second == TranscriptProjection.project(events))
  }

  /// The memo stays total: a feed that shrinks (never happens for the append-only
  /// session feed, but the memo must not corrupt) resets and re-folds, and can grow
  /// again correctly afterwards.
  @Test("the memo resets and re-folds when the feed shrinks, then grows correctly")
  func memoResetsOnShrink() {
    let full: [SessionEvent] = [
      .messageStarted(messageId: "m1"),
      .messageDelta(messageId: "m1", text: "hi", reasoning: nil),
      .messageCompleted(messageId: "m1")
    ]
    let memo = TranscriptProjection.Memo()
    _ = memo.project(full)
    let shorter = Array(full.prefix(1))
    #expect(memo.project(shorter) == TranscriptProjection.project(shorter))
    #expect(memo.project(full) == TranscriptProjection.project(full))
  }
}
