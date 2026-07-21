import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

/// Regression cover for the SECOND hang found while clearing #94: an attempt whose outcome
/// was decided by which half of its task group finished FIRST.
///
/// The reader half hands the folder half its terminator (`queue.finish()`) before it
/// rethrows, so a failing reader makes the folder runnable and then races it. When the
/// folder won, the attempt returned as a SUCCESS and the reader's error was discarded with
/// the group — and a discarded ``ContractError/resyncRequired`` leaves the dead resume point
/// in place, so the next attempt resumes from a cursor the daemon has already refused rather
/// than re-hydrating through `snapshot()`.
///
/// Assertions here read exactly ONE outbound frame per attempt, so a regression FAILS on a
/// mismatched payload instead of blocking on a request the engine never sends.
@Suite("Resync refusal outcome", .timeLimit(.minutes(1)))
struct ResyncRefusalOutcomeTests {
  /// The race is a scheduler race, so this drives it repeatedly: the invariant must hold on
  /// EVERY attempt, not on most of them. The count is set so a regression is caught with
  /// overwhelming probability (the pre-fix loss rate was ~5% per attempt, i.e. a survival
  /// probability under 1e-3 across this many iterations), while the whole loop still runs in
  /// well under a second against the in-memory harness.
  private static let attempts = 150

  /// Attempt 1 — a first connect that leaves the engine holding a retained baseline and a
  /// cursor at offset 1 (the state a resume is minted from).
  private func establishBaseline(
    _ transport: FakeTransport, _ observedStates: inout AsyncStream<Snapshot>.Iterator
  ) async throws {
    var outbound = transport.outbound.makeAsyncIterator()
    let byTag = try await requestsByTag(&outbound)
    transport.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag["snapshot"]?.id),
        value: try Wire.encoded(Fixtures.snapshot)))
    #expect(try #require(await observedStates.next()) == Fixtures.snapshot)
    transport.emit(
      Wire.chunk(
        requestId: try #require(byTag["events"]?.id),
        values: [
          try Wire.encoded(
            Fixtures.offsetEvent(WorkGraphEvent.issueChanged(Fixtures.issueInReview), at: 1))
        ]))
    #expect(try #require(await observedStates.next()).issues == [Fixtures.issueInReview])
    transport.close()
  }

  /// Attempt 2 — the resume is REFUSED. The reader half fails; the folder half finishes off
  /// the terminator that same failure produced. Whichever lands first, the refusal is the
  /// attempt's outcome.
  private func refuseResume(_ transport: FakeTransport) async throws {
    var outbound = transport.outbound.makeAsyncIterator()
    let refused = try await nextSent(&outbound)
    #expect(refused.rpcTag == "events")
    #expect(refused.payload == (try Fixtures.resumePayload(1)))
    transport.emit(
      Wire.exitFail(
        requestId: try #require(refused.id),
        error: try Wire.encoded(Fixtures.resyncRefusal(sinceOffset: 1))))
    transport.close()
  }

  @Test("a refused resume always discards the cursor, whichever half of the attempt ends first")
  func refusedResumeAlwaysDiscardsTheCursor() async throws {
    for iteration in 0..<Self.attempts {
      let harness = ReconnectHarness()
      let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
      let engine = WorkGraphResync(
        connect: harness.connect,
        backoff: .noDelay,
        observer: { observedContinuation.yield($0) })
      var transports = harness.transports.makeAsyncIterator()
      let states = await engine.states()
      var observedStates = observed.makeAsyncIterator()

      try await establishBaseline(try #require(await transports.next()), &observedStates)
      try await refuseResume(try #require(await transports.next()))

      // Attempt 3 — the observable that separates "refusal handled" from "refusal
      // swallowed". Reading just the FIRST frame decides it under either arrival order, so
      // this can never block on a request the engine does not send: a first connect issues
      // `snapshot` (which a resume never does) and an ORIGIN `events` (no `resume`), while a
      // swallowed refusal issues exactly one frame — `events` carrying the dead cursor.
      let third = try #require(await transports.next())
      var out3 = third.outbound.makeAsyncIterator()
      let resubscribe = try await nextSent(&out3)
      let swallowed = """
        iteration \(iteration): the refused resume was swallowed — attempt 3 re-sent the \
        dead cursor instead of re-hydrating through snapshot().
        """
      switch resubscribe.rpcTag {
      case "snapshot":
        break  // a re-hydrate: only a first connect issues this.
      case "events":
        #expect(
          resubscribe.payload == (try toJSONValue(EventsPayload())),
          Comment(rawValue: swallowed))
      default:
        Issue.record("iteration \(iteration): unexpected request \(resubscribe.rpcTag ?? "nil")")
      }

      await engine.stop()
      third.close()
      for await _ in states {}
    }
  }

  @Test("a cancelled consumer's queue wait ends rather than blocking on an absent producer")
  func cancelledQueueWaitReturns() async throws {
    // The folder half of an attempt parks in `BoundedDeltaQueue.next()`. Draining an
    // attempt's group requires that wait to end on cancellation — otherwise teardown is
    // gated on a producer that may already be gone.
    let queue = BoundedDeltaQueue<Int>(limit: 8)
    let waiting = Task { await queue.next() }
    // Let the consumer install its waiter before cancelling, so this exercises the
    // cancellation of an ALREADY-suspended wait, not an early-exit check.
    try await Task.sleep(for: .milliseconds(20))
    waiting.cancel()
    #expect(await waiting.value == nil)
  }
}
