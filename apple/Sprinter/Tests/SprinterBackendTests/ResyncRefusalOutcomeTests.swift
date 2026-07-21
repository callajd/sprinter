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
  /// EVERY attempt, not on most of them.
  ///
  /// The count is calibrated against a MEASURED loss rate, not an assumed one: with the
  /// `runAttempt` drain reverted, this harness swallowed the refusal in **80 of 750 attempts
  /// across 5 runs — 10.7%** (per-run: 18, 21, 14, 13, 14 out of 150) on the machine this
  /// branch was verified on. (An earlier revision of this comment cited "~5%", which no
  /// measurement supports — the observed density is about twice that.)
  ///
  /// The count is deliberately NOT fitted to that measurement. A briefly-trimmed 96 was the
  /// tight fit — enough at 10.7%, and still enough at half of it — but the density is a
  /// property of THIS machine's scheduler, and the gate that matters (`make check`, with
  /// coverage instrumentation) schedules differently. The whole loop runs in well under a
  /// tenth of a second, so the headroom costs nothing: take the margin, not the fit.
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
      // Drain the feed to termination under a BOUND. A stopped engine must finish its stream;
      // if it does not, that is a teardown regression and it has to surface as this named
      // failure in seconds, not as a minute-long stall ended by the suite's `.timeLimit`.
      let drained = await runBounded { for await _ in states {} }
      #expect(
        drained == .success,
        Comment(rawValue: "iteration \(iteration): the feed never finished after stop()."))
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
    // BOUNDED: the whole point is that this wait ends on cancellation, so awaiting it
    // unbounded would turn the regression it guards into a hang instead of a failure.
    guard let value = await boundedValue(of: waiting) else {
      Issue.record("the cancelled queue wait never returned")
      return
    }
    #expect(value == nil)
  }

  /// Spins (yielding, under a hard cap) until a consumer has installed a waiter newer than
  /// `generation`, and returns its generation. Capped so a consumer that never suspends
  /// fails the test rather than spinning until the suite's `.timeLimit`.
  private func waiterInstalled<Element: Sendable>(
    on queue: BoundedDeltaQueue<Element>, after generation: Int
  ) async throws -> Int {
    for _ in 0..<10_000 {
      let current = await queue.waiterGeneration
      if current != generation { return current }
      await Task.yield()
    }
    throw LoopbackError.setupFailed("no waiter was installed on the queue")
  }

  @Test("a late release from a retired waiter cannot resume a later consumer's wait")
  func staleReleaseDoesNotResumeALaterWaiter() async throws {
    // The cancellation handler schedules its release as an UNSTRUCTURED task, so it can run
    // arbitrarily late — after `enqueue` already resumed the cancelled `next()` normally, and
    // after a SUBSEQUENT `next()` installed a fresh waiter. Resuming that later waiter with
    // `nil` is read by `fold`'s `while let offsetEvent = await queue.next()` as "the feed
    // finished", silently truncating the stream. Only the waiter a given `next()` installed
    // may be released by that `next()`'s cancellation, which the generation identity pins.
    //
    // Driven through the release seam rather than by racing the scheduler: the interleaving
    // is real but rare, and a test that has to win a race is not a regression test.
    let queue = BoundedDeltaQueue<Int>(limit: 8)

    let cancelled = Task { await queue.next() }
    let staleGeneration = try await waiterInstalled(on: queue, after: 0)
    // Its wait ends NORMALLY, before its cancellation release has run.
    try await queue.enqueue(1)
    #expect(await boundedValue(of: cancelled) == 1)
    cancelled.cancel()

    // A later consumer installs a fresh waiter...
    let later = Task { await queue.next() }
    _ = try await waiterInstalled(on: queue, after: staleGeneration)
    // ...and only now does the retired waiter's release land.
    await queue.releaseWaiter(generation: staleGeneration)
    try await queue.enqueue(2)

    let delivered = await boundedValue(of: later)
    #expect(
      delivered == 2,
      """
      the stale release resumed a LATER consumer's waiter (got \
      \(String(describing: delivered))) — fold would read that nil as end-of-feed.
      """)
  }
}
