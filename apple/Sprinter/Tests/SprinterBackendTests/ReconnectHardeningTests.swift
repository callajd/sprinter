import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

/// Cold-review hardening for the reconnect engine: the backoff must WIDEN against a
/// flapping daemon (FIX2), and memory must stay BOUNDED through the production
/// ``RpcBackend/events(sinceOffset:)`` path even though that adapter interposes an
/// unbounded stream over the bounded gate (FIX1).
@Suite("Reconnect hardening — flap backoff + bounded flow control")
struct ReconnectHardeningTests {
  /// FIX2 / FIX A — the backoff must widen against a flapping daemon. A daemon that ACCEPTS
  /// the socket then IMMEDIATELY drops the subscription (no snapshot, no delta) is not
  /// healthy: it made no successful read AND did not stay established for the min-healthy
  /// duration. If the backoff reset on bare `connect()` success it would clear `failures`
  /// every attempt, pinning the reconnect ceiling at `base` — a hot spin. Because the reset
  /// now fires only on a HEALTHY connection, an accept-then-drop flap is scored a failure and
  /// the ceiling WIDENS exponentially. Timing is fully injected on TWO axes: the jitter
  /// closure records each ceiling and returns `.zero` (the loop never waits on the reconnect
  /// clock), and a ``ManualClock`` that is NEVER advanced makes every attempt's established-
  /// duration exactly zero (< the health threshold) — so the flap can never be mistaken for
  /// an idle-but-healthy connection. Bounded, no hang.
  @Test("an accept-then-immediately-drop flap widens the reconnect backoff, not pinned at base")
  func acceptThenDropWidensBackoff() async throws {
    let recorder = CeilingRecorder()
    let backoff = ReconnectBackoff(
      base: .milliseconds(1),
      maximum: .seconds(100),
      multiplier: 2,
      jitter: { ceiling in
        recorder.record(ceiling)
        return .zero
      })
    // Every attempt accepts (connect succeeds) then immediately drops: `snapshot()` throws
    // and `events()` finishes at once, so the attempt collapses with NO read. The ManualClock
    // never advances, so its established-duration is 0 < the health threshold: a flap.
    let connect: WorkGraphResync.Connect = { ControllableBackend(tearsDown: true, onClose: {}) }
    let engine = WorkGraphResync(connect: connect, backoff: backoff, clock: ManualClock())
    let states = await engine.states()

    // Bounded: stop as soon as five ceilings have been recorded (the loop spins with no
    // clock wait, so this resolves promptly and cannot hang).
    let ceilings = await recorder.waitFor(5)
    await engine.stop()

    let sample = Array(ceilings.prefix(5))
    #expect(sample.count == 5)
    // 1 ms → 2 ms → 4 ms → 8 ms → 16 ms: each ceiling strictly wider than the last. A
    // reset-on-connect bug would pin every ceiling at 1 ms (base).
    for (previous, current) in zip(sample, sample.dropFirst()) {
      #expect(current > previous, "backoff ceiling did not widen: \(sample)")
    }
    for await _ in states {}
  }

  /// FIX A — the regression a prior FIX (reset only on an applied delta) introduced. On the
  /// RESUME path a healthy reconnect that receives NO new deltas (the client is already caught
  /// up) never reset the backoff, so a healthy-but-idle connection that later dropped during a
  /// quiet period was mis-scored a FAILURE — and repeated quiet-period blips widened the
  /// ceiling toward the 30 s cap even though every connection was fine. The fix resets on a
  /// CONNECTION-HEALTH signal that covers the idle case: a subscription that stayed established
  /// at least ``minHealthyDuration`` is healthy even with zero reads. This test drives both
  /// halves deterministically on a ``ManualClock`` (no wall-clock wait): an idle resume that
  /// stayed established past the threshold RESETS (its reconnect ceiling stays at `base`),
  /// while an accept-then-instant-drop flap (established-duration 0, no read) WIDENS.
  @Test(
    "a healthy idle resume resets the backoff; an instant flap widens it (no quiet-period drift)")
  func healthyIdleResumeResetsWhileFlapWidens() async throws {
    let recorder = CeilingRecorder()
    let base = Duration.milliseconds(4)
    let backoff = ReconnectBackoff(
      base: base, maximum: .seconds(100), multiplier: 2,
      jitter: { ceiling in
        recorder.record(ceiling)
        return .zero
      })
    let clock = ManualClock()
    let threshold = Duration.seconds(1)
    let harness = ReconnectHarness()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let engine = WorkGraphResync(
      connect: harness.connect, backoff: backoff, clock: clock, minHealthyDuration: threshold,
      observer: { observedContinuation.yield($0) })
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // Attempt 1: baseline + delta@1 records the resume cursor (1), then drops. The applied
    // delta is a successful read → attempt 1 is healthy (backoff reset).
    try await establishCursorThenDrop(&transports, &observedStates)

    // Attempt 2: INCREMENTAL resume, NO deltas. Advance the clock past the health threshold so
    // this idle connection is established long enough to be HEALTHY, then drop during a quiet
    // period. Even with no read, the backoff must RESET — the regression FIX A closes.
    let second = try await nextResumeAttempt(&transports, resumingFrom: 1)
    clock.advance(by: threshold)  // idle-but-established past the health window.
    second.close()  // quiet-period drop → healthy (reset), NOT a failure.

    // Attempt 3: INCREMENTAL resume, NO deltas, dropped IMMEDIATELY with the clock NOT advanced
    // (established-duration 0 < threshold, no read) — an accept-then-flap that MUST widen.
    let third = try await nextResumeAttempt(&transports, resumingFrom: 1)
    third.close()  // instant flap → widen.

    // Attempt 4: stays OPEN so the reconnect loop quiesces (no further drop/ceiling).
    let fourth = try await nextResumeAttempt(&transports, resumingFrom: 1)

    // Three drops → exactly three recorded reconnect ceilings (attempt 4 never drops).
    let sample = Array((await recorder.waitFor(3)).prefix(3))
    await engine.stop()
    fourth.close()

    // Attempt 1 (read → healthy) AND attempt 2 (idle but established past the threshold →
    // healthy) both reset the backoff, so their reconnect ceilings stay at `base`. Attempt 3
    // (instant flap → not healthy) does NOT reset, so its ceiling WIDENS past base.
    #expect(sample[0] == base)
    #expect(
      sample[1] == base, "a healthy idle resume must reset the backoff, not widen it: \(sample)")
    #expect(sample[2] > base, "an instant flap must still widen the backoff: \(sample)")

    for await _ in states {}
  }

  /// FIX B — the bounded-gap guard, end to end. A live-tail offset can be PERMANENTLY missing
  /// within a connection (the daemon commits the durable entry before publishing the live
  /// delta and does not guarantee a gapless live tail). Left unbounded the contiguous prefix
  /// would stall behind that hole while ``ContiguousOffsetTracker``'s ahead-set grew without
  /// limit and the resume cursor froze — the client silently stops advancing until an
  /// unrelated reconnect. The guard caps the ahead-set: once the un-filled window exceeds
  /// `gapLimit` the tracker throws ``ContiguousOffsetTracker/StalledGap``, which collapses the
  /// attempt into a resync — INCREMENTALLY from the FROZEN cursor, so the daemon re-sends the
  /// missing offset from the durable log (loss-free). Proven: with the gap at 2 permanently
  /// missing and `gapLimit` 3, a bounded run of higher offsets trips the guard and the engine
  /// reconnects with `sinceOffset` set to the frozen prefix (1), not a frozen client.
  @Test("a permanently-missing live-tail offset forces a resync from the frozen cursor")
  func stalledGapForcesResync() async throws {
    let harness = ReconnectHarness()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let engine = WorkGraphResync(
      connect: harness.connect, backoff: .noDelay, gapLimit: 3,
      observer: { observedContinuation.yield($0) })
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // Attempt 1: baseline, then offset 1 (prefix → 1, cursor recorded).
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let byTag1 = try await requestsByTag(&out1)
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag1["snapshot"]?.id), value: try Wire.encoded(Fixtures.snapshot))
    )
    _ = try #require(await observedStates.next())  // baseline.
    let eventsId = try #require(byTag1["events"]?.id)
    first.emit(Wire.chunk(requestId: eventsId, values: [try offsetDelta(1)]))
    _ = try #require(await observedStates.next())  // offset 1 → cursor 1 recorded.

    // Offset 2 is PERMANENTLY missing on the live tail; 3, 4, 5, 6 pile up ahead of the gap.
    // With `gapLimit` 3 the 4th held-ahead offset (6) trips ``StalledGap`` — the prefix cannot
    // advance, so rather than growing the ahead-set unbounded / freezing, the attempt resyncs.
    first.emit(
      Wire.chunk(
        requestId: eventsId,
        values: [try offsetDelta(3), try offsetDelta(4), try offsetDelta(5), try offsetDelta(6)]))

    // The stall collapses the attempt; the engine resyncs INCREMENTALLY from the FROZEN cursor
    // (1) — a present `sinceOffset`, so the daemon re-sends the missing 2 from the durable log.
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let resume = try await nextSent(&out2)
    #expect(resume.rpcTag == "events")
    #expect(resume.payload == (try Fixtures.resumePayload(1)))

    await engine.stop()
    first.close()
    second.close()
    for await _ in states {}
  }

  /// FIX1 — memory stays bounded through the production ``RpcBackend/events(sinceOffset:)``
  /// path even though it interposes an UNBOUNDED stream over the bounded gate. A fast daemon
  /// floods far past the bounded reconciler backlog while the reconciler is stalled; rather
  /// than buffering the whole flood the pipeline OVERFLOWS and resyncs — a 2nd `events`
  /// request with a present `sinceOffset`, so the overflowed deltas are re-sent (no silent
  /// drop). The per-request ``AckGate`` bound caps how far the daemon can run ahead and the
  /// ``BoundedDeltaQueue`` bound collapses the attempt into a resync; the unbounded
  /// interposed stream never grows without bound.
  @Test("a fast daemon + stalled reconciler stays bounded through RpcBackend.events (→ resync)")
  func floodStaysBoundedThroughRpcBackendEvents() async throws {
    let harness = ReconnectHarness()
    let counter = CallCounter()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let observer: @Sendable (Snapshot) async -> Void = { state in
      let call = await counter.increment()
      observedContinuation.yield(state)
      // Stall from the flood on (call 3), holding the reconciler so the bounded backlog
      // overflows. The baseline (1) and offset 1 (2) pass first, recording the cursor.
      if call >= 3 { try? await Task.sleep(for: .seconds(60)) }
    }
    let engine = WorkGraphResync(
      connect: harness.connect, bufferLimit: 4, backoff: .noDelay, observer: observer)
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // Attempt 1: baseline + offset 1, GATED on their observed states so the cursor is
    // recorded before we flood.
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let byTag1 = try await requestsByTag(&out1)
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag1["snapshot"]?.id), value: try Wire.encoded(Fixtures.snapshot))
    )
    _ = try #require(await observedStates.next())  // baseline (call 1).
    let eventsId = try #require(byTag1["events"]?.id)
    first.emit(Wire.chunk(requestId: eventsId, values: [try offsetDelta(1)]))
    _ = try #require(await observedStates.next())  // offset 1 recorded (call 2).

    // Flood FAR past the bounded backlog (bufferLimit 4) in one burst while stalled.
    var flood: [String] = []
    for offset in 2...300 { flood.append(try offsetDelta(offset)) }
    first.emit(Wire.chunk(requestId: eventsId, values: flood))

    // Bounded: the pipeline overflows and resumes INCREMENTALLY from the recorded cursor —
    // a present `sinceOffset`, not a fresh snapshot, so the overflowed deltas are re-sent.
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let resume = try await nextSent(&out2)
    #expect(resume.rpcTag == "events")
    let payload = try #require(resume.payload)
    #expect(try fromJSONValue(EventsPayload.self, payload).sinceOffset != nil)

    await engine.stop()
    first.close()
    second.close()
    for await _ in states {}
  }

  // MARK: - Helpers

  /// One offset-tagged issue delta for the flood test.
  private func offsetDelta(_ offset: Int) throws -> String {
    try Wire.encoded(Fixtures.offsetEvent(Fixtures.issueEvent, at: offset))
  }

  /// Attempt 1 for the idle-health test: answer the snapshot, apply delta@1 (recording resume
  /// cursor 1), drain both observed states, then drop — a healthy first connect that leaves a
  /// resume cursor for the following attempt.
  private func establishCursorThenDrop(
    _ transports: inout AsyncStream<FakeTransport>.Iterator,
    _ observedStates: inout AsyncStream<Snapshot>.Iterator
  ) async throws {
    let first = try #require(await transports.next())
    var out = first.outbound.makeAsyncIterator()
    let byTag = try await requestsByTag(&out)
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag["snapshot"]?.id), value: try Wire.encoded(Fixtures.snapshot)))
    _ = try #require(await observedStates.next())  // baseline.
    first.emit(
      Wire.chunk(requestId: try #require(byTag["events"]?.id), values: [try offsetDelta(1)]))
    _ = try #require(await observedStates.next())  // offset 1 → cursor recorded.
    first.close()  // drop attempt 1.
  }

  /// Reads the next reconnect attempt's transport and consumes its incremental-resume `events`
  /// request, asserting it resumes strictly after `offset` (a present `sinceOffset`, not a
  /// fresh snapshot), then returns the transport for the test to drive.
  private func nextResumeAttempt(
    _ transports: inout AsyncStream<FakeTransport>.Iterator,
    resumingFrom offset: Int
  ) async throws -> FakeTransport {
    let transport = try #require(await transports.next())
    var out = transport.outbound.makeAsyncIterator()
    let resume = try await nextSent(&out)
    #expect(resume.rpcTag == "events")
    #expect(resume.payload == (try Fixtures.resumePayload(offset)))
    return transport
  }
}

/// A synchronous, awaitable collector of backoff ceilings for the flap-widening test. It
/// is fed from the (non-async) jitter closure, so it guards its state with a lock rather
/// than actor isolation; a test awaits ``waitFor(_:)`` for the first `count` ceilings.
private final class CeilingRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var ceilings: [Duration] = []
  private var threshold: Int?
  private var waiter: CheckedContinuation<[Duration], Never>?

  /// Records one ceiling and, once the awaited threshold is met, resumes the waiter
  /// (off the lock, so the resume never runs under the lock).
  func record(_ ceiling: Duration) {
    lock.lock()
    ceilings.append(ceiling)
    let readyWaiter: CheckedContinuation<[Duration], Never>?
    let snapshot = ceilings
    if let threshold, ceilings.count >= threshold, let waiter {
      readyWaiter = waiter
      self.waiter = nil
      self.threshold = nil
    } else {
      readyWaiter = nil
    }
    lock.unlock()
    readyWaiter?.resume(returning: snapshot)
  }

  /// Suspends until at least `count` ceilings have been recorded, returning them in order.
  func waitFor(_ count: Int) async -> [Duration] {
    await withCheckedContinuation { (continuation: CheckedContinuation<[Duration], Never>) in
      lock.lock()
      if ceilings.count >= count {
        let snapshot = ceilings
        lock.unlock()
        continuation.resume(returning: snapshot)
        return
      }
      threshold = count
      waiter = continuation
      lock.unlock()
    }
  }
}
