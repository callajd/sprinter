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
  /// FIX2 — the backoff must widen against a flapping daemon. A daemon that ACCEPTS the
  /// socket then IMMEDIATELY drops the subscription (no snapshot, no delta) makes zero
  /// progress. If the backoff reset on bare `connect()` success it would clear `failures`
  /// every attempt, pinning the reconnect ceiling at `base` — a hot spin. Because the reset
  /// now fires only on demonstrated progress, an accept-then-drop flap is treated as a
  /// failure and the ceiling WIDENS exponentially. Timing is fully injected: the jitter
  /// closure records each ceiling and returns `.zero`, so the loop never waits on the clock
  /// (bounded, no hang) yet reveals whether the schedule widens or is pinned.
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
    // and `events()` finishes at once, so the attempt collapses with NO progress recorded.
    let connect: WorkGraphResync.Connect = { ControllableBackend(tearsDown: true, onClose: {}) }
    let engine = WorkGraphResync(connect: connect, backoff: backoff)
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

  /// Reads the two requests one attempt issues (`events` + `snapshot`, order not
  /// wire-guaranteed) and indexes them by rpc tag.
  private func requestsByTag(
    _ outbound: inout AsyncStream<Data>.Iterator
  ) async throws -> [String: SentFrame] {
    var byTag: [String: SentFrame] = [:]
    for _ in 0..<2 {
      let frame = try await nextSent(&outbound)
      if let tag = frame.rpcTag {
        byTag[tag] = frame
      }
    }
    return byTag
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
