import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Reconnect / snapshot-then-stream resync")
struct WorkGraphResyncTests {
  /// The core D4 property: on (re)connect the engine fetches the `snapshot`
  /// baseline FIRST, publishes it, then folds live deltas onto it — and a dropped
  /// connection re-runs snapshot-then-subscribe (a fresh baseline), never a
  /// delta-only stream. Driven in lockstep so every published state is observed
  /// (no missed / duplicated state).
  @Test("reconnect re-fetches the snapshot then resumes live events")
  func reconnectResync() async throws {
    let harness = ReconnectHarness()
    let engine = WorkGraphResync(connect: harness.connect, retryDelay: .zero)
    var transports = harness.transports.makeAsyncIterator()
    var states = await engine.states().makeAsyncIterator()

    // ── Attempt 1: snapshot baseline, then one live delta ──
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let snapshotRequest = try await nextSent(&out1)
    #expect(snapshotRequest.rpcTag == "snapshot")
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(snapshotRequest.id), value: try Wire.encoded(Fixtures.snapshot)))
    #expect(await states.next() == Fixtures.snapshot)

    let eventsRequest = try await nextSent(&out1)
    #expect(eventsRequest.rpcTag == "events")
    first.emit(
      Wire.chunk(
        requestId: try #require(eventsRequest.id),
        values: [try Wire.encoded(WorkGraphEvent.issueChanged(Fixtures.issueInReview))]))
    // The delta upserts the issue onto the baseline — applied, not missed.
    let reconciled = try #require(await states.next())
    #expect(reconciled.issues == [Fixtures.issueInReview])

    // ── Drop the connection ──
    first.close()

    // ── Attempt 2: a fresh baseline is re-fetched (snapshot-then-subscribe) ──
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let reconnectSnapshot = try await nextSent(&out2)
    #expect(reconnectSnapshot.rpcTag == "snapshot")
    second.emit(
      Wire.exitSuccess(
        requestId: try #require(reconnectSnapshot.id),
        value: try Wire.encoded(Fixtures.snapshotAfterReconnect)))
    #expect(await states.next() == Fixtures.snapshotAfterReconnect)

    await engine.stop()
    second.close()
  }

  /// The carried #36 F1 recovery: when the reconciler stalls behind BE1.1's
  /// ack-on-receipt `events` stream, the bounded backlog overflows and forces a
  /// snapshot-resync (a fresh connect + snapshot) rather than dropping deltas or
  /// growing unbounded. The observer stalls the reconciler on the FIRST delta so
  /// the backlog overflows a `bufferLimit` of 1; the resync is proven by the
  /// second connection re-fetching a snapshot.
  @Test("a bounded-buffer overflow forces a snapshot-resync")
  func overflowTriggersResync() async throws {
    let harness = ReconnectHarness()
    let counter = CallCounter()
    let observer: @Sendable (Snapshot) async -> Void = { _ in
      // Stall only the first delta (call 2): the baseline (call 1) passes, and
      // the resync's cancellation lifts the stall so the engine reconnects.
      if await counter.increment() == 2 {
        try? await Task.sleep(for: .seconds(60))
      }
    }
    let engine = WorkGraphResync(
      connect: harness.connect, bufferLimit: 1, retryDelay: .zero, observer: observer)
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()

    // Attempt 1: answer the snapshot, then flood deltas past the bound while the
    // reconciler is stalled.
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(try await nextSent(&out1).id),
        value: try Wire.encoded(Fixtures.snapshot)))
    let eventsId = try #require(try await nextSent(&out1).id)
    let delta = try Wire.encoded(Fixtures.issueEvent)
    first.emit(Wire.chunk(requestId: eventsId, values: [delta, delta, delta]))

    // The overflow tears the attempt down and the engine reconnects, re-fetching
    // a fresh snapshot — the resync recovery.
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    #expect(try await nextSent(&out2).rpcTag == "snapshot")

    await engine.stop()
    first.close()
    second.close()
    // Drain the feed so its backing task completes.
    for await _ in states {}
  }

  /// A second `states()` call yields an already-finished stream (single consumer).
  @Test("states() is single-consumer")
  func singleConsumer() async throws {
    let engine = WorkGraphResync(connect: { RpcBackend(transport: FakeTransport()) })
    _ = await engine.states()
    var second = await engine.states().makeAsyncIterator()
    #expect(await second.next() == nil)
    await engine.stop()
  }
}
