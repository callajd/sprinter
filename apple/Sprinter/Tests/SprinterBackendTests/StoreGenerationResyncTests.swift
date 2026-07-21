import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

/// The STORE-GENERATION boundary on the client (the round-2 review's B2).
///
/// The daemon's durable store NEVER migrates: bumping its schema version drops the
/// database and recreates it, restarting the event log's offsets at `1` and destroying
/// every entity a connected client retained. The app and the daemon run together
/// locally, so that lands on a LIVE client holding a stale cursor and a stale baseline
/// — and the delta model cannot repair it, because deltas are upsert-only (there is no
/// `*Removed`), so nothing streamed can remove an entity the reset destroyed.
///
/// The daemon therefore makes the generation EXPLICIT — an IDENTITY the client sends with
/// its cursor and the daemon compares, not a staleness inferred from offsets (which stops
/// working the moment the new log outgrows the stale cursor). A mismatch fails the request
/// with ``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)``. This suite
/// pins the client's half of that contract.
@Suite("Store-generation resync (ResyncRequired)")
struct StoreGenerationResyncTests {
  /// A client that reconnected with a stale `sinceOffset` would either be handed nothing
  /// (a silent stall) or an origin replay its stale contiguous cursor discards wholesale —
  /// and in EITHER case would keep serving the destroyed entities forever.
  ///
  /// This proves the client does the only correct thing when the resume is refused: it
  /// discards BOTH the cursor and the retained baseline and re-hydrates through
  /// `snapshot()`, publishing the daemon's POST-reset state rather than folding onto
  /// pre-reset state.
  @Test("a ResyncRequired resume drives the client back through snapshot(), losing stale state")
  func resyncRequiredForcesSnapshotRehydrate() async throws {
    let harness = ReconnectHarness()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let engine = WorkGraphResync(
      connect: harness.connect,
      backoff: .noDelay,
      observer: { observedContinuation.yield($0) })
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // ── Attempt 1: a normal first connect that leaves a retained baseline + cursor 1 ──
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let byTag1 = try await requestsByTag(&out1)
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag1["snapshot"]?.id), value: try Wire.encoded(Fixtures.snapshot))
    )
    #expect(try #require(await observedStates.next()) == Fixtures.snapshot)
    first.emit(
      Wire.chunk(
        requestId: try #require(byTag1["events"]?.id),
        values: [
          try Wire.encoded(
            Fixtures.offsetEvent(WorkGraphEvent.issueChanged(Fixtures.issueInReview), at: 1))
        ]))
    #expect(try #require(await observedStates.next()).issues == [Fixtures.issueInReview])
    first.close()

    // ── Attempt 2: the daemon has been reset. The resume is refused. ──
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let refused = try await nextSent(&out2)
    #expect(refused.rpcTag == "events")
    // The resume carries the cursor AND the generation the retained baseline was hydrated
    // in — the pair the daemon needs to decide whether the cursor is live at all.
    #expect(refused.payload == (try Fixtures.resumePayload(1)))
    // The daemon has been reset: its generation is now a DIFFERENT one, and it says so —
    // with the cursor WITHIN the new log's extent, the case no offset rule can see.
    second.emit(
      Wire.exitFail(
        requestId: try #require(refused.id),
        error: try Wire.encoded(Fixtures.resyncRefusal(sinceOffset: 1))))
    second.close()

    // ── Attempt 3: NOT a resume. The client subscribes around a FRESH snapshot. ──
    let third = try #require(await transports.next())
    var out3 = third.outbound.makeAsyncIterator()
    let byTag3 = try await requestsByTag(&out3)
    // The cursor is gone: `events` goes out with NO `sinceOffset` (origin), exactly as on
    // a first connect — and a `snapshot` request accompanies it, which a resume never issues.
    #expect(byTag3["events"]?.payload == (try toJSONValue(EventsPayload())))
    let rehydrate = try #require(byTag3["snapshot"])

    // The post-reset daemon serves a DIFFERENT baseline. Publishing it proves the retained
    // state was discarded rather than folded onto: `snapshotAfterReconnect` carries the
    // workstream as `.done`, which no upsert-only delta could have produced from attempt 1.
    third.emit(
      Wire.exitSuccess(
        requestId: try #require(rehydrate.id),
        value: try Wire.encoded(Fixtures.snapshotAfterReconnect)))
    #expect(try #require(await observedStates.next()) == Fixtures.snapshotAfterReconnect)

    await engine.stop()
    third.close()
    for await _ in states {}
  }
}
