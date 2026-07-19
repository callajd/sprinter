import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Reconnect / snapshot-then-stream resync")
struct WorkGraphResyncTests {
  /// The core D4 property AND the CE2.2 incremental-resume behavior: the FIRST connect
  /// subscribes AROUND the snapshot (events + snapshot), publishes the baseline, then
  /// folds live deltas — tracking each delta's durable offset. A dropped connection then
  /// resumes INCREMENTALLY: it re-subscribes `events` with `sinceOffset` set to the
  /// last-applied offset and folds new deltas onto the RETAINED baseline, issuing NO
  /// fresh `snapshot` (not a re-derive). The retained state (the attempt-1 delta) is
  /// still present after the reconnect, proving the resume did not re-fetch a baseline.
  @Test("reconnect resumes incrementally from the last-applied offset, no fresh snapshot")
  func reconnectResumesFromOffset() async throws {
    let harness = ReconnectHarness()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let engine = WorkGraphResync(
      connect: harness.connect,
      backoff: .noDelay,
      observer: { observedContinuation.yield($0) })
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // ── Attempt 1: subscribe-around-snapshot (events + snapshot race; index by tag) ──
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let byTag1 = try await requestsByTag(&out1)
    let snapshotRequest = try #require(byTag1["snapshot"])
    let eventsRequest = try #require(byTag1["events"])
    // The first attempt replays from ORIGIN — a present payload with no `sinceOffset`.
    #expect(eventsRequest.payload == (try toJSONValue(EventsPayload())))

    first.emit(
      Wire.exitSuccess(
        requestId: try #require(snapshotRequest.id), value: try Wire.encoded(Fixtures.snapshot)))
    #expect(try #require(await observedStates.next()) == Fixtures.snapshot)

    // One live delta at offset 1 upserts the issue to in_review and advances the cursor.
    first.emit(
      Wire.chunk(
        requestId: try #require(eventsRequest.id),
        values: [
          try Wire.encoded(
            Fixtures.offsetEvent(WorkGraphEvent.issueChanged(Fixtures.issueInReview), at: 1))
        ]))
    let afterDelta = try #require(await observedStates.next())
    #expect(afterDelta.issues == [Fixtures.issueInReview])

    // ── Drop the connection ──
    first.close()

    // ── Attempt 2: INCREMENTAL resume — events(sinceOffset: 1), and NO snapshot ──
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let resumeRequest = try await nextSent(&out2)
    #expect(resumeRequest.rpcTag == "events")
    #expect(resumeRequest.payload == (try toJSONValue(EventsPayload(sinceOffset: 1))))

    // A new delta folds onto the RETAINED baseline (issue still in_review) — proving no
    // fresh snapshot was fetched (a re-derive would have replaced the issue list).
    second.emit(
      Wire.chunk(
        requestId: try #require(resumeRequest.id),
        values: [try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 2))]))
    let afterResume = try #require(await observedStates.next())
    #expect(afterResume.issues == [Fixtures.issueInReview])
    #expect(afterResume.workstreams == [Fixtures.workstream])

    await engine.stop()
    second.close()
    for await _ in states {}
  }

  /// The CE2.0 carried correctness constraint: the live feed can publish OUT of durable-
  /// offset order, so the resume cursor must be the CONTIGUOUS PREFIX (highest N with all
  /// offsets ≤ N applied), NOT the max seen. Attempt 1 applies offsets 1, 2, 4 (a GAP at
  /// 3); the reconnect must resume from 2 (not 4), so the daemon re-sends 3 — the event
  /// that a max-seen cursor would have LOST. Re-sending 3 on reconnect recovers it.
  @Test("reconnect resumes from the contiguous prefix, not the max offset (no lost event)")
  func reconnectResumesFromContiguousPrefix() async throws {
    let harness = ReconnectHarness()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let engine = WorkGraphResync(
      connect: harness.connect,
      backoff: .noDelay,
      observer: { observedContinuation.yield($0) })
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // ── Attempt 1: baseline, then offsets 1, 2, 4 (offset 3 is the un-delivered gap) ──
    let first = try #require(await transports.next())
    var out1 = first.outbound.makeAsyncIterator()
    let byTag1 = try await requestsByTag(&out1)
    first.emit(
      Wire.exitSuccess(
        requestId: try #require(byTag1["snapshot"]?.id), value: try Wire.encoded(Fixtures.snapshot))
    )
    #expect(try #require(await observedStates.next()) == Fixtures.snapshot)

    let eventsId = try #require(byTag1["events"]?.id)
    first.emit(
      Wire.chunk(
        requestId: eventsId,
        values: [
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 1)),
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.issueEvent, at: 2)),
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 4))
        ]))
    // Drain the three applied states (the issue stays original — offset 3 never arrived).
    for _ in 0..<3 { _ = try #require(await observedStates.next()) }

    // ── Drop, then resume: cursor is the contiguous prefix 2, NOT the max 4 ──
    first.close()
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let resumeRequest = try await nextSent(&out2)
    #expect(resumeRequest.rpcTag == "events")
    #expect(resumeRequest.payload == (try toJSONValue(EventsPayload(sinceOffset: 2))))

    // The daemon re-sends from 3: offset 3 (the recovered gap) advances the issue to
    // in_review, offset 4 re-applies idempotently. No event is lost.
    second.emit(
      Wire.chunk(
        requestId: try #require(resumeRequest.id),
        values: [
          try Wire.encoded(
            Fixtures.offsetEvent(WorkGraphEvent.issueChanged(Fixtures.issueInReview), at: 3)),
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 4))
        ]))
    var recovered = try #require(await observedStates.next())
    while recovered.issues != [Fixtures.issueInReview] {
      recovered = try #require(await observedStates.next())
    }
    #expect(recovered.issues == [Fixtures.issueInReview])

    await engine.stop()
    second.close()
    for await _ in states {}
  }

  /// The carried #36 F1 recovery, now via incremental resume: when the reconciler stalls
  /// behind the demand-gated `events` stream, the bounded backlog overflows and forces a
  /// reconnect — which resumes from the last-applied contiguous offset (NOT a fresh
  /// snapshot re-derive). The overflowed deltas were never applied, so they are `>` the
  /// cursor and get re-sent — no loss. Proven by the second connection re-subscribing
  /// `events` with `sinceOffset` set and issuing NO snapshot.
  @Test("a bounded-buffer overflow forces an incremental-resume reconnect")
  func overflowForcesIncrementalResume() async throws {
    let harness = ReconnectHarness()
    let counter = CallCounter()
    let (observed, observedContinuation) = AsyncStream<Snapshot>.makeStream()
    let observer: @Sendable (Snapshot) async -> Void = { state in
      let call = await counter.increment()
      observedContinuation.yield(state)
      // Stall from the flood's first delta on (call 3), holding the reconciler so the
      // bounded backlog overflows. The baseline (1) and offset-1 (2) pass first, so the
      // cursor is durably recorded BEFORE the overflow — the resume is guaranteed present.
      if call >= 3 {
        try? await Task.sleep(for: .seconds(60))
      }
    }
    let engine = WorkGraphResync(
      connect: harness.connect, bufferLimit: 1, backoff: .noDelay, observer: observer)
    var transports = harness.transports.makeAsyncIterator()
    let states = await engine.states()
    var observedStates = observed.makeAsyncIterator()

    // Attempt 1: answer the snapshot and apply offset 1 — GATED on its observed state, so
    // the cursor is recorded before we flood.
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
    _ = try #require(await observedStates.next())  // offset 1 applied + recorded (call 2).

    // Now flood past the bound while the reconciler stalls (call 3 onward).
    first.emit(
      Wire.chunk(
        requestId: eventsId, values: [try offsetDelta(2), try offsetDelta(3), try offsetDelta(4)]))

    // The overflow tears the attempt down; the engine reconnects and resumes INCREMENTALLY
    // from the recorded contiguous cursor — a present `sinceOffset`, NOT a fresh snapshot.
    let second = try #require(await transports.next())
    var out2 = second.outbound.makeAsyncIterator()
    let resumeRequest = try await nextSent(&out2)
    #expect(resumeRequest.rpcTag == "events")
    let payload = try #require(resumeRequest.payload)
    #expect(try fromJSONValue(EventsPayload.self, payload).sinceOffset != nil)

    await engine.stop()
    first.close()
    second.close()
    for await _ in states {}
  }

  /// The CE2.1 carried teardown constraint: on reconnect the OLD ``Backend`` is fully
  /// `close()`d BEFORE the next dial, so no in-flight frame crosses connections. A
  /// `connect` seam logs `dial`/`close` in order: attempt 1 tears down at once (its
  /// `snapshot()` throws), attempt 2 stays alive (so the loop quiesces after exactly one
  /// reconnect). The close of attempt 1 must precede the dial of attempt 2.
  @Test("reconnect closes the old backend before dialing the next")
  func reconnectClosesBeforeRedial() async throws {
    let log = OrderLog()
    let attempts = CallCounter()
    let connect: WorkGraphResync.Connect = {
      let attempt = await attempts.increment()
      await log.record("dial")
      // Attempt 1 tears down immediately (snapshot throws) → one reconnect; attempt 2
      // stays alive (events open) so the loop quiesces rather than spinning.
      return ControllableBackend(
        tearsDown: attempt == 1, onClose: { await log.record("close") })
    }
    let engine = WorkGraphResync(connect: connect, backoff: .noDelay)
    let states = await engine.states()

    // dial(1), close(1), dial(2): exactly one reconnect, then attempt 2 stays alive.
    await log.waitFor(entries: 3)
    await engine.stop()

    let entries = await log.entries
    // Every close precedes the NEXT dial: scanning the log, a dial is never preceded by an
    // un-closed dial.
    var open = false
    for entry in entries {
      if entry == "dial" {
        #expect(!open, "dialed a new backend before closing the previous one")
        open = true
      } else if entry == "close" {
        open = false
      }
    }
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

  // MARK: - Helpers

  /// One offset-tagged issue delta for the flood tests.
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

/// A ``Backend`` for the teardown-ordering test. When `tearsDown` is `true` its
/// `snapshot()` throws so the attempt collapses at once (forcing a reconnect); otherwise
/// its `snapshot()` succeeds and `events()` stays open, so the attempt STAYS ALIVE and the
/// reconnect loop quiesces (never spins). `close()` reports through the injected hook.
final class ControllableBackend: Backend {
  private let tearsDown: Bool
  private let onClose: @Sendable () async -> Void

  init(tearsDown: Bool, onClose: @escaping @Sendable () async -> Void) {
    self.tearsDown = tearsDown
    self.onClose = onClose
  }

  func snapshot() async throws -> Snapshot {
    if tearsDown { throw BackendError.connectionClosed }
    return Fixtures.snapshot
  }
  func createWorkstreamFromPlan(_ plan: WorkstreamPlan) async throws -> WorkstreamId {
    throw BackendError.connectionClosed
  }
  func control(workstreamId: WorkstreamId, action: ControlAction) async throws {}
  func retryIssue(issueId: IssueId) async throws {}
  func events() -> AsyncThrowingStream<WorkGraphEvent, any Error> {
    if tearsDown {
      return AsyncThrowingStream { $0.finish() }
    }
    // Stays open until the consumer terminates (on engine stop), so the attempt is alive.
    return AsyncThrowingStream { continuation in
      let task = Task {
        try? await Task.sleep(for: .seconds(3600))
        continuation.finish()
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }
  func sessionEvents(sessionId: SessionId) -> AsyncThrowingStream<SessionEvent, any Error> {
    AsyncThrowingStream { $0.finish() }
  }
  func sessionSend(sessionId: SessionId, input: SessionInput) async throws {}
  func interrupt(sessionId: SessionId) async throws {}
  func answerUiRequest(sessionId: SessionId, response: UiResponse) async throws {}
  func close() async { await onClose() }
}

/// An ordered, awaitable event log for the teardown-ordering test.
private actor OrderLog {
  private(set) var entries: [String] = []
  private var waiters: [(threshold: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func record(_ entry: String) {
    entries.append(entry)
    let count = entries.count
    waiters.removeAll { waiter in
      guard count >= waiter.threshold else { return false }
      waiter.continuation.resume()
      return true
    }
  }

  func waitFor(entries threshold: Int) async {
    if entries.count >= threshold { return }
    await withCheckedContinuation { continuation in
      waiters.append((threshold, continuation))
    }
  }
}
