import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Snapshot reconciliation")
struct SnapshotReconcilerTests {
  private let reconciler = SnapshotReconciler()

  @Test("an issue delta replaces the matching issue in place")
  func replacesIssue() {
    let result = reconciler.reconcile(
      Fixtures.snapshot, applying: .issueChanged(Fixtures.issueInReview))
    #expect(result.issues == [Fixtures.issueInReview])
    // Sibling collections are untouched.
    #expect(result.workstreams == Fixtures.snapshot.workstreams)
    #expect(result.epics == Fixtures.snapshot.epics)
  }

  @Test("a workstream delta replaces the matching workstream")
  func replacesWorkstream() {
    let updated = Workstream(
      id: WorkstreamId(rawValue: "ws-1"), name: "Foundation", repo: "callajd/sprinter",
      status: .done, epics: [EpicId(rawValue: "ep-1")])
    let result = reconciler.reconcile(Fixtures.snapshot, applying: .workstreamChanged(updated))
    #expect(result.workstreams == [updated])
  }

  @Test("an epic delta replaces the matching epic")
  func replacesEpic() {
    let updated = Epic(
      id: EpicId(rawValue: "ep-1"), workstreamId: WorkstreamId(rawValue: "ws-1"), name: "BE1",
      status: .done, issues: [IssueId(rawValue: "iss-1")])
    let result = reconciler.reconcile(Fixtures.snapshot, applying: .epicChanged(updated))
    #expect(result.epics == [updated])
  }

  @Test("a new job delta appends when absent")
  func appendsNewJob() {
    let job = Job(
      id: JobId(rawValue: "job-1"), issueId: IssueId(rawValue: "iss-1"), kind: .implement,
      status: .running, sessionId: Fixtures.sessionId, transcriptRef: nil, pullRequest: nil)
    let result = reconciler.reconcile(Fixtures.snapshot, applying: .jobChanged(job))
    #expect(result.jobs == [job])
  }

  @Test("a new session delta appends when absent")
  func appendsNewSession() {
    let session = Session(
      id: Fixtures.sessionId, jobId: JobId(rawValue: "job-1"), status: .active)
    let result = reconciler.reconcile(Fixtures.snapshot, applying: .sessionChanged(session))
    #expect(result.sessions == [session])
  }

  @Test("folding a sequence of deltas keeps the state consistent")
  func foldsSequence() {
    let session = Session(id: Fixtures.sessionId, jobId: JobId(rawValue: "job-1"), status: .active)
    let idle = Session(id: Fixtures.sessionId, jobId: JobId(rawValue: "job-1"), status: .idle)
    var state = Fixtures.snapshot
    for event in [
      WorkGraphEvent.sessionChanged(session),
      WorkGraphEvent.issueChanged(Fixtures.issueInReview),
      WorkGraphEvent.sessionChanged(idle)
    ] {
      state = reconciler.reconcile(state, applying: event)
    }
    // The later session delta replaced the earlier (no duplicate), the issue delta
    // applied, and the append happened exactly once.
    #expect(state.sessions == [idle])
    #expect(state.issues == [Fixtures.issueInReview])
  }
}

@Suite("Bounded delta queue")
struct BoundedDeltaQueueTests {
  @Test("hands a delta straight to a waiting consumer")
  func directHandoff() async throws {
    let queue = BoundedDeltaQueue<Int>(limit: 1)
    let consumer = Task { await queue.next() }
    // Give the consumer a moment to register as the waiter, then hand off.
    try await Task.sleep(for: .milliseconds(20))
    try await queue.enqueue(7)
    #expect(await consumer.value == 7)
  }

  @Test("overflows once the backlog exceeds the limit (never drops, never grows)")
  func overflows() async throws {
    let queue = BoundedDeltaQueue<Int>(limit: 2)
    // No consumer is waiting, so these buffer up to the limit.
    try await queue.enqueue(1)
    try await queue.enqueue(2)
    await #expect(throws: BoundedDeltaQueue<Int>.Overflow.self) {
      try await queue.enqueue(3)
    }
    // The buffered deltas are still there to drain — nothing was dropped.
    #expect(await queue.next() == 1)
    #expect(await queue.next() == 2)
  }

  @Test("finish() ends a waiting consumer with nil")
  func finishEndsConsumer() async throws {
    let queue = BoundedDeltaQueue<Int>(limit: 1)
    let consumer = Task { await queue.next() }
    try await Task.sleep(for: .milliseconds(20))
    await queue.finish()
    #expect(await consumer.value == nil)
  }
}
