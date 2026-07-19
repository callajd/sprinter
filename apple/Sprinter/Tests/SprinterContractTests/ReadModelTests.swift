import Foundation
import Testing

@testable import SprinterContract

@Suite("Read model")
struct ReadModelTests {
  @Test("decodes the full snapshot with all node kinds")
  func decodesSnapshot() throws {
    let snapshot = try Golden.decode(Snapshot.self, from: "snapshot")
    #expect(snapshot.workstreams.count == 1)
    #expect(snapshot.epics.count == 1)
    #expect(snapshot.issues.count == 2)
    #expect(snapshot.jobs.count == 2)
    #expect(snapshot.sessions.count == 1)
    #expect(try Golden.roundTrip(snapshot) == snapshot)
  }

  @Test("maps workstream fields and branded ids")
  func decodesWorkstream() throws {
    let workstream = try Golden.decode(Workstream.self, from: "workstream")
    #expect(workstream.id == WorkstreamId(rawValue: "ws-1"))
    #expect(workstream.repo == "callajd/sprinter")
    #expect(workstream.status == .active)
    #expect(workstream.epics == [EpicId(rawValue: "ep-1")])
    #expect(try Golden.roundTrip(workstream) == workstream)
  }

  @Test("maps epic fields")
  func decodesEpic() throws {
    let epic = try Golden.decode(Epic.self, from: "epic")
    #expect(epic.workstreamId == WorkstreamId(rawValue: "ws-1"))
    #expect(epic.issues.count == 2)
    #expect(try Golden.roundTrip(epic) == epic)
  }

  @Test("decodes the distinct terminal cancelled WorkStatus (CE5.1)")
  func decodesCancelledStatus() throws {
    let workstream = try Golden.decode(Workstream.self, from: "workstream-cancelled")
    let epic = try Golden.decode(Epic.self, from: "epic-cancelled")
    #expect(workstream.status == .cancelled)
    #expect(epic.status == .cancelled)
    #expect(try Golden.roundTrip(workstream) == workstream)
    #expect(try Golden.roundTrip(epic) == epic)
  }

  @Test("decodes an issue carrying its PR (optional-key present)")
  func decodesIssueWithPr() throws {
    let issue = try Golden.decode(Issue.self, from: "issue-with-pr")
    #expect(issue.number == 10)
    #expect(issue.status == .inReview)
    #expect(issue.pullRequest?.merged == false)
    #expect(issue.dependsOn == [IssueId(rawValue: "iss-0")])
    #expect(try Golden.roundTrip(issue) == issue)
  }

  @Test("decodes an issue without a PR (optional-key absent -> nil)")
  func decodesIssueNoPr() throws {
    let issue = try Golden.decode(Issue.self, from: "issue-no-pr")
    #expect(issue.status == .inProgress)
    #expect(issue.pullRequest == nil)
    #expect(issue.dependsOn.isEmpty)
    #expect(try Golden.roundTrip(issue) == issue)
  }

  @Test("decodes a job with every optional-key field present")
  func decodesJobFull() throws {
    let job = try Golden.decode(Job.self, from: "job-full")
    #expect(job.kind == .implement)
    #expect(job.status == .running)
    #expect(job.sessionId == SessionId(rawValue: "ses-1"))
    #expect(job.transcriptRef == "transcripts/ses-1.jsonl")
    #expect(job.pullRequest?.merged == true)
    #expect(try Golden.roundTrip(job) == job)
  }

  @Test("decodes a minimal job (all optional keys absent)")
  func decodesJobMinimal() throws {
    let job = try Golden.decode(Job.self, from: "job-minimal")
    #expect(job.kind == .review)
    #expect(job.status == .queued)
    #expect(job.sessionId == nil)
    #expect(job.transcriptRef == nil)
    #expect(job.pullRequest == nil)
    #expect(try Golden.roundTrip(job) == job)
  }

  @Test("decodes a session")
  func decodesSession() throws {
    let session = try Golden.decode(Session.self, from: "session")
    #expect(session.id == SessionId(rawValue: "ses-1"))
    #expect(session.jobId == JobId(rawValue: "job-1"))
    #expect(session.status == .active)
    #expect(try Golden.roundTrip(session) == session)
  }

  @Test("decodes a standalone pull-request ref")
  func decodesPullRequestRef() throws {
    let pullRequest = try Golden.decode(PullRequestRef.self, from: "pull-request-ref")
    #expect(pullRequest.number == 15)
    #expect(pullRequest.merged == true)
    #expect(try Golden.roundTrip(pullRequest) == pullRequest)
  }

  @Test("decodes every work-graph delta variant")
  func decodesWorkGraphEvents() throws {
    let events = try Golden.decode([WorkGraphEvent].self, from: "work-graph-events")
    #expect(events.count == 5)
    #expect(events[0] == .workstreamChanged(try Golden.decode(Workstream.self, from: "workstream")))
    for event in events {
      #expect(try Golden.roundTrip(event) == event)
    }
  }

  @Test("decodes the offset-stamped events-stream envelope (contract v3 / CE2.0)")
  func decodesOffsetEvents() throws {
    let items = try Golden.decode([OffsetEvent].self, from: "offset-events")
    #expect(items.count == 3)
    // Each item pairs a delta with its durable offset; unwrapping `.event` gives the
    // bare delta the existing consumers use (RpcBackend / WorkGraphResync).
    let workstream = try Golden.decode(Workstream.self, from: "workstream")
    #expect(items[0].event == .workstreamChanged(workstream))
    // Real `event_log` offsets are 1-based (> 0); the sample resumes from a mid-log
    // position — the durable-replay coordinate the client feeds back as `sinceOffset`.
    #expect(items.map(\.offset) == [3, 4, 5])
    for item in items {
      #expect(try Golden.roundTrip(item) == item)
    }
  }
}
