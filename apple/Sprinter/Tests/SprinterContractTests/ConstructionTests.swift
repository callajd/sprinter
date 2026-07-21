import Foundation
import Testing

@testable import SprinterContract

/// Construction / encode-direction symmetry (INV-CONTRACT, the send side).
///
/// Each test builds a value via the mirror's public initializers and asserts it
/// equals the value decoded from the real contract golden — proving the Swift
/// constructors produce contract-equivalent values (not just that decoding works).
@Suite("Construction symmetry")
struct ConstructionTests {
  @Test("builds a pull-request ref equal to the golden")
  func buildsPullRequestRef() throws {
    let built = PullRequestRef(
      number: 15, url: "https://github.com/callajd/sprinter/pull/15", merged: true)
    #expect(built == (try Golden.decode(PullRequestRef.self, from: "pull-request-ref")))
  }

  @Test("builds a workstream equal to the golden")
  func buildsWorkstream() throws {
    let built = Workstream(
      id: WorkstreamId(rawValue: "ws-1"),
      name: "Foundation",
      repo: "callajd/sprinter",
      status: .active,
      epics: [EpicId(rawValue: "ep-1")])
    #expect(built == (try Golden.decode(Workstream.self, from: "workstream")))
  }

  @Test("builds an epic equal to the golden")
  func buildsEpic() throws {
    let built = Epic(
      id: EpicId(rawValue: "ep-1"),
      workstreamId: WorkstreamId(rawValue: "ws-1"),
      name: "FE2",
      status: .active,
      issues: [IssueId(rawValue: "iss-1"), IssueId(rawValue: "iss-2")])
    #expect(built == (try Golden.decode(Epic.self, from: "epic")))
  }

  @Test("builds an issue (with PR) equal to the golden")
  func buildsIssueWithPr() throws {
    let built = Issue(
      id: IssueId(rawValue: "iss-1"),
      epicId: EpicId(rawValue: "ep-1"),
      number: 10,
      title: "RPC contract mirror",
      status: .inReview,
      dependsOn: [IssueId(rawValue: "iss-0")],
      pullRequest: PullRequestRef(
        number: 16, url: "https://github.com/callajd/sprinter/pull/16", merged: false))
    #expect(built == (try Golden.decode(Issue.self, from: "issue-with-pr")))
  }

  @Test("builds an issue (no PR) equal to the golden")
  func buildsIssueNoPr() throws {
    let built = Issue(
      id: IssueId(rawValue: "iss-2"),
      epicId: EpicId(rawValue: "ep-1"),
      number: 11,
      title: "Swift contract bridge",
      status: .inProgress,
      dependsOn: [],
      pullRequest: nil)
    #expect(built == (try Golden.decode(Issue.self, from: "issue-no-pr")))
  }

  @Test("builds a job (all optionals) equal to the golden")
  func buildsJobFull() throws {
    let built = Job(
      id: JobId(rawValue: "job-1"),
      issueId: IssueId(rawValue: "iss-1"),
      kind: .implement,
      status: .running,
      sessionId: SessionId(rawValue: "ses-1"),
      transcriptRef: "transcripts/ses-1.jsonl",
      pullRequest: PullRequestRef(
        number: 15, url: "https://github.com/callajd/sprinter/pull/15", merged: true))
    #expect(built == (try Golden.decode(Job.self, from: "job-full")))
  }

  @Test("builds a minimal job equal to the golden")
  func buildsJobMinimal() throws {
    let built = Job(
      id: JobId(rawValue: "job-2"),
      issueId: IssueId(rawValue: "iss-2"),
      kind: .review,
      status: .queued,
      sessionId: nil,
      transcriptRef: nil,
      pullRequest: nil)
    #expect(built == (try Golden.decode(Job.self, from: "job-minimal")))
  }

  @Test("builds a session equal to the golden")
  func buildsSession() throws {
    let built = Session(
      id: SessionId(rawValue: "ses-1"), jobId: JobId(rawValue: "job-1"), status: .active)
    #expect(built == (try Golden.decode(Session.self, from: "session")))
  }

  @Test("builds a snapshot equal to the golden")
  func buildsSnapshot() throws {
    let decoded = try Golden.decode(Snapshot.self, from: "snapshot")
    let built = Snapshot(
      workstreams: decoded.workstreams,
      epics: decoded.epics,
      issues: decoded.issues,
      jobs: decoded.jobs,
      sessions: decoded.sessions,
      agents: decoded.agents)
    #expect(built == decoded)
    // The registry is a REQUIRED parameter like every other collection: the wire
    // always carries the key, so there is no default to let a construction site
    // quietly omit it. What the golden carries is a NON-empty registry.
    #expect(!decoded.agents.isEmpty)
  }

  @Test("builds Usage equal to the golden")
  func buildsUsage() throws {
    let built = Usage(
      inputTokens: 1200, outputTokens: 340, cacheReadTokens: 800, cacheWriteTokens: 64)
    let golden = try Golden.decode([Usage].self, from: "usages")
    #expect(built == golden[0])
  }

  @Test("builds a session input equal to the golden")
  func buildsSessionInput() throws {
    let built = SessionInput(text: "kick it off", images: ["img-ref-1"], mode: .prompt)
    let golden = try Golden.decode([SessionInput].self, from: "session-inputs")
    #expect(built == golden[0])
  }

  @Test("builds a UI response equal to the golden")
  func buildsUiResponse() throws {
    let built = UiResponse(requestId: "req-2", answer: .confirmed(confirmed: true))
    let golden = try Golden.decode([UiResponse].self, from: "ui-responses")
    #expect(built == golden[1])
  }

  @Test("builds a workstream plan equal to the golden")
  func buildsWorkstreamPlan() throws {
    let built = WorkstreamPlan(
      name: "Foundation",
      repo: "callajd/sprinter",
      spec: "Build the daemon↔client contract and its mirrors.")
    #expect(built == (try Golden.decode(WorkstreamPlan.self, from: "workstream-plan")))
  }

  @Test("builds every command payload equal to its golden")
  func buildsCommandPayloads() throws {
    let create = CreateWorkstreamFromPlanPayload(
      plan: WorkstreamPlan(name: "Foundation", repo: "callajd/sprinter", spec: "build it"))
    #expect(
      create
        == (try Golden.decode(
          CreateWorkstreamFromPlanPayload.self, from: "payload-create-workstream-from-plan")))

    let control = ControlPayload(workstreamId: WorkstreamId(rawValue: "ws-1"), action: .pause)
    #expect(control == (try Golden.decode(ControlPayload.self, from: "payload-control")))

    let retry = RetryIssuePayload(issueId: IssueId(rawValue: "iss-1"))
    #expect(retry == (try Golden.decode(RetryIssuePayload.self, from: "payload-retry-issue")))

    // The `sessionEvents` resume cursor — both wire forms (present + absent origin replay).
    let subscribe = SessionEventsPayload(sessionId: SessionId(rawValue: "ses-1"), sinceOffset: 12)
    #expect(
      subscribe == (try Golden.decode(SessionEventsPayload.self, from: "payload-session-events")))
    let subscribeFromOrigin = SessionEventsPayload(sessionId: SessionId(rawValue: "ses-1"))
    #expect(
      subscribeFromOrigin
        == (try Golden.decode(
          SessionEventsPayload.self, from: "payload-session-events-no-offset")))

    let interrupt = InterruptPayload(sessionId: SessionId(rawValue: "ses-1"))
    #expect(interrupt == (try Golden.decode(InterruptPayload.self, from: "payload-interrupt")))

    // The `events` resume cursor — both wire forms (present + absent origin replay).
    let events = EventsPayload(sinceOffset: 12)
    #expect(events == (try Golden.decode(EventsPayload.self, from: "payload-events")))
    let eventsFromOrigin = EventsPayload()
    #expect(
      eventsFromOrigin
        == (try Golden.decode(EventsPayload.self, from: "payload-events-no-offset")))
  }

  @Test("builds the send/answer payloads equal to their goldens")
  func buildsSendPayloads() throws {
    let send = SessionSendPayload(
      sessionId: SessionId(rawValue: "ses-1"),
      input: SessionInput(text: "go", images: nil, mode: .prompt))
    #expect(send == (try Golden.decode(SessionSendPayload.self, from: "payload-session-send")))

    let answer = AnswerUiRequestPayload(
      sessionId: SessionId(rawValue: "ses-1"),
      response: UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true)))
    #expect(
      answer
        == (try Golden.decode(AnswerUiRequestPayload.self, from: "payload-answer-ui-request")))
  }
}
