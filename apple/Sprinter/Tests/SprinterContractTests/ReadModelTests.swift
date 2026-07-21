import Foundation
import Testing

@testable import SprinterContract

@Suite("Read model")
struct ReadModelTests {
  @Test("decodes the full snapshot with all node kinds")
  func decodesSnapshot() throws {
    let snapshot = try Golden.decode(Snapshot.self, from: "snapshot")
    // The STATE layer rides the snapshot: without it a client could resolve none of
    // the repositories its workstreams reference.
    #expect(snapshot.repositories.count == 1)
    #expect(snapshot.workstreams.count == 1)
    #expect(snapshot.epics.count == 1)
    #expect(snapshot.issues.count == 2)
    #expect(snapshot.jobs.count == 2)
    // TWO executions for ONE job: a job is advanced by a TREE of executions (DE2.2), so
    // a client must fold several under one job rather than assuming a single row.
    #expect(snapshot.executions.count == 2)
    #expect(snapshot.executions.filter { $0.jobId == JobId(rawValue: "job-1") }.count == 2)
    #expect(snapshot.executions.filter(\.isLive).count == 1)
    // The REGISTRY layer rides the snapshot whole — every revision, retired included
    // (an Agent names no repository; the per-repo view is a fold, INV-DERIVED), in
    // the lexicographic-by-id order the daemon's `listAgents` pins.
    #expect(snapshot.agents.count == 3)
    #expect(snapshot.agents.map(\.id.rawValue) == ["agt-1", "agt-2", "agt-3"])
    // And the STORE GENERATION — the coordinate space this state's durable offsets live
    // in. A client retains it with the state and hands it back on every resume, so a
    // snapshot that dropped it would leave the client unable to resume at all.
    #expect(
      snapshot.generation
        == StoreGenerationId(rawValue: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b"))
    #expect(try Golden.roundTrip(snapshot) == snapshot)
  }

  @Test("maps workstream fields and branded ids")
  func decodesWorkstream() throws {
    let workstream = try Golden.decode(Workstream.self, from: "workstream")
    #expect(workstream.id == WorkstreamId(rawValue: "ws-1"))
    #expect(workstream.repositoryId == RepositoryId(rawValue: "repo:github:1296269"))
    #expect(workstream.status == .active)
    #expect(workstream.epics == [EpicId(rawValue: "ep-1")])
    #expect(try Golden.roundTrip(workstream) == workstream)
  }

  @Test("maps repository fields, its observed refs, and its observedAt")
  func decodesRepository() throws {
    let repository = try Golden.decode(Repository.self, from: "repository")
    #expect(repository.id == RepositoryId(rawValue: "repo:github:1296269"))
    #expect(repository.host == .github)
    #expect(repository.owner == "callajd")
    #expect(repository.name == "sprinter")
    // The refs arrive ORDERED BY BRANCH NAME, as a list — not a keyed object, so a
    // malformed name fails loudly on the daemon rather than being silently dropped.
    #expect(repository.refs.map(\.name.rawValue) == ["feat/x-1", "main"])
    #expect(
      repository.tip(of: BranchName(rawValue: "main"))
        == CommitSha(rawValue: "0123456789abcdef0123456789abcdef01234567"))
    // A branch that was not observed is `nil` — "not observed", never "does not exist".
    #expect(repository.tip(of: BranchName(rawValue: "nope")) == nil)
    // It is REFERENCED, not owned, so it carries the observation instant (INV-OBSERVED).
    #expect(repository.observedAt == "2026-07-20T12:00:00.000Z")
    #expect(try Golden.roundTrip(repository) == repository)
  }

  @Test("decodes a repository whose ref set is EMPTY — nothing observed yet")
  func decodesRepositoryWithNoRefs() throws {
    // An empty ref set is a VALID observation, not a malformed record, so the mirror
    // must decode it as an empty list rather than treating the key as missing.
    let repository = try Golden.decode(Repository.self, from: "repository-no-refs")
    #expect(repository.refs.isEmpty)
    #expect(repository.tip(of: BranchName(rawValue: "main")) == nil)
    #expect(try Golden.roundTrip(repository) == repository)
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
    #expect(job.executionId == ExecutionId(rawValue: "exe-1"))
    #expect(job.transcriptRef == "transcripts/exe-1.jsonl")
    #expect(job.pullRequest?.merged == true)
    #expect(try Golden.roundTrip(job) == job)
  }

  @Test("decodes a minimal job (all optional keys absent)")
  func decodesJobMinimal() throws {
    let job = try Golden.decode(Job.self, from: "job-minimal")
    #expect(job.kind == .review)
    #expect(job.status == .queued)
    #expect(job.executionId == nil)
    #expect(job.transcriptRef == nil)
    #expect(job.pullRequest == nil)
    #expect(try Golden.roundTrip(job) == job)
  }

  @Test("decodes an execution")
  func decodesExecution() throws {
    let execution = try Golden.decode(Execution.self, from: "execution")
    #expect(execution.id == ExecutionId(rawValue: "exe-1"))
    #expect(execution.jobId == JobId(rawValue: "job-1"))
    // It names the exact registry revision that ran it, holds the turn itself, and is a
    // ROOT — `parent` is OMITTED on the wire, which decodes to `nil` (never a null).
    #expect(execution.agentId == AgentId(rawValue: "agt-1"))
    #expect(execution.mode == .autonomous)
    #expect(execution.parent == nil)
    // Liveness is the TRANSCRIPT, and nothing else: this run is still going.
    #expect(execution.transcript == .live(LiveTranscript()))
    #expect(execution.isLive == true)
    #expect(try Golden.roundTrip(execution) == execution)
  }

  @Test("decodes a CHILD execution — the tree edge, the other mode, a sealed transcript")
  func decodesChildExecution() throws {
    let execution = try Golden.decode(Execution.self, from: "execution-child")
    #expect(execution.parent == ExecutionId(rawValue: "exe-1"))
    #expect(execution.mode == .interactive)
    // A settled run: the closed, cacheable range `[0, 12]`. A tail subscription cannot
    // be formed against it — `SealedTranscript` is not a `LiveTranscript` (D4).
    #expect(execution.transcript == .sealed(SealedTranscript(lastOffset: 12)))
    #expect(execution.isLive == false)
    #expect(try Golden.roundTrip(execution) == execution)
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
    #expect(events.count == 7)
    // The STATE layer rides the same delta union as the work graph.
    #expect(events[0] == .repositoryChanged(try Golden.decode(Repository.self, from: "repository")))
    #expect(events[1] == .workstreamChanged(try Golden.decode(Workstream.self, from: "workstream")))
    #expect(events[6] == .agentChanged(try Golden.decode(Agent.self, from: "agent-revised")))
    for event in events {
      #expect(try Golden.roundTrip(event) == event)
    }
  }

  @Test("decodes the offset-stamped events-stream envelope (CE2.0)")
  func decodesOffsetEvents() throws {
    let items = try Golden.decode([OffsetEvent].self, from: "offset-events")
    #expect(items.count == 4)
    // Each item pairs a delta with its durable offset; unwrapping `.event` gives the
    // bare delta the existing consumers use (RpcBackend / WorkGraphResync).
    let workstream = try Golden.decode(Workstream.self, from: "workstream")
    #expect(items[0].event == .workstreamChanged(workstream))
    // Real `event_log` offsets are 1-based (> 0); the sample resumes from a mid-log
    // position — the durable-replay coordinate the client feeds back as `sinceOffset`.
    #expect(items.map(\.offset) == [3, 4, 5, 6])
    for item in items {
      #expect(try Golden.roundTrip(item) == item)
    }
  }
}
