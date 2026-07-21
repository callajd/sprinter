import SprinterContract
import Testing

@testable import SprinterInspector

@Suite("Session → PR resolver")
struct SessionPullRequestResolverTests {
  /// Resolves the PR via `session.jobId → job.pullRequest` (the job's own PR).
  @Test("resolves the PR via session.jobId → job.pullRequest")
  func resolvesViaJobPullRequest() {
    let snapshot = InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest)
    let pane = SessionPullRequestResolver.resolve(
      sessionId: InspectorFixtures.sessionId, in: snapshot)

    #expect(pane.sessionId == InspectorFixtures.sessionId)
    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .open(InspectorFixtures.jobPullRequest))
    #expect(pane.pullRequest == InspectorFixtures.jobPullRequest)
  }

  /// Resolves the PR transitively via `job.issueId → issue.pullRequest` when the job
  /// itself carries none.
  @Test("resolves the PR via job.issueId → issue.pullRequest")
  func resolvesViaIssuePullRequest() {
    let snapshot = InspectorFixtures.snapshotWithIssuePullRequest(
      InspectorFixtures.issuePullRequest)
    let pane = SessionPullRequestResolver.resolve(
      sessionId: InspectorFixtures.sessionId, in: snapshot)

    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .open(InspectorFixtures.issuePullRequest))
  }

  /// A resolved job that has produced no PR is the first-class "no PR yet" state.
  @Test("a job with no PR is awaitingPullRequest (no PR yet)")
  func noPullRequestYet() {
    let snapshot = InspectorFixtures.snapshotWithJobPullRequest(nil)
    let pane = SessionPullRequestResolver.resolve(
      sessionId: InspectorFixtures.sessionId, in: snapshot)

    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .awaitingPullRequest)
    #expect(pane.pullRequest == nil)
  }

  /// A session whose job is not in the snapshot resolves to `.unresolved`, never a
  /// crash.
  @Test("a session with no job in the snapshot is unresolved")
  func unresolvedSession() {
    let empty = Snapshot(
      workstreams: [], epics: [], issues: [], jobs: [], sessions: [], agents: [])
    let pane = SessionPullRequestResolver.resolve(
      sessionId: InspectorFixtures.sessionId, in: empty)

    #expect(pane.issueId == nil)
    #expect(pane.state == .unresolved)
  }

  /// The session↔job join falls back to the job that back-references the session
  /// when the session record itself is absent (`job.sessionId == sessionId`) —
  /// mirroring `BoardProjection.liveActivity`'s `job.sessionId ?? session.id`.
  @Test("falls back to job.sessionId when the session record is absent")
  func fallsBackToJobBackReference() {
    let snapshot = Snapshot(
      workstreams: [],
      epics: [],
      issues: [InspectorFixtures.issue],
      jobs: [InspectorFixtures.jobWithPullRequest(InspectorFixtures.jobPullRequest)],
      sessions: [],  // no session record — resolve via the job's back-reference
      agents: [])

    let pane = SessionPullRequestResolver.resolve(
      sessionId: InspectorFixtures.sessionId, in: snapshot)
    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .open(InspectorFixtures.jobPullRequest))
  }
}
