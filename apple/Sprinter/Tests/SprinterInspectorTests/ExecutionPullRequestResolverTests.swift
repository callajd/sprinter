import SprinterContract
import Testing

@testable import SprinterInspector

@Suite("Execution → PR resolver")
struct ExecutionPullRequestResolverTests {
  /// Resolves the PR via `execution.jobId → job.pullRequest` (the job's own PR).
  @Test("resolves the PR via execution.jobId → job.pullRequest")
  func resolvesViaJobPullRequest() {
    let snapshot = InspectorFixtures.snapshotWithJobPullRequest(InspectorFixtures.jobPullRequest)
    let pane = ExecutionPullRequestResolver.resolve(
      executionId: InspectorFixtures.executionId, in: snapshot)

    #expect(pane.executionId == InspectorFixtures.executionId)
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
    let pane = ExecutionPullRequestResolver.resolve(
      executionId: InspectorFixtures.executionId, in: snapshot)

    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .open(InspectorFixtures.issuePullRequest))
  }

  /// A resolved job that has produced no PR is the first-class "no PR yet" state.
  @Test("a job with no PR is awaitingPullRequest (no PR yet)")
  func noPullRequestYet() {
    let snapshot = InspectorFixtures.snapshotWithJobPullRequest(nil)
    let pane = ExecutionPullRequestResolver.resolve(
      executionId: InspectorFixtures.executionId, in: snapshot)

    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .awaitingPullRequest)
    #expect(pane.pullRequest == nil)
  }

  /// An execution whose job is not in the snapshot resolves to `.unresolved`, never a
  /// crash.
  @Test("an execution with no job in the snapshot is unresolved")
  func unresolvedExecution() {
    let empty = Snapshot(
      repositories: [], workstreams: [], epics: [], issues: [], jobs: [], executions: [],
      agents: [],
      generation: StoreGenerationId(rawValue: "gen-test"))
    let pane = ExecutionPullRequestResolver.resolve(
      executionId: InspectorFixtures.executionId, in: empty)

    #expect(pane.issueId == nil)
    #expect(pane.state == .unresolved)
  }

  /// The execution↔job join falls back to the job that back-references the execution
  /// when the execution record itself is absent (`job.executionId == executionId`) —
  /// mirroring `BoardProjection.liveActivity`'s `job.executionId ?? execution.id`.
  @Test("falls back to job.executionId when the execution record is absent")
  func fallsBackToJobBackReference() {
    let snapshot = Snapshot(
      repositories: [],
      workstreams: [],
      epics: [],
      issues: [InspectorFixtures.issue],
      jobs: [InspectorFixtures.jobWithPullRequest(InspectorFixtures.jobPullRequest)],
      executions: [],  // no execution record — resolve via the job's back-reference
      agents: [],
      generation: StoreGenerationId(rawValue: "gen-test"))

    let pane = ExecutionPullRequestResolver.resolve(
      executionId: InspectorFixtures.executionId, in: snapshot)
    #expect(pane.issueId == InspectorFixtures.issueId)
    #expect(pane.state == .open(InspectorFixtures.jobPullRequest))
  }
}
