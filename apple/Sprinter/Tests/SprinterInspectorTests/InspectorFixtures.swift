import SprinterContract

/// Deterministic read-model fixtures for the inspector tests — one session whose
/// job resolves to a PR two different ways, plus the "no PR yet" variant.
enum InspectorFixtures {
  static let sessionId = SessionId(rawValue: "session-1")
  static let jobId = JobId(rawValue: "job-1")
  static let issueId = IssueId(rawValue: "issue-1")

  static let session = Session(id: sessionId, jobId: jobId, status: .active)

  /// The job's own closing PR (the `session.jobId → job.pullRequest` path).
  static let jobPullRequest = PullRequestRef(
    number: 42, url: "https://example.com/pull/42", merged: false)

  /// The issue's closing PR (the `job.issueId → issue.pullRequest` path).
  static let issuePullRequest = PullRequestRef(
    number: 7, url: "https://example.com/pull/7", merged: false)

  static let issue = Issue(
    id: issueId,
    epicId: EpicId(rawValue: "epic-1"),
    number: 7,
    title: "Inspector",
    status: .inReview,
    dependsOn: [],
    pullRequest: nil)

  /// A job carrying its own PR — resolves via `job.pullRequest`.
  static func jobWithPullRequest(_ pullRequest: PullRequestRef?) -> Job {
    Job(
      id: jobId,
      issueId: issueId,
      kind: .implement,
      status: .running,
      sessionId: sessionId,
      transcriptRef: nil,
      pullRequest: pullRequest)
  }

  /// A snapshot where the job itself carries the PR.
  static func snapshotWithJobPullRequest(_ pullRequest: PullRequestRef?) -> Snapshot {
    Snapshot(
      workstreams: [],
      epics: [],
      issues: [issue],
      jobs: [jobWithPullRequest(pullRequest)],
      sessions: [session],
      agents: [])
  }

  /// A snapshot where the job has NO PR but its issue does — the transitive
  /// `job.issueId → issue.pullRequest` path.
  static func snapshotWithIssuePullRequest(_ pullRequest: PullRequestRef?) -> Snapshot {
    Snapshot(
      workstreams: [],
      epics: [],
      issues: [
        Issue(
          id: issueId,
          epicId: EpicId(rawValue: "epic-1"),
          number: 7,
          title: "Inspector",
          status: .inReview,
          dependsOn: [],
          pullRequest: pullRequest)
      ],
      jobs: [jobWithPullRequest(nil)],
      sessions: [session],
      agents: [])
  }
}
