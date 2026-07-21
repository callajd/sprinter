import SprinterContract

/// Deterministic owned-DTO fixtures for the board tests: a **cross-repo** snapshot
/// (two workstreams, each repo-scoped, D14) with an active agent on one issue.
enum BoardFixtures {
  // ── Workstream A: callajd/sprinter, active, with a live agent ────────────────

  static let issueA = Issue(
    id: IssueId(rawValue: "iss-a"),
    epicId: EpicId(rawValue: "ep-a"),
    number: 38,
    title: "Board + activity",
    status: .inProgress,
    dependsOn: [],
    pullRequest: nil)

  static let epicA = Epic(
    id: EpicId(rawValue: "ep-a"),
    workstreamId: WorkstreamId(rawValue: "ws-a"),
    name: "Mission Control",
    status: .active,
    issues: [IssueId(rawValue: "iss-a")])

  /// The two repositories the workstreams REFERENCE. The board resolves each into
  /// its `owner/name` display string, so a snapshot missing one would render the raw id.
  static let repositoryA = Repository(
    id: RepositoryId(rawValue: "repo:github:1296269"),
    host: .github,
    owner: "callajd",
    name: "sprinter",
    refs: [
      RepositoryRef(
        name: BranchName(rawValue: "main"),
        sha: CommitSha(rawValue: "0123456789abcdef0123456789abcdef01234567"))
    ],
    observedAt: "2026-07-20T12:00:00.000Z")
  static let repositoryB = Repository(
    id: RepositoryId(rawValue: "repo:github:1296270"),
    host: .github,
    owner: "callajd",
    name: "sprinter-daemon",
    refs: [
      RepositoryRef(
        name: BranchName(rawValue: "main"),
        sha: CommitSha(rawValue: "0123456789abcdef0123456789abcdef01234567"))
    ],
    observedAt: "2026-07-20T12:00:00.000Z")

  static let workstreamA = Workstream(
    id: WorkstreamId(rawValue: "ws-a"),
    name: "SwiftUI app",
    repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
    status: .active,
    epics: [EpicId(rawValue: "ep-a")])

  /// A running job on `iss-a`, with an active execution — the per-agent activity the
  /// board surfaces.
  static let runningJob = Job(
    id: JobId(rawValue: "job-a"),
    issueId: IssueId(rawValue: "iss-a"),
    kind: .implement,
    status: .running,
    executionId: ExecutionId(rawValue: "exe-a"),
    transcriptRef: nil,
    pullRequest: nil)

  static let activeExecution = Execution(
    id: ExecutionId(rawValue: "exe-a"),
    jobId: JobId(rawValue: "job-a"),
    agentId: AgentId(rawValue: "agt-1"),
    parent: nil,
    mode: .autonomous,
    transcript: .live(LiveTranscript()))

  // ── Workstream B: a DIFFERENT repo, fully done, no live agent ────────────────

  static let issueB = Issue(
    id: IssueId(rawValue: "iss-b"),
    epicId: EpicId(rawValue: "ep-b"),
    number: 12,
    title: "Daemon core",
    status: .done,
    dependsOn: [],
    pullRequest: PullRequestRef(number: 12, url: "https://example.test/pr/12", merged: true))

  static let epicB = Epic(
    id: EpicId(rawValue: "ep-b"),
    workstreamId: WorkstreamId(rawValue: "ws-b"),
    name: "Foundation",
    status: .done,
    issues: [IssueId(rawValue: "iss-b")])

  static let workstreamB = Workstream(
    id: WorkstreamId(rawValue: "ws-b"),
    name: "Daemon",
    repositoryId: RepositoryId(rawValue: "repo:github:1296270"),
    status: .done,
    epics: [EpicId(rawValue: "ep-b")])

  /// The baseline: two repo-scoped workstreams, one with a live agent.
  static let snapshot = Snapshot(
    repositories: [repositoryA, repositoryB],
    workstreams: [workstreamA, workstreamB],
    epics: [epicA, epicB],
    issues: [issueA, issueB],
    jobs: [runningJob],
    executions: [activeExecution],
    agents: [],
    generation: StoreGenerationId(rawValue: "gen-test"))

  // ── Live-update deltas ───────────────────────────────────────────────────────

  /// Epic A advanced to `done` — flips its board status ongoing → complete.
  static let epicADone = Epic(
    id: EpicId(rawValue: "ep-a"),
    workstreamId: WorkstreamId(rawValue: "ws-a"),
    name: "Mission Control",
    status: .done,
    issues: [IssueId(rawValue: "iss-a")])

  // ── Parameterized builders (kept here so the Testing-importing test file avoids
  //    the `Issue` name clash between `SprinterContract` and `Testing`) ───────────

  /// A minimal `inProgress` issue under epic `ep`.
  static func issue(_ id: String) -> Issue {
    Issue(
      id: IssueId(rawValue: id),
      epicId: EpicId(rawValue: "ep"),
      number: 1,
      title: id,
      status: .inProgress,
      dependsOn: [],
      pullRequest: nil)
  }

  /// An `implement` job on `issue`, optionally carrying an execution.
  static func job(_ id: String, issue: String, status: JobStatus, execution: String?) -> Job {
    Job(
      id: JobId(rawValue: id),
      issueId: IssueId(rawValue: issue),
      kind: .implement,
      status: status,
      executionId: execution.map { ExecutionId(rawValue: $0) },
      transcriptRef: nil,
      pullRequest: nil)
  }

  /// A one-workstream / one-epic snapshot whose epic lists `issueIds`, wired to the
  /// given jobs and executions — the minimal shape for the activity-derivation cases.
  static func singleEpicSnapshot(
    issueIds: [String],
    jobs: [Job],
    executions: [Execution]
  ) -> Snapshot {
    Snapshot(
      repositories: [repositoryA],
      workstreams: [
        Workstream(
          id: WorkstreamId(rawValue: "ws"),
          name: "W",
          repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
          status: .active,
          epics: [EpicId(rawValue: "ep")])
      ],
      epics: [
        Epic(
          id: EpicId(rawValue: "ep"),
          workstreamId: WorkstreamId(rawValue: "ws"),
          name: "E",
          status: .active,
          issues: issueIds.map { IssueId(rawValue: $0) })
      ],
      issues: issueIds.map { issue($0) },
      jobs: jobs,
      executions: executions,
      agents: [],
      generation: StoreGenerationId(rawValue: "gen-test"))
  }
}
