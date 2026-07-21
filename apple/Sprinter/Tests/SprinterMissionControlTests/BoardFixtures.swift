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

  static let workstreamA = Workstream(
    id: WorkstreamId(rawValue: "ws-a"),
    name: "SwiftUI app",
    repo: "callajd/sprinter",
    status: .active,
    epics: [EpicId(rawValue: "ep-a")])

  /// A running job on `iss-a`, with an active session — the per-agent activity the
  /// board surfaces.
  static let runningJob = Job(
    id: JobId(rawValue: "job-a"),
    issueId: IssueId(rawValue: "iss-a"),
    kind: .implement,
    status: .running,
    sessionId: SessionId(rawValue: "sess-a"),
    transcriptRef: nil,
    pullRequest: nil)

  static let activeSession = Session(
    id: SessionId(rawValue: "sess-a"),
    jobId: JobId(rawValue: "job-a"),
    status: .active)

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
    repo: "callajd/sprinter-daemon",
    status: .done,
    epics: [EpicId(rawValue: "ep-b")])

  /// The baseline: two repo-scoped workstreams, one with a live agent.
  static let snapshot = Snapshot(
    workstreams: [workstreamA, workstreamB],
    epics: [epicA, epicB],
    issues: [issueA, issueB],
    jobs: [runningJob],
    sessions: [activeSession],
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

  /// An `implement` job on `issue`, optionally carrying a session.
  static func job(_ id: String, issue: String, status: JobStatus, session: String?) -> Job {
    Job(
      id: JobId(rawValue: id),
      issueId: IssueId(rawValue: issue),
      kind: .implement,
      status: status,
      sessionId: session.map { SessionId(rawValue: $0) },
      transcriptRef: nil,
      pullRequest: nil)
  }

  /// A one-workstream / one-epic snapshot whose epic lists `issueIds`, wired to the
  /// given jobs and sessions — the minimal shape for the activity-derivation cases.
  static func singleEpicSnapshot(
    issueIds: [String],
    jobs: [Job],
    sessions: [Session]
  ) -> Snapshot {
    Snapshot(
      workstreams: [
        Workstream(
          id: WorkstreamId(rawValue: "ws"),
          name: "W",
          repo: "callajd/sprinter",
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
      sessions: sessions,
      agents: [],
      generation: StoreGenerationId(rawValue: "gen-test"))
  }
}
