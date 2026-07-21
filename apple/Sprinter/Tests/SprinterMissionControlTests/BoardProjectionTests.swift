import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Board projection — hierarchy, status, activity")
struct BoardProjectionTests {
  /// A RUNNING execution for `job`: its transcript is still open, which is the whole of
  /// its liveness (DE2.2 — there is no execution-status enum any more).
  private func liveExecution(_ id: String, job: String) -> Execution {
    Execution(
      id: ExecutionId(rawValue: id), jobId: JobId(rawValue: job),
      agentId: AgentId(rawValue: "agt-1"), parent: nil, mode: .autonomous,
      transcript: .live(LiveTranscript()))
  }

  /// A SETTLED execution for `job`: its transcript is sealed at the extent it reached.
  private func settledExecution(_ id: String, job: String) -> Execution {
    Execution(
      id: ExecutionId(rawValue: id), jobId: JobId(rawValue: job),
      agentId: AgentId(rawValue: "agt-1"), parent: nil, mode: .autonomous,
      transcript: .sealed(SealedTranscript(lastOffset: 3)))
  }

  /// The snapshot projects into the cross-repo `Workstream ⊃ Epic ⊃ Issue` tree,
  /// preserving declared order and keeping each workstream on its own repo (D14).
  @Test("projects the cross-repo hierarchy in declared order")
  func projectsCrossRepoHierarchy() {
    let board = BoardProjection.project(BoardFixtures.snapshot)

    #expect(board.map(\.id) == [WorkstreamId(rawValue: "ws-a"), WorkstreamId(rawValue: "ws-b")])
    #expect(board.map(\.repo) == ["callajd/sprinter", "callajd/sprinter-daemon"])

    #expect(board.first?.epics.map(\.id) == [EpicId(rawValue: "ep-a")])
    #expect(board.first?.epics.first?.issues.map(\.id) == [IssueId(rawValue: "iss-a")])
  }

  /// Each node's lifecycle surfaces as its projected board status: workstream A /
  /// epic A / issue A are ongoing; the fully-done workstream B is complete.
  @Test("derives ongoing / complete board status per node")
  func derivesStatusPerNode() {
    let board = BoardProjection.project(BoardFixtures.snapshot)

    #expect(board.first?.status == .ongoing)
    #expect(board.first?.epics.first?.status == .ongoing)
    #expect(board.first?.epics.first?.issues.first?.status == .ongoing)

    #expect(board.last?.status == .complete)
    #expect(board.last?.epics.first?.status == .complete)
    #expect(board.last?.epics.first?.issues.first?.status == .complete)
  }

  /// Per-agent activity surfaces for the issue with a running job (naming the job,
  /// its kind, and its execution); the done issue with no agent surfaces none.
  @Test("surfaces per-agent activity for a running job")
  func surfacesLiveAgentActivity() {
    let board = BoardProjection.project(BoardFixtures.snapshot)

    let live = board.first?.epics.first?.issues.first
    #expect(live?.hasLiveAgent == true)
    #expect(
      live?.activity
        == IssueActivity(
          jobId: JobId(rawValue: "job-a"),
          kind: .implement,
          executionId: ExecutionId(rawValue: "exe-a")))

    let idle = board.last?.epics.first?.issues.first
    #expect(idle?.hasLiveAgent == false)
    #expect(idle?.activity == nil)
  }

  /// Activity is derived from EITHER signal: a `running` job with no execution is
  /// live, and a non-running job whose execution is `active` is live; a queued job
  /// with a non-active execution is not.
  @Test("live-agent activity comes from a running job or an active execution")
  func activityFromEitherSignal() {
    let snapshot = BoardFixtures.singleEpicSnapshot(
      issueIds: ["iss-run", "iss-q", "iss-idle"],
      jobs: [
        BoardFixtures.job("job-run", issue: "iss-run", status: .running, execution: nil),
        BoardFixtures.job("job-q", issue: "iss-q", status: .queued, execution: "exe-q"),
        BoardFixtures.job("job-idle", issue: "iss-idle", status: .queued, execution: "exe-idle")
      ],
      executions: [
        liveExecution("exe-q", job: "job-q"),
        // A SETTLED execution: its transcript is sealed, so it is not a live agent.
        settledExecution("exe-idle", job: "job-idle")
      ])

    let issues = BoardProjection.project(snapshot).first?.epics.first?.issues ?? []
    let byId = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    #expect(byId[IssueId(rawValue: "iss-run")]?.hasLiveAgent == true)
    #expect(byId[IssueId(rawValue: "iss-q")]?.hasLiveAgent == true)
    #expect(byId[IssueId(rawValue: "iss-idle")]?.hasLiveAgent == false)
  }

  /// A TERMINAL job is not a live agent even with a lingering `active` execution; and
  /// a job made live by its execution names that execution when the job's own
  /// `executionId` is nil.
  @Test("a terminal job is not live; an active execution's id is the fallback")
  func terminalExclusionAndExecutionFallback() {
    let snapshot = BoardFixtures.singleEpicSnapshot(
      issueIds: ["iss-done", "iss-fb"],
      jobs: [
        BoardFixtures.job("job-done", issue: "iss-done", status: .succeeded, execution: nil),
        BoardFixtures.job("job-fb", issue: "iss-fb", status: .queued, execution: nil)
      ],
      executions: [
        liveExecution("exe-done", job: "job-done"),
        liveExecution("exe-fb", job: "job-fb")
      ])

    let issues = BoardProjection.project(snapshot).first?.epics.first?.issues ?? []
    let byId = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    // N2: a succeeded job with a stale active execution is NOT a live agent.
    #expect(byId[IssueId(rawValue: "iss-done")]?.hasLiveAgent == false)
    // N1: a job live via its active execution (declared executionId nil) names it.
    let fallback = byId[IssueId(rawValue: "iss-fb")]
    #expect(fallback?.hasLiveAgent == true)
    #expect(fallback?.activity?.executionId == ExecutionId(rawValue: "exe-fb"))
  }

  /// PINS THE DELIBERATE WIDENING (DE2.2) and its dependency on the daemon's seal.
  ///
  /// ``BoardProjection/liveActivity`` now fires for a QUEUED job holding an OPEN
  /// transcript. That is not a hypothetical state: `startup-reconcile` produces it on
  /// purpose — a paused workstream's `running` job is settled to `queued` — and the ONLY
  /// thing that clears it is the same settle SEALING the job's executions. So the board's
  /// answer here is a direct readout of whether that seal landed, which is why it is
  /// pinned by a test rather than left to the docstring.
  ///
  /// Three jobs, one per state the seal can leave behind:
  ///
  /// - `iss-open` — queued, transcript OPEN. The pre-seal state: LIVE (the widening).
  /// - `iss-sealed` — queued, transcript SEALED. What a correct settle produces: NOT live.
  /// - `iss-partial` — queued, ROOT sealed but a CHILD still open. What a settle that
  ///   sealed only `getExecutionForJob`'s root produced: still LIVE, so a root-only seal
  ///   does not clear the board and the stall stays visible. This is the Swift-side
  ///   witness for the daemon's "seal EVERY execution" test.
  @Test("a queued job with an open transcript is live until EVERY execution is sealed")
  func queuedJobLivenessTracksTheSeal() {
    let child = Execution(
      id: ExecutionId(rawValue: "exe-partial-child"), jobId: JobId(rawValue: "job-partial"),
      agentId: AgentId(rawValue: "agt-1"), parent: ExecutionId(rawValue: "exe-partial"),
      mode: .autonomous, transcript: .live(LiveTranscript()))
    let snapshot = BoardFixtures.singleEpicSnapshot(
      issueIds: ["iss-open", "iss-sealed", "iss-partial"],
      jobs: [
        BoardFixtures.job("job-open", issue: "iss-open", status: .queued, execution: "exe-open"),
        BoardFixtures.job(
          "job-sealed", issue: "iss-sealed", status: .queued, execution: "exe-sealed"),
        BoardFixtures.job(
          "job-partial", issue: "iss-partial", status: .queued, execution: "exe-partial")
      ],
      executions: [
        liveExecution("exe-open", job: "job-open"),
        settledExecution("exe-sealed", job: "job-sealed"),
        settledExecution("exe-partial", job: "job-partial"),
        child
      ])

    let issues = BoardProjection.project(snapshot).first?.epics.first?.issues ?? []
    let byId = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    #expect(byId[IssueId(rawValue: "iss-open")]?.hasLiveAgent == true)
    #expect(byId[IssueId(rawValue: "iss-sealed")]?.hasLiveAgent == false)
    #expect(byId[IssueId(rawValue: "iss-partial")]?.hasLiveAgent == true)
    // `iss-partial` is live BECAUSE of the child: `indexedPreferringLive` keeps the OPEN
    // execution over the sealed root, so a single unsealed node anywhere in the tree keeps
    // the issue live. The id the activity NAMES is still the job's declared one — the
    // fallback to the indexed execution only applies when `job.executionId` is nil.
    #expect(
      byId[IssueId(rawValue: "iss-partial")]?.activity?.executionId
        == ExecutionId(rawValue: "exe-partial"))
  }

  /// A child id listed by its parent but absent from the snapshot is skipped, not
  /// force-resolved (INV-NOFORCE) — the projection stays total on a dangling ref.
  @Test("skips a dangling child reference")
  func skipsDanglingReference() {
    let snapshot = Snapshot(
      repositories: [BoardFixtures.repositoryA],
      workstreams: [
        Workstream(
          id: WorkstreamId(rawValue: "ws"),
          name: "W",
          repositoryId: RepositoryId(rawValue: "repo:github:1296269"),
          status: .active,
          epics: [EpicId(rawValue: "ep"), EpicId(rawValue: "ep-missing")])
      ],
      epics: [
        Epic(
          id: EpicId(rawValue: "ep"),
          workstreamId: WorkstreamId(rawValue: "ws"),
          name: "E",
          status: .active,
          issues: [IssueId(rawValue: "iss-missing")])
      ],
      issues: [],
      jobs: [],
      executions: [],
      agents: [],
      generation: StoreGenerationId(rawValue: "gen-test"))

    let board = BoardProjection.project(snapshot)
    #expect(board.first?.epics.map(\.id) == [EpicId(rawValue: "ep")])
    #expect(board.first?.epics.first?.issues.isEmpty == true)
  }
}

@Suite("Board status projection")
struct BoardStatusTests {
  @Test("maps every WorkStatus onto the board vocabulary")
  func mapsWorkStatus() {
    #expect(BoardStatus(WorkStatus.pending) == .notYetStarted)
    #expect(BoardStatus(WorkStatus.active) == .ongoing)
    #expect(BoardStatus(WorkStatus.blocked) == .paused)
    #expect(BoardStatus(WorkStatus.done) == .complete)
    // CE5.1: cancelled is terminal but distinct from complete on the board.
    #expect(BoardStatus(WorkStatus.cancelled) == .cancelled)
  }

  @Test("maps every IssueStatus onto the board vocabulary")
  func mapsIssueStatus() {
    #expect(BoardStatus(IssueStatus.pending) == .notYetStarted)
    #expect(BoardStatus(IssueStatus.ready) == .notYetStarted)
    #expect(BoardStatus(IssueStatus.inProgress) == .ongoing)
    #expect(BoardStatus(IssueStatus.inReview) == .ongoing)
    #expect(BoardStatus(IssueStatus.blocked) == .paused)
    #expect(BoardStatus(IssueStatus.done) == .complete)
  }
}
