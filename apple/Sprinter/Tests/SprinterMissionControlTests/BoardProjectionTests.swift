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
