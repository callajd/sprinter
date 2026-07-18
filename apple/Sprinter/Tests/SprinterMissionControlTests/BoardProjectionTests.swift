import SprinterContract
import Testing

@testable import SprinterMissionControl

@Suite("Board projection — hierarchy, status, activity")
struct BoardProjectionTests {
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
  /// its kind, and its session); the done issue with no agent surfaces none.
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
          sessionId: SessionId(rawValue: "sess-a")))

    let idle = board.last?.epics.first?.issues.first
    #expect(idle?.hasLiveAgent == false)
    #expect(idle?.activity == nil)
  }

  /// Activity is derived from EITHER signal: a `running` job with no session is
  /// live, and a non-running job whose session is `active` is live; a queued job
  /// with a non-active session is not.
  @Test("live-agent activity comes from a running job or an active session")
  func activityFromEitherSignal() {
    let snapshot = BoardFixtures.singleEpicSnapshot(
      issueIds: ["iss-run", "iss-q", "iss-idle"],
      jobs: [
        BoardFixtures.job("job-run", issue: "iss-run", status: .running, session: nil),
        BoardFixtures.job("job-q", issue: "iss-q", status: .queued, session: "sess-q"),
        BoardFixtures.job("job-idle", issue: "iss-idle", status: .queued, session: "sess-idle")
      ],
      sessions: [
        Session(
          id: SessionId(rawValue: "sess-q"), jobId: JobId(rawValue: "job-q"), status: .active),
        Session(
          id: SessionId(rawValue: "sess-idle"), jobId: JobId(rawValue: "job-idle"), status: .idle)
      ])

    let issues = BoardProjection.project(snapshot).first?.epics.first?.issues ?? []
    let byId = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    #expect(byId[IssueId(rawValue: "iss-run")]?.hasLiveAgent == true)
    #expect(byId[IssueId(rawValue: "iss-q")]?.hasLiveAgent == true)
    #expect(byId[IssueId(rawValue: "iss-idle")]?.hasLiveAgent == false)
  }

  /// A TERMINAL job is not a live agent even with a lingering `active` session; and
  /// a job made live by its session names that session when the job's own
  /// `sessionId` is nil.
  @Test("a terminal job is not live; an active session's id is the fallback")
  func terminalExclusionAndSessionFallback() {
    let snapshot = BoardFixtures.singleEpicSnapshot(
      issueIds: ["iss-done", "iss-fb"],
      jobs: [
        BoardFixtures.job("job-done", issue: "iss-done", status: .succeeded, session: nil),
        BoardFixtures.job("job-fb", issue: "iss-fb", status: .queued, session: nil)
      ],
      sessions: [
        Session(
          id: SessionId(rawValue: "sess-done"), jobId: JobId(rawValue: "job-done"), status: .active),
        Session(
          id: SessionId(rawValue: "sess-fb"), jobId: JobId(rawValue: "job-fb"), status: .active)
      ])

    let issues = BoardProjection.project(snapshot).first?.epics.first?.issues ?? []
    let byId = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    // N2: a succeeded job with a stale active session is NOT a live agent.
    #expect(byId[IssueId(rawValue: "iss-done")]?.hasLiveAgent == false)
    // N1: a job live via its active session (declared sessionId nil) names it.
    let fallback = byId[IssueId(rawValue: "iss-fb")]
    #expect(fallback?.hasLiveAgent == true)
    #expect(fallback?.activity?.sessionId == SessionId(rawValue: "sess-fb"))
  }

  /// A child id listed by its parent but absent from the snapshot is skipped, not
  /// force-resolved (INV-NOFORCE) — the projection stays total on a dangling ref.
  @Test("skips a dangling child reference")
  func skipsDanglingReference() {
    let snapshot = Snapshot(
      workstreams: [
        Workstream(
          id: WorkstreamId(rawValue: "ws"),
          name: "W",
          repo: "callajd/sprinter",
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
      sessions: [])

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
