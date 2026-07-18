import SprinterContract

/// Projects a baseline-consistent ``Snapshot`` into the view-facing
/// `BoardWorkstream ⊃ BoardEpic ⊃ BoardIssue` hierarchy (BE2.1 / D13).
///
/// The read model is stored flat (parallel arrays of workstreams, epics, issues,
/// jobs, sessions cross-referenced by id); the board is a tree. This is the pure,
/// `Sendable`, offline-testable transform that rebuilds the tree from one snapshot:
/// it walks each workstream's declared `epics`, each epic's declared `issues`, and
/// resolves the referenced nodes out of the snapshot's flat arrays — preserving the
/// declared child ordering rather than the flat-array order.
///
/// Every projection is a fresh full tree derived from one baseline-consistent
/// snapshot, so wiring it to ``WorkGraphResync/states()`` (snapshot-then-live, D4)
/// makes the board update as new snapshots arrive with no incremental-patch logic
/// of its own (INV-REACTIVE). A dangling child reference (a listed id absent from
/// the snapshot) is skipped rather than force-resolved (INV-NOFORCE); a
/// baseline-consistent snapshot never dangles, but the projection stays total.
public enum BoardProjection {
  /// Rebuilds the board tree from one snapshot.
  public static func project(_ snapshot: Snapshot) -> [BoardWorkstream] {
    let epicsById = indexed(snapshot.epics, by: \.id)
    let issuesById = indexed(snapshot.issues, by: \.id)
    let activityByIssue = liveActivity(in: snapshot)

    return snapshot.workstreams.map { workstream in
      let epics = workstream.epics.compactMap { epicsById[$0] }.map { epic in
        let issues = epic.issues.compactMap { issuesById[$0] }.map { issue in
          BoardIssue(
            id: issue.id,
            number: issue.number,
            title: issue.title,
            status: BoardStatus(issue.status),
            activity: activityByIssue[issue.id])
        }
        return BoardEpic(
          id: epic.id,
          name: epic.name,
          status: BoardStatus(epic.status),
          issues: issues)
      }
      return BoardWorkstream(
        id: workstream.id,
        name: workstream.name,
        repo: workstream.repo,
        status: BoardStatus(workstream.status),
        epics: epics)
    }
  }

  /// Which issues currently have a live agent, keyed by issue id.
  ///
  /// An issue is "live" when it has a `running` ``Job``, or a non-terminal job whose
  /// ``Session`` is `active` — the signals BE2.1 surfaces as per-agent activity. A
  /// TERMINAL job (`succeeded`/`failed`/`cancelled`) is never live even if a stale
  /// `active` session still points at it. The first live job encountered (in
  /// snapshot order) represents the issue, so activity is deterministic for a
  /// given snapshot.
  private static func liveActivity(in snapshot: Snapshot) -> [IssueId: IssueActivity] {
    let sessionsByJob = indexed(snapshot.sessions, by: \.jobId)
    var activity: [IssueId: IssueActivity] = [:]
    for job in snapshot.jobs {
      let session = sessionsByJob[job.id]
      let terminal = job.status == .succeeded || job.status == .failed || job.status == .cancelled
      let live = job.status == .running || (session?.status == .active && !terminal)
      guard live else { continue }
      if activity[job.issueId] == nil {
        // Prefer the job's declared session; fall back to the session that
        // references the job, so a live session is named even when `job.sessionId`
        // has not been persisted yet.
        activity[job.issueId] = IssueActivity(
          jobId: job.id, kind: job.kind, sessionId: job.sessionId ?? session?.id)
      }
    }
    return activity
  }

  /// Indexes a collection by a derived id, keeping the last element on a
  /// (baseline-inconsistent) duplicate rather than trapping (INV-NOFORCE).
  private static func indexed<Element, ID: Hashable>(
    _ elements: [Element],
    by id: (Element) -> ID
  ) -> [ID: Element] {
    Dictionary(elements.map { (id($0), $0) }, uniquingKeysWith: { _, last in last })
  }
}
