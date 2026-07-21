import SprinterContract

/// Projects a baseline-consistent ``Snapshot`` into the view-facing
/// `BoardWorkstream ⊃ BoardEpic ⊃ BoardIssue` hierarchy (BE2.1 / D13).
///
/// The read model is stored flat (parallel arrays of workstreams, epics, issues,
/// jobs, executions cross-referenced by id); the board is a tree. This is the pure,
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
    let repositoriesById = indexed(snapshot.repositories, by: \.id)
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
      // The workstream carries a REFERENCE; the board shows a name. Resolve it out
      // of the snapshot's own state layer, and fall back to the raw id when the
      // referenced record is absent.
      //
      // The fallback is a TOTALITY guarantee, not a prediction that it never fires.
      // The daemon builds a snapshot by reading workstreams first and repositories
      // last, precisely so a concurrently-materialised plan cannot leave a workstream
      // here whose repository is missing (`buildSnapshot`, `rpc-handlers.ts`) — but
      // that is the daemon's ordering discipline, not something this projection can
      // check, and a raw id on screen is strictly better than a dropped workstream or
      // a force-unwrap (INV-NOFORCE).
      let repository = repositoriesById[workstream.repositoryId]
      return BoardWorkstream(
        id: workstream.id,
        name: workstream.name,
        repo: repository.map { "\($0.owner)/\($0.name)" } ?? workstream.repositoryId.rawValue,
        status: BoardStatus(workstream.status),
        epics: epics)
    }
  }

  /// Which issues currently have a live agent, keyed by issue id.
  ///
  /// An issue is "live" when it has a `running` ``Job``, or a non-terminal job whose
  /// ``Execution`` is still running — i.e. its transcript is still OPEN
  /// (``Execution/isLive``, DE2.2; there is no execution-status enum to consult). A
  /// TERMINAL job (`succeeded`/`failed`/`cancelled`) is never live even if a stale
  /// live execution still points at it. The first live job encountered (in snapshot
  /// order) represents the issue, so activity is deterministic for a given snapshot.
  ///
  /// A job may now own a TREE of executions, so the index below keeps the job's LIVE
  /// one when there is one: "is this issue live?" must not depend on which sibling
  /// happened to come last in the snapshot.
  ///
  /// DELIBERATE WIDENING (DE2.2). This clause used to read `execution?.status == .active`
  /// and was DEAD — nothing ever wrote `active`, so liveness came from `job.status ==
  /// .running` alone. Replacing it with ``Execution/isLive`` is the first time the
  /// execution clause decides anything, and it does widen the board: a non-terminal job
  /// that is not `running` but holds an open transcript now renders live where it never
  /// did. That is INTENDED. Liveness IS the transcript variant in the remodelled domain —
  /// there is no execution-status enum left to consult, and narrowing back to
  /// `job.status == .running` would put the board's answer back on the job's status enum
  /// and make the transcript decorative, which is the second-source-of-truth the model
  /// removed (`INV-SUM` / `INV-ENFORCE`). The state it newly surfaces — an open
  /// transcript on a job nobody is driving — is the live-orphan the daemon's startup
  /// seal exists to clear (CE4.1-R4); showing it is how a stall becomes visible rather
  /// than a job that silently reads idle. The `!terminal` guard is what keeps the
  /// widening bounded: a stale live execution on a settled job is still never live.
  private static func liveActivity(in snapshot: Snapshot) -> [IssueId: IssueActivity] {
    let executionsByJob = indexedPreferringLive(snapshot.executions)
    var activity: [IssueId: IssueActivity] = [:]
    for job in snapshot.jobs {
      let execution = executionsByJob[job.id]
      let terminal = job.status == .succeeded || job.status == .failed || job.status == .cancelled
      let live = job.status == .running || (execution?.isLive == true && !terminal)
      guard live else { continue }
      if activity[job.issueId] == nil {
        // Prefer the job's declared execution; fall back to the execution that
        // references the job, so a live execution is named even when `job.executionId`
        // has not been persisted yet.
        //
        // NOTE THE ASYMMETRY, which is deliberate: `live` above may have been decided by
        // a LIVE CHILD that `indexedPreferringLive` kept over a sealed root, while the id
        // named here is still the job's DECLARED execution — i.e. the root. So an
        // activity can name a SEALED execution. That is the right id to name: the job's
        // declared execution is its dispatch ROOT, the same one `job.transcriptRef` is
        // derived from, so naming it keeps the activity's id and the job's transcript
        // reference pointing at one thing rather than at a subagent the user never
        // dispatched. Liveness is a BOOLEAN about the whole tree; the id is the tree's
        // entry point. Pinned by `queuedJobLivenessTracksTheSeal`, so a change here is a
        // decision rather than a drift. (An affordance that must name the live NODE has
        // to read `snapshot.executions` — see `indexedPreferringLive`.)
        activity[job.issueId] = IssueActivity(
          jobId: job.id, kind: job.kind, executionId: job.executionId ?? execution?.id)
      }
    }
    return activity
  }

  /// Indexes the executions by job, keeping a LIVE one over a settled one — a job owns
  /// a tree of executions (DE2.2), so "the execution for this job" must not be decided
  /// by snapshot order. Among equals the last wins, as before (never a trap,
  /// INV-NOFORCE).
  ///
  /// LOSSY BY DESIGN, and the snapshot genuinely exercises it: since the daemon's
  /// `buildSnapshot` ships a job's WHOLE tree (not just its root), a job with N executions
  /// arrives with N entries here and this reduces them to ONE. That is correct for the
  /// board's question — "does this issue have a live agent?" is a BOOLEAN, and one live
  /// execution answers it — but it is a real projection, not an index: the siblings it
  /// drops are not available downstream, and `IssueActivity.executionId` names the kept
  /// one. Any future board affordance that needs to say something ABOUT the tree (a count,
  /// a subagent list, a per-execution row) must read `snapshot.executions` directly rather
  /// than reaching for this dictionary.
  private static func indexedPreferringLive(_ executions: [Execution]) -> [JobId: Execution] {
    Dictionary(
      executions.map { ($0.jobId, $0) },
      uniquingKeysWith: { first, last in
        first.isLive ? first : last
      })
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
