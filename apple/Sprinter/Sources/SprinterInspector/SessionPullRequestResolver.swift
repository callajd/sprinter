import SprinterContract

/// Resolves the PR a session produced by walking the ``Snapshot`` (BE4.1) — the
/// pure, `Sendable`, offline-testable transform behind the inspector's PR pane.
///
/// There is NO direct Session→PR field, so the resolution is transitive over the
/// read model, mirroring the session↔job join `BoardProjection.liveActivity` uses
/// (`indexed(by: \.jobId)`, `job.sessionId ?? session.id`):
///
/// 1. **session → job.** Prefer the session record's `jobId`; fall back to the job
///    that names this session (`job.sessionId == sessionId`), so the join holds even
///    when one side has not persisted its back-reference yet.
/// 2. **job → PR.** The PR is `job.pullRequest`; when the job carries none, follow
///    `job.issueId → issue.pullRequest` (the issue's closing PR).
///
/// "No PR yet" (a resolved job that has produced none) and "unresolved" (no job for
/// the session in this snapshot) are distinct first-class ``PullRequestPaneState``s
/// — never a force-unwrap or a crash (INV-NOFORCE).
public enum SessionPullRequestResolver {
  /// Resolves `sessionId`'s PR pane from one baseline-consistent snapshot.
  public static func resolve(sessionId: SessionId, in snapshot: Snapshot) -> PullRequestPane {
    guard let job = resolveJob(sessionId: sessionId, in: snapshot) else {
      return PullRequestPane(sessionId: sessionId, issueId: nil, state: .unresolved)
    }
    if let pullRequest = job.pullRequest ?? issuePullRequest(job.issueId, in: snapshot) {
      return PullRequestPane(sessionId: sessionId, issueId: job.issueId, state: .open(pullRequest))
    }
    return PullRequestPane(sessionId: sessionId, issueId: job.issueId, state: .awaitingPullRequest)
  }

  /// The job executing `sessionId`: the session's declared `jobId`, else the job
  /// that back-references the session.
  private static func resolveJob(sessionId: SessionId, in snapshot: Snapshot) -> Job? {
    let jobsById = indexed(snapshot.jobs, by: \.id)
    let session = snapshot.sessions.first { $0.id == sessionId }
    if let session, let job = jobsById[session.jobId] {
      return job
    }
    return snapshot.jobs.first { $0.sessionId == sessionId }
  }

  /// The closing PR of `issueId`, when the issue is present and carries one.
  private static func issuePullRequest(
    _ issueId: IssueId, in snapshot: Snapshot
  ) -> PullRequestRef? {
    snapshot.issues.first { $0.id == issueId }?.pullRequest
  }

  /// Indexes a collection by a derived id, keeping the last element on a
  /// (baseline-inconsistent) duplicate rather than trapping (INV-NOFORCE) — the same
  /// total, order-agnostic index `BoardProjection` uses.
  private static func indexed<Element, ID: Hashable>(
    _ elements: [Element],
    by id: (Element) -> ID
  ) -> [ID: Element] {
    Dictionary(elements.map { (id($0), $0) }, uniquingKeysWith: { _, last in last })
  }
}
