import SprinterContract

/// Applies a live ``WorkGraphEvent`` delta onto a ``Snapshot`` baseline — the
/// reconciliation half of the snapshot-then-stream resync (D4).
///
/// On (re)connect the client hydrates the full baseline via the `snapshot` RPC,
/// then folds each streamed `events` delta onto it with this reconciler. Every
/// delta UPSERTS its node by id — replacing the node already in the baseline or
/// appending a new one — so the derived state stays consistent without a
/// delta-only stream that could miss the pre-subscribe gap. The fold is a pure
/// value transform (no I/O, no shared state), so it is trivially `Sendable` and
/// exhaustively testable offline.
public struct SnapshotReconciler: Sendable {
  public init() {}

  /// Folds one delta onto `snapshot`, returning the updated baseline.
  public func reconcile(_ snapshot: Snapshot, applying event: WorkGraphEvent) -> Snapshot {
    switch event {
    // The STATE layer folds under the same upsert rule. A repository record is
    // REPLACED WHOLESALE on every refresh (a new observation under a new
    // `observedAt`), so the carried value is always complete and replacing the one
    // in the baseline is exactly right — there is nothing to merge.
    case .repositoryChanged(let repository):
      return snapshot.replacing(
        repositories: Self.upsert(repository, into: snapshot.repositories, by: \.id))
    case .workstreamChanged(let workstream):
      return snapshot.replacing(
        workstreams: Self.upsert(workstream, into: snapshot.workstreams, by: \.id))
    case .epicChanged(let epic):
      return snapshot.replacing(epics: Self.upsert(epic, into: snapshot.epics, by: \.id))
    case .issueChanged(let issue):
      return snapshot.replacing(issues: Self.upsert(issue, into: snapshot.issues, by: \.id))
    case .jobChanged(let job):
      return snapshot.replacing(jobs: Self.upsert(job, into: snapshot.jobs, by: \.id))
    case .sessionChanged(let session):
      return snapshot.replacing(sessions: Self.upsert(session, into: snapshot.sessions, by: \.id))
    // The registry folds under the SAME upsert rule, and in practice the upsert is
    // always an APPEND: a stored revision is immutable, so both an edit and a
    // retirement arrive as a NEW id linked by `supersedes` (the retirement also
    // carrying `retiredAt`). A registry delta is never a removal, and never a
    // rewrite of a revision already in the baseline.
    case .agentChanged(let agent):
      return snapshot.replacing(agents: Self.upsert(agent, into: snapshot.agents, by: \.id))
    }
  }

  /// Replaces the element sharing `element`'s id, or appends it when absent — an
  /// order-preserving upsert (a new node lands at the end).
  private static func upsert<Element, ID: Equatable>(
    _ element: Element,
    into elements: [Element],
    by id: (Element) -> ID
  ) -> [Element] {
    let key = id(element)
    if let index = elements.firstIndex(where: { id($0) == key }) {
      var updated = elements
      updated[index] = element
      return updated
    }
    return elements + [element]
  }
}

extension Snapshot {
  /// Returns a copy with the given collections swapped in; an omitted argument
  /// keeps the current value. Keeps ``SnapshotReconciler`` free of the full
  /// initializer boilerplate at each case.
  ///
  /// The ``Snapshot/generation`` is deliberately NOT replaceable: it is the coordinate
  /// space this baseline was hydrated in, and folding a delta never moves the baseline to
  /// a different one. A delta from another generation cannot reach here at all — the
  /// daemon refuses the resume that would have carried it.
  fileprivate func replacing(
    repositories: [Repository]? = nil,
    workstreams: [Workstream]? = nil,
    epics: [Epic]? = nil,
    issues: [Issue]? = nil,
    jobs: [Job]? = nil,
    sessions: [Session]? = nil,
    agents: [Agent]? = nil
  ) -> Snapshot {
    Snapshot(
      repositories: repositories ?? self.repositories,
      workstreams: workstreams ?? self.workstreams,
      epics: epics ?? self.epics,
      issues: issues ?? self.issues,
      jobs: jobs ?? self.jobs,
      sessions: sessions ?? self.sessions,
      agents: agents ?? self.agents,
      generation: generation)
  }
}
