/// The owned REGISTRY layer — global entities scoped to no repository — mirrored
/// from `@sprinter/domain`'s `registry.ts`.
///
/// ``Agent`` is its only member today. Optional-key fields (`Schema.optionalKey`)
/// are modelled as Swift optionals: the contract OMITS the key when absent, and
/// synthesized `Codable` maps a missing key to `nil` (and omits `nil` on encode),
/// so the wire shape matches exactly.

/// An agent in the registry: the identity that RUNS work.
///
/// Global by construction — it names no repository and no workstream, so "the
/// agents used in this repo" is a fold the client computes over that repo's
/// executions rather than a list carried on the agent (INV-DERIVED).
///
/// The registry is APPEND-ONLY and a stored revision is IMMUTABLE, so BOTH
/// mutating operations arrive as an append under a NEW id:
///
/// - an EDIT is a new revision whose ``supersedes`` names the lineage head it
///   replaces;
/// - a RETIREMENT is a new revision carrying BOTH ``supersedes`` AND a
///   ``retiredAt`` stamp — never the same id restamped, because the revision it
///   retires must stay resolvable for the executions that ran on it.
///
/// Nothing is ever removed or rewritten, so the contract exposes no delete and no
/// `AgentRemoved` delta, and a client folds every ``AgentChanged`` as an
/// upsert-by-id that in practice always appends.
///
/// Retired-ness is read off ``retiredAt``'s presence (``isRetired``) — there is
/// deliberately no `AgentStatus` enum to keep in sync with it (INV-SUM).
public struct Agent: Codable, Equatable, Sendable {
  /// Identifies this REVISION of the agent (a new revision has a new id).
  public let id: AgentId
  /// The human-facing name of the agent.
  public let name: String
  /// The model it drives.
  public let model: String
  /// The revision of its definition.
  public let version: String
  /// The tool names it is permitted to use, in declaration order.
  public let tools: [String]
  /// The PREVIOUS revision this record replaces; `nil` on the first revision.
  public let supersedes: AgentId?
  /// The ISO-8601 UTC instant the agent was retired; `nil` while it is in service.
  ///
  /// - Note: the TypeScript side models this as a `Timestamp` — a CANONICAL form
  ///   (`YYYY-MM-DDTHH:MM:SS.sssZ`, always UTC, always millisecond precision) chosen so
  ///   that lexicographic order IS chronological order across the wire, SQLite `TEXT`,
  ///   and this mirror. This property is NOT enforced here: the mirror is a bare
  ///   `String?`, so a non-canonical instant would decode without complaint and compare
  ///   wrongly. It is inert today because Swift is DECODE-ONLY on this field — every
  ///   value it sees was canonicalised by the daemon, which is the only writer — and it
  ///   becomes real the moment the client mints or edits an ``Agent`` — the SAME
  ///   decode-only reprieve, and the same expiry, as the unasserted encode-side mirror
  ///   fidelity tracked by issue #89 (a client-authored payload is what ends both).
  public let retiredAt: String?

  /// True when THIS RECORD carries a ``retiredAt`` stamp. The only expression of a
  /// record's retired-ness (INV-SUM).
  ///
  /// - Important: this is deliberately NOT the question "is this agent still in
  ///   service". Under append-only semantics, retiring `agt-2` appends a NEW revision
  ///   `agt-3` carrying `supersedes: agt-2` and the stamp; `agt-2` is immutable and
  ///   stays un-stamped forever, so `agt-2.isRetired` is `false` even though its
  ///   lineage is retired — correctly, because the RECORD never was. UI that means
  ///   "still in service" must ask ``isLineageRetired(_:in:)`` instead; reaching for
  ///   this one renders a retired lineage as live.
  public var isRetired: Bool { retiredAt != nil }

  /// True when this is the FIRST revision of its lineage — it replaces nothing.
  public var isOriginalRevision: Bool { supersedes == nil }

  public init(
    id: AgentId,
    name: String,
    model: String,
    version: String,
    tools: [String],
    supersedes: AgentId?,
    retiredAt: String?
  ) {
    self.id = id
    self.name = name
    self.model = model
    self.version = version
    self.tools = tools
    self.supersedes = supersedes
    self.retiredAt = retiredAt
  }
}

/// True when `agent`'s LINEAGE has been retired — i.e. `agent` itself carries a
/// ``Agent/retiredAt`` stamp, or some revision in `all` retires it, directly or
/// through a chain of later revisions. Mirror of `@sprinter/domain`'s
/// `isLineageRetired`.
///
/// This is the "is this agent still in service" question, and it is the one a UI
/// almost always means. ``Agent/isRetired`` answers a question about ONE RECORD, and
/// under append-only semantics the two differ: a retirement is a NEW revision carrying
/// the stamp, so the revision it retires stays un-stamped forever and reads as live.
///
/// Answering it needs the REVERSE of the ``Agent/supersedes`` link, which no single
/// revision carries. This derives that reverse index from a whole-registry collection
/// — superseded and retired revisions included, exactly what ``Snapshot/agents``
/// hands over — and walks FORWARD from `agent` until it reaches a stamped revision or
/// the head of the lineage. Revisions belonging to other lineages are ignored.
///
/// The answer does not depend on `all`'s order: each revision has at most one successor,
/// so there is exactly one forward path. The daemon's store makes a forked lineage (one
/// revision superseded twice) UNSTORABLE — a UNIQUE index on `supersedes` — so the
/// history that would let the walk pick a branch never reaches a client.
///
/// - Complexity: O(n) in `all` per call — the reverse index is rebuilt each time, so
///   folding this over a whole registry is O(n²). Fine for a lookup; if a view ever
///   folds it across every revision, build the successor index once at the call site.
///
/// The walk visits each revision at most once, so it terminates even on the cyclic
/// `supersedes` structure the precondition forbids.
public func isLineageRetired(_ agent: Agent, in all: some Sequence<Agent>) -> Bool {
  var successors: [AgentId: Agent] = [:]
  for revision in all {
    if let supersedes = revision.supersedes, successors[supersedes] == nil {
      successors[supersedes] = revision
    }
  }
  var seen: Set<AgentId> = []
  var current: Agent? = agent
  while let revision = current, !seen.contains(revision.id) {
    if revision.isRetired { return true }
    seen.insert(revision.id)
    current = successors[revision.id]
  }
  return false
}
