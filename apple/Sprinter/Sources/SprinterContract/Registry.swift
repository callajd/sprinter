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
/// The registry is APPEND-ONLY: editing an agent mints a NEW revision whose
/// ``supersedes`` names the one it replaced, and retiring stamps ``retiredAt``.
/// Nothing is ever removed, so the contract exposes no delete and no
/// `AgentRemoved` delta.
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
  public let retiredAt: String?

  /// True when the agent has been retired — i.e. it carries a ``retiredAt`` stamp.
  /// The ONLY expression of retired-ness (INV-SUM).
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
