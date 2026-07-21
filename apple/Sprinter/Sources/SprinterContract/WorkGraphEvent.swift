/// A single work-graph delta streamed by the `events` RPC (INV-REACTIVE).
///
/// The contract's `Schema.TaggedUnion` inlines a `_tag` discriminant alongside the
/// variant's single carried node, e.g. `{ "_tag": "IssueChanged", "issue": { … } }`.
/// The mirror decodes by switching on `_tag`; an unknown tag is a decode failure
/// (a contract the mirror does not understand), never a silent drop.
public enum WorkGraphEvent: Codable, Equatable, Sendable {
  /// The STATE layer's delta. A repository record is REPLACED WHOLESALE on every
  /// refresh, under a new `observedAt`, so this always carries the complete new
  /// observation and an upsert by id is exactly right. There is deliberately no
  /// removal variant — a repository leaves the graph only with the whole store.
  case repositoryChanged(Repository)
  case workstreamChanged(Workstream)
  case epicChanged(Epic)
  case issueChanged(Issue)
  case jobChanged(Job)
  case executionChanged(Execution)
  case agentChanged(Agent)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case repository
    case workstream
    case epic
    case issue
    case job
    case execution
    case agent
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "RepositoryChanged":
      self = .repositoryChanged(try container.decode(Repository.self, forKey: .repository))
    case "WorkstreamChanged":
      self = .workstreamChanged(try container.decode(Workstream.self, forKey: .workstream))
    case "EpicChanged":
      self = .epicChanged(try container.decode(Epic.self, forKey: .epic))
    case "IssueChanged":
      self = .issueChanged(try container.decode(Issue.self, forKey: .issue))
    case "JobChanged":
      self = .jobChanged(try container.decode(Job.self, forKey: .job))
    case "ExecutionChanged":
      self = .executionChanged(try container.decode(Execution.self, forKey: .execution))
    case "AgentChanged":
      self = .agentChanged(try container.decode(Agent.self, forKey: .agent))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown WorkGraphEvent tag: \(tag)"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .repositoryChanged(let value):
      try container.encode("RepositoryChanged", forKey: .tag)
      try container.encode(value, forKey: .repository)
    case .workstreamChanged(let value):
      try container.encode("WorkstreamChanged", forKey: .tag)
      try container.encode(value, forKey: .workstream)
    case .epicChanged(let value):
      try container.encode("EpicChanged", forKey: .tag)
      try container.encode(value, forKey: .epic)
    case .issueChanged(let value):
      try container.encode("IssueChanged", forKey: .tag)
      try container.encode(value, forKey: .issue)
    case .jobChanged(let value):
      try container.encode("JobChanged", forKey: .tag)
      try container.encode(value, forKey: .job)
    case .executionChanged(let value):
      try container.encode("ExecutionChanged", forKey: .tag)
      try container.encode(value, forKey: .execution)
    case .agentChanged(let value):
      try container.encode("AgentChanged", forKey: .tag)
      try container.encode(value, forKey: .agent)
    }
  }
}

/// A single streamed `events` item: a ``WorkGraphEvent`` paired with its DURABLE
/// `offset` — the daemon's `event_log` position the delta was journaled at
/// (CE2.0). The stream carries the offset so a reconnecting client can remember
/// its last-seen position and hand it back as the request's `sinceOffset` cursor to
/// resume STRICTLY AFTER it (no gap, no duplicate). Wire shape:
/// `{ "offset": 12, "event": { "_tag": "IssueChanged", "issue": { … } } }`.
///
/// The offset is the contract's `NonNegativeInt`, which encodes as a bare JSON
/// integer, so the mirror maps it to `Int`. Consumers that only need the delta
/// unwrap ``event``; tracking the offset for resume is CE2.2's job.
public struct OffsetEvent: Codable, Equatable, Sendable {
  public let offset: Int
  public let event: WorkGraphEvent

  public init(offset: Int, event: WorkGraphEvent) {
    self.offset = offset
    self.event = event
  }
}
