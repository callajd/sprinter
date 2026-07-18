/// A single work-graph delta streamed by the `events` RPC (INV-REACTIVE).
///
/// The contract's `Schema.TaggedUnion` inlines a `_tag` discriminant alongside the
/// variant's single carried node, e.g. `{ "_tag": "IssueChanged", "issue": { … } }`.
/// The mirror decodes by switching on `_tag`; an unknown tag is a decode failure
/// (a contract the mirror does not understand), never a silent drop.
public enum WorkGraphEvent: Codable, Equatable, Sendable {
  case workstreamChanged(Workstream)
  case epicChanged(Epic)
  case issueChanged(Issue)
  case jobChanged(Job)
  case sessionChanged(Session)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case workstream
    case epic
    case issue
    case job
    case session
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "WorkstreamChanged":
      self = .workstreamChanged(try container.decode(Workstream.self, forKey: .workstream))
    case "EpicChanged":
      self = .epicChanged(try container.decode(Epic.self, forKey: .epic))
    case "IssueChanged":
      self = .issueChanged(try container.decode(Issue.self, forKey: .issue))
    case "JobChanged":
      self = .jobChanged(try container.decode(Job.self, forKey: .job))
    case "SessionChanged":
      self = .sessionChanged(try container.decode(Session.self, forKey: .session))
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
    case .sessionChanged(let value):
      try container.encode("SessionChanged", forKey: .tag)
      try container.encode(value, forKey: .session)
    }
  }
}
