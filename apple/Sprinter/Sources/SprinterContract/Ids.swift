/// Branded identifier types for the owned read model (mirror of
/// `@sprinter/domain`'s branded `Schema.NonEmptyString` ids).
///
/// Each id is a distinct Swift type wrapping a `String`, so the compiler rejects
/// passing (say) an `EpicId` where a `WorkstreamId` is required — the Swift analog
/// of the contract's nominal `Schema.brand`. On the wire each is a plain JSON
/// string, so they encode/decode through a single-value container (never an
/// object with a `rawValue` key).

/// A string-backed identifier that codes as a bare JSON string.
///
/// Conformers get `Codable` for free via the single-value container below; the
/// non-emptiness the contract brands in is a producer-side guarantee, so the
/// mirror decodes the string as-is (INV-CONTRACT).
public protocol StringIdentifier: RawRepresentable, Codable, Hashable, Sendable
where RawValue == String {
  init(rawValue: String)
}

extension StringIdentifier {
  public init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    self.init(rawValue: try container.decode(String.self))
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(rawValue)
  }
}

/// Identifies a ``Workstream`` — the top of the `Workstream ⊃ Epic ⊃ Issue` hierarchy.
public struct WorkstreamId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies an ``Epic`` — a related set of issues within a workstream.
public struct EpicId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies an ``Issue`` — one ~PR-sized unit of code work.
public struct IssueId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies a ``Job`` — one bounded cognitive task.
public struct JobId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies a ``Session`` — one agent run executing a ``Job``.
public struct SessionId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies an ``Agent`` — a member of the registry layer: owned, global, scoped
/// to no repository.
///
/// The registry is append-only, so an `AgentId` identifies one immutable REVISION:
/// editing an agent mints a NEW id whose record points back at the previous one
/// through ``Agent/supersedes``.
public struct AgentId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}
