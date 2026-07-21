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

/// Identifies a ``Repository`` — a repository as OBSERVED on a code host, and the
/// anchor the state layer hangs from (``Workstream/repositoryId``).
///
/// It is OPAQUE. Equality is the only defined operation — nothing may parse it, order
/// it, or read an owner/name back out of it. The NATURAL key of a repository is its
/// `(host, owner, name)` triple, which the daemon holds unique.
public struct RepositoryId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// A full git commit object name — 40 lowercase hex characters on the contract.
///
/// **The mirror is DECODE-ONLY and does NOT re-validate**, exactly as ``Timestamp``'s
/// mirror does not (same posture, cross-referencing #89). The daemon's schema is the
/// single point of truth for what a commit sha is: it decodes every externally-sourced
/// sha at the boundary and refuses a malformed one there, so anything reaching this
/// type has already been checked. Re-checking here would put a SECOND definition of
/// "valid" in the tree — one that can drift from the daemon's — and a client that
/// rejected a value the daemon accepted would simply be unable to render state the
/// daemon considers real. So this wrapper carries the string as-is and exists for TYPE
/// SAFETY: it keeps a sha from being passed where a branch name belongs.
public struct CommitSha: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// A git branch name.
///
/// Decode-only and NOT re-validated, for the same reason as ``CommitSha`` above (and
/// ``Timestamp``, #89): the daemon's schema decides what a branch name is, at the one
/// boundary every externally-sourced name crosses.
public struct BranchName: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}

/// Identifies one STORE GENERATION — the lifetime of a single durable daemon store,
/// from the moment its schema is created to the moment it is dropped and recreated.
///
/// The daemon's store NEVER migrates: bumping its schema version drops the database
/// and recreates it, restarting every durable offset at `1`. A durable offset is
/// therefore only meaningful WITHIN the generation it was minted in, and this id is
/// what makes that context explicit on the wire: the client reads it off
/// ``Snapshot/generation``, retains it with the state, and hands it back with every
/// resume cursor. A cursor whose generation the daemon no longer has is refused
/// (``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)``) instead of
/// being resumed against a log it never belonged to.
///
/// It is OPAQUE. Equality is the only defined operation — nothing may parse it,
/// order it, or infer age or version from it.
public struct StoreGenerationId: StringIdentifier {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
}
