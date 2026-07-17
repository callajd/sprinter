/// `SprinterCore` — owned domain seed for the Swift client.
///
/// Scaffold-stage seed for the read model (`Workstream ⊃ Epic ⊃ Issue`, see
/// `conventions.md`). Owned domain types get **plain** names (`Workstream`,
/// `WorkStatus`); the Swift client is a *foreign consumer* of the RPC contract
/// (decision D10) and `FE2.4` builds the real contract bridge. This module is
/// intentionally minimal — it exists so the `make check` gate exercises a real
/// module with real logic (and real coverage) rather than an empty target.

/// Lifecycle status shared by work-graph nodes.
public enum WorkStatus: String, Sendable, CaseIterable, Codable {
  case pending
  case active
  case done
}

/// Raised when a work-graph node is constructed with invalid fields.
///
/// Errors are named `*Error` per `conventions.md`.
public enum WorkGraphError: Error, Equatable, Sendable {
  case emptyIdentifier
  case emptyName
}

/// A workstream: the top of the `Workstream ⊃ Epic ⊃ Issue` hierarchy.
public struct Workstream: Sendable, Equatable, Codable {
  /// Stable identifier (e.g. `"fdn"`).
  public let id: String
  /// Human-facing name (e.g. `"Foundation"`).
  public let name: String
  /// Lifecycle status.
  public let status: WorkStatus

  /// Constructs a workstream, rejecting empty identifiers or names.
  ///
  /// - Throws: ``WorkGraphError`` when `id` or `name` is empty.
  public init(id: String, name: String, status: WorkStatus) throws {
    guard !id.isEmpty else { throw WorkGraphError.emptyIdentifier }
    guard !name.isEmpty else { throw WorkGraphError.emptyName }
    self.id = id
    self.name = name
    self.status = status
  }

  /// Whether this workstream has reached its terminal state.
  public var isComplete: Bool {
    status == .done
  }
}
