import SprinterContract

/// The board-facing lifecycle a Mission Control node surfaces (BE2.1).
///
/// The read model carries two distinct lifecycle vocabularies — ``WorkStatus`` on
/// the planning nodes (``Workstream``/``Epic``) and ``IssueStatus`` on the leaf
/// ``Issue`` — but the board renders one uniform status per node so the
/// `Workstream ⊃ Epic ⊃ Issue` hierarchy reads coherently top to bottom. This enum
/// is that projection target; the two `init`s below are the total mappings from
/// each source vocabulary (INV-CONTRACT: derived from the mirrored DTOs, never a
/// redefined wire type).
public enum BoardStatus: String, Sendable, Equatable, CaseIterable {
  /// Nothing has begun yet (`WorkStatus.pending`; `IssueStatus.pending`/`.ready`).
  case notYetStarted
  /// Work is in flight (`WorkStatus.active`; `IssueStatus.inProgress`/`.inReview`).
  case ongoing
  /// Work is stalled and needs attention (`.blocked` on either vocabulary).
  case paused
  /// Work has reached its terminal state (`.done` on either vocabulary).
  case complete
}

extension BoardStatus {
  /// Projects a planning-node ``WorkStatus`` onto the board vocabulary. Total.
  public init(_ status: WorkStatus) {
    switch status {
    case .pending: self = .notYetStarted
    case .active: self = .ongoing
    case .blocked: self = .paused
    case .done: self = .complete
    }
  }

  /// Projects a leaf ``IssueStatus`` onto the board vocabulary. Total: the two
  /// pre-work states (`pending`/`ready`) and the two in-flight states
  /// (`inProgress`/`inReview`) each collapse to one board status.
  public init(_ status: IssueStatus) {
    switch status {
    case .pending, .ready: self = .notYetStarted
    case .inProgress, .inReview: self = .ongoing
    case .blocked: self = .paused
    case .done: self = .complete
    }
  }
}
