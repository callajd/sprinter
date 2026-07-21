import SprinterContract

/// A live agent working an ``Issue`` — the "per-agent activity" the board surfaces
/// alongside static status (BE2.1).
///
/// It is derived from the snapshot's ``Job``/``Execution`` model: an issue is being
/// worked when it has a `running` ``Job`` (or a `queued`/other job whose
/// ``Execution`` is `active`). The activity names the job driving the work, its
/// ``JobKind``, and the execution (if any) carrying the transcript — enough for the
/// board to show *which* agent is live, not merely that the issue is `inProgress`.
public struct IssueActivity: Sendable, Equatable {
  /// The job whose live execution this activity reflects.
  public let jobId: JobId
  /// What the live agent is doing (implement / review / …).
  public let kind: JobKind
  /// The execution carrying the run's transcript, when one has started.
  public let executionId: ExecutionId?

  public init(jobId: JobId, kind: JobKind, executionId: ExecutionId?) {
    self.jobId = jobId
    self.kind = kind
    self.executionId = executionId
  }
}

/// A leaf ``Issue`` as the board renders it: its number/title, its projected
/// ``BoardStatus``, and any live-agent ``IssueActivity``.
public struct BoardIssue: Sendable, Equatable, Identifiable {
  public let id: IssueId
  public let number: Int
  public let title: String
  public let status: BoardStatus
  /// The live agent working this issue, or `nil` when no agent is active.
  public let activity: IssueActivity?

  /// Whether an agent is currently working this issue (drives the board's activity
  /// affordance, distinct from the static ``status``).
  public var hasLiveAgent: Bool { activity != nil }

  public init(
    id: IssueId,
    number: Int,
    title: String,
    status: BoardStatus,
    activity: IssueActivity?
  ) {
    self.id = id
    self.number = number
    self.title = title
    self.status = status
    self.activity = activity
  }
}

/// An ``Epic`` as the board renders it: its projected ``BoardStatus`` and its
/// issues, in the epic's declared order.
public struct BoardEpic: Sendable, Equatable, Identifiable {
  public let id: EpicId
  public let name: String
  public let status: BoardStatus
  public let issues: [BoardIssue]

  public init(id: EpicId, name: String, status: BoardStatus, issues: [BoardIssue]) {
    self.id = id
    self.name = name
    self.status = status
    self.issues = issues
  }
}

/// A repo-scoped ``Workstream`` as the board renders it (D14: each workstream binds
/// one repository, so a multi-repo board is many workstreams). Carries its
/// projected ``BoardStatus`` and its epics, in the workstream's declared order.
public struct BoardWorkstream: Sendable, Equatable, Identifiable {
  public let id: WorkstreamId
  public let name: String
  /// The repository this workstream is scoped to, RENDERED as `owner/name`.
  ///
  /// It is a projected DISPLAY string, resolved from the observed `Repository` the
  /// workstream references — not the reference itself. The board renders a name; the
  /// identity lives on the reference itself, and a workstream whose repository
  /// is missing from the snapshot renders its raw id rather than dropping the row
  /// (INV-NOFORCE: the projection stays total).
  public let repo: String
  public let status: BoardStatus
  public let epics: [BoardEpic]

  public init(
    id: WorkstreamId,
    name: String,
    repo: String,
    status: BoardStatus,
    epics: [BoardEpic]
  ) {
    self.id = id
    self.name = name
    self.repo = repo
    self.status = status
    self.epics = epics
  }
}
