/// The owned read model — `Workstream ⊃ Epic ⊃ Issue`, plus ``Job`` and
/// ``Session`` — mirrored from `@sprinter/domain`'s `Schema.Struct`s.
///
/// Optional-key fields (`Schema.optionalKey`) are modelled as Swift optionals: the
/// contract OMITS the key when absent, and synthesized `Codable` maps a missing key
/// to `nil` (and omits `nil` on encode), so the wire shape matches exactly. Fields
/// whose wire name is not the Swift property name (`pr`) map via `CodingKeys`.
///
/// The read-model status enums live here (next to the nodes that carry them),
/// mirrored as `String`-raw enums whose raw values are the exact wire tokens
/// (`Schema.Literals`). Tokens that are not valid Swift identifiers (e.g.
/// `resolve-conflict`) carry an explicit raw value.

/// Lifecycle status shared by the planning nodes ``Workstream`` and ``Epic``.
///
/// `done` and `cancelled` are BOTH terminal but distinct (contract v2 / CE5.1): a
/// cancelled node was abandoned, not finished, so the board renders it apart from a
/// completed one.
public enum WorkStatus: String, Codable, CaseIterable, Sendable {
  case pending
  case active
  case done
  case blocked
  case cancelled
}

/// Issue lifecycle status.
public enum IssueStatus: String, Codable, CaseIterable, Sendable {
  case pending
  case ready
  case inProgress = "in_progress"
  case inReview = "in_review"
  case done
  case blocked
}

/// Job kinds — an open set the daemon core is agnostic to.
public enum JobKind: String, Codable, CaseIterable, Sendable {
  case implement
  case review
  case resolveConflict = "resolve-conflict"
  case addressFindings = "address-findings"
  case plan
}

/// Job execution status.
public enum JobStatus: String, Codable, CaseIterable, Sendable {
  case queued
  case running
  case succeeded
  case failed
  case cancelled
}

/// Session lifecycle status.
public enum SessionStatus: String, Codable, CaseIterable, Sendable {
  case starting
  case active
  case idle
  case interrupted
  case completed
  case failed
}

/// A reference to the pull request that closes an ``Issue``.
public struct PullRequestRef: Codable, Equatable, Sendable {
  public let number: Int
  public let url: String
  public let merged: Bool

  public init(number: Int, url: String, merged: Bool) {
    self.number = number
    self.url = url
    self.merged = merged
  }
}

/// An issue: one ~PR-sized feature, naming its parent epic and — once opened —
/// carrying the one PR that closes it.
public struct Issue: Codable, Equatable, Sendable {
  public let id: IssueId
  public let epicId: EpicId
  public let number: Int
  public let title: String
  public let status: IssueStatus
  public let dependsOn: [IssueId]
  public let pullRequest: PullRequestRef?

  private enum CodingKeys: String, CodingKey {
    case id
    case epicId
    case number
    case title
    case status
    case dependsOn
    case pullRequest = "pr"
  }

  public init(
    id: IssueId,
    epicId: EpicId,
    number: Int,
    title: String,
    status: IssueStatus,
    dependsOn: [IssueId],
    pullRequest: PullRequestRef?
  ) {
    self.id = id
    self.epicId = epicId
    self.number = number
    self.title = title
    self.status = status
    self.dependsOn = dependsOn
    self.pullRequest = pullRequest
  }
}

/// An epic: a related set of issues, naming its parent workstream.
public struct Epic: Codable, Equatable, Sendable {
  public let id: EpicId
  public let workstreamId: WorkstreamId
  public let name: String
  public let status: WorkStatus
  public let issues: [IssueId]

  public init(
    id: EpicId,
    workstreamId: WorkstreamId,
    name: String,
    status: WorkStatus,
    issues: [IssueId]
  ) {
    self.id = id
    self.workstreamId = workstreamId
    self.name = name
    self.status = status
    self.issues = issues
  }
}

/// A workstream: a related set of epics with one spec and one repo.
public struct Workstream: Codable, Equatable, Sendable {
  public let id: WorkstreamId
  public let name: String
  public let repo: String
  public let status: WorkStatus
  public let epics: [EpicId]

  public init(
    id: WorkstreamId,
    name: String,
    repo: String,
    status: WorkStatus,
    epics: [EpicId]
  ) {
    self.id = id
    self.name = name
    self.repo = repo
    self.status = status
    self.epics = epics
  }
}

/// A job: one bounded cognitive task, run as one ``Session``, paired 1:1 with one PR.
public struct Job: Codable, Equatable, Sendable {
  public let id: JobId
  public let issueId: IssueId
  public let kind: JobKind
  public let status: JobStatus
  public let sessionId: SessionId?
  public let transcriptRef: String?
  public let pullRequest: PullRequestRef?

  private enum CodingKeys: String, CodingKey {
    case id
    case issueId
    case kind
    case status
    case sessionId
    case transcriptRef
    case pullRequest = "pr"
  }

  public init(
    id: JobId,
    issueId: IssueId,
    kind: JobKind,
    status: JobStatus,
    sessionId: SessionId?,
    transcriptRef: String?,
    pullRequest: PullRequestRef?
  ) {
    self.id = id
    self.issueId = issueId
    self.kind = kind
    self.status = status
    self.sessionId = sessionId
    self.transcriptRef = transcriptRef
    self.pullRequest = pullRequest
  }
}

/// A session: one agent run executing a ``Job``.
public struct Session: Codable, Equatable, Sendable {
  public let id: SessionId
  public let jobId: JobId
  public let status: SessionStatus

  public init(id: SessionId, jobId: JobId, status: SessionStatus) {
    self.id = id
    self.jobId = jobId
    self.status = status
  }
}

/// The full owned read-model state, hydrated on connect by the `snapshot` RPC.
public struct Snapshot: Codable, Equatable, Sendable {
  public let workstreams: [Workstream]
  public let epics: [Epic]
  public let issues: [Issue]
  public let jobs: [Job]
  public let sessions: [Session]

  public init(
    workstreams: [Workstream],
    epics: [Epic],
    issues: [Issue],
    jobs: [Job],
    sessions: [Session]
  ) {
    self.workstreams = workstreams
    self.epics = epics
    self.issues = issues
    self.jobs = jobs
    self.sessions = sessions
  }
}
