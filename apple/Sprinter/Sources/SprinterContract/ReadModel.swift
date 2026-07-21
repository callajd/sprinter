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
/// `done` and `cancelled` are BOTH terminal but distinct (CE5.1): a
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

/// The code hosts a ``Repository`` can be observed on — a CLOSED set on the contract.
///
/// It is an enum rather than a `String` because the daemon's set is closed: a host
/// value with no adapter behind it names a repository nothing can read. A tag this
/// mirror does not know is therefore a real decode failure, not something to render as
/// unknown — it means this client is older than the contract it is talking to.
public enum RepositoryHost: String, Codable, CaseIterable, Sendable {
  case github
}

/// One OBSERVED ref: a branch name paired with the commit its tip pointed at when the
/// repository was observed.
public struct RepositoryRef: Codable, Equatable, Sendable {
  public let name: BranchName
  public let sha: CommitSha

  public init(name: BranchName, sha: CommitSha) {
    self.name = name
    self.sha = sha
  }
}

/// A repository as OBSERVED on a code host — the anchor the state layer hangs from.
///
/// Unlike the owned nodes above it carries ``observedAt``: Sprinter does not create
/// repositories, it reads them off a host, so every record is a SNAPSHOT and says when
/// it was taken (INV-OBSERVED). Staleness is RENDERED from that stamp — neither the
/// daemon nor this mirror withholds a record for being old, so a client that wants to
/// show "last observed N minutes ago" has the evidence to do it.
///
/// ``refs`` is the observed ref map `BranchName → CommitSha`, on the wire as a LIST
/// ordered by branch name. The daemon models it that way deliberately: as a keyed
/// object a malformed branch name would be silently DROPPED on decode, while as a list
/// it fails loudly. An EMPTY list is valid — it means nothing has been observed yet,
/// NOT that the repository has no branches.
public struct Repository: Codable, Equatable, Sendable {
  public let id: RepositoryId
  public let host: RepositoryHost
  public let owner: String
  public let name: String
  public let refs: [RepositoryRef]
  /// The ISO-8601 UTC instant this observation was made.
  ///
  /// - Note: the TypeScript side models this as a `Timestamp` (canonical
  ///   `YYYY-MM-DDTHH:MM:SS.sssZ`), so lexicographic order IS chronological order. As
  ///   with ``Agent/retiredAt``, that property is NOT re-enforced here: this mirror is
  ///   DECODE-ONLY, and every value it sees was canonicalised by the daemon, which is
  ///   the only writer (the same posture, and the same expiry, as #89).
  public let observedAt: String

  public init(
    id: RepositoryId,
    host: RepositoryHost,
    owner: String,
    name: String,
    refs: [RepositoryRef],
    observedAt: String
  ) {
    self.id = id
    self.host = host
    self.owner = owner
    self.name = name
    self.refs = refs
    self.observedAt = observedAt
  }

  /// The commit `branch`'s tip pointed at when this repository was observed, or `nil`
  /// when that branch was not among the observed refs.
  ///
  /// `nil` means exactly "NOT OBSERVED" — never "the branch does not exist", which
  /// only the code host can say.
  public func tip(of branch: BranchName) -> CommitSha? {
    refs.first { $0.name == branch }?.sha
  }
}

/// A workstream: a related set of epics with one spec and one repository.
///
/// ``repositoryId`` REFERENCES a ``Repository`` carried in the same ``Snapshot`` (and
/// upserted by ``WorkGraphEvent/repositoryChanged(_:)``). It replaced a bare `repo`
/// string, and the difference is not cosmetic: a string is not an identity, so two
/// spellings of one repository were two different anchors and nothing could be
/// referenced from it.
public struct Workstream: Codable, Equatable, Sendable {
  public let id: WorkstreamId
  public let name: String
  public let repositoryId: RepositoryId
  public let status: WorkStatus
  public let epics: [EpicId]

  public init(
    id: WorkstreamId,
    name: String,
    repositoryId: RepositoryId,
    status: WorkStatus,
    epics: [EpicId]
  ) {
    self.id = id
    self.name = name
    self.repositoryId = repositoryId
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

/// The full owned state, hydrated on connect by the `snapshot` RPC: the read model
/// plus the registry layer (``agents``).
///
/// ``repositories`` is the STATE layer: every ``Repository`` the daemon has OBSERVED
/// on a code host. It is hydrated because ``Workstream/repositoryId`` is a REFERENCE
/// — a client holding workstreams without the repositories they name could resolve
/// none of them, nor render how stale each observation is.
///
/// ``agents`` is the WHOLE append-only registry, not a per-repository slice — an
/// ``Agent`` is global, so the per-repo view is a fold over that repo's executions
/// (INV-DERIVED). Retired and superseded revisions are included, because a
/// historical node may still resolve to one. Like every other collection here it is
/// a REQUIRED initializer parameter: the wire always carries the key, so a default
/// would let a construction site quietly omit a collection the contract guarantees.
///
/// ``generation`` is the identity of the daemon's STORE GENERATION — the coordinate
/// space this state's durable offsets live in. It is REQUIRED for the same reason,
/// and it is what a client must retain alongside the state: every cursor-bearing
/// resume (``EventsPayload``/``SessionEventsPayload``) hands it back, and a cursor
/// whose generation the daemon no longer has is refused with
/// ``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)`` rather than
/// silently resumed against a log it never belonged to. It is OPAQUE — equality is
/// the only defined operation; nothing may parse or order it.
public struct Snapshot: Codable, Equatable, Sendable {
  public let repositories: [Repository]
  public let workstreams: [Workstream]
  public let epics: [Epic]
  public let issues: [Issue]
  public let jobs: [Job]
  public let sessions: [Session]
  public let agents: [Agent]
  public let generation: StoreGenerationId

  public init(
    repositories: [Repository],
    workstreams: [Workstream],
    epics: [Epic],
    issues: [Issue],
    jobs: [Job],
    sessions: [Session],
    agents: [Agent],
    generation: StoreGenerationId
  ) {
    self.repositories = repositories
    self.workstreams = workstreams
    self.epics = epics
    self.issues = issues
    self.jobs = jobs
    self.sessions = sessions
    self.agents = agents
    self.generation = generation
  }
}
