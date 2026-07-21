/// The command payloads the RPC procedures carry and the contract's owned error
/// types — mirrored from `@sprinter/contract`'s procedure surface.
///
/// Each `payload` struct mirrors one procedure's request shape; the streamed and
/// request/response success values are owned types (``Snapshot``, the
/// ``OffsetEvent`` envelope over ``WorkGraphEvent``, ``ExecutionEvent``, a bare
/// ``WorkstreamId``) already mirrored elsewhere in this module.

/// The lifecycle action a `control` command applies to a workstream.
public enum ControlAction: String, Codable, CaseIterable, Sendable {
  case start
  case pause
  case resume
  case cancel
}

/// The NATURAL KEY of a repository on a code host — the triple that identifies it,
/// independently of any ``RepositoryId`` the daemon has minted for it.
///
/// It is one value rather than three sibling fields because its parts are meaningful
/// only together (an `owner` with no `host` names nothing), and because it is exactly
/// what a client that has never seen a repository id can supply.
public struct RepositoryKey: Codable, Equatable, Sendable {
  public let host: RepositoryHost
  public let owner: String
  public let name: String

  public init(host: RepositoryHost, owner: String, name: String) {
    self.host = host
    self.owner = owner
    self.name = name
  }
}

/// The plan the `createWorkstreamFromPlan` command turns into a new workstream.
///
/// ``repository`` is the NATURAL KEY, not a ``RepositoryId``, and that is forced by
/// who composes this: a client that has never seen a repository id and cannot mint
/// one. The daemon RESOLVES the key through its code-host port to an existing
/// ``Repository`` or creates one from the observation; a plan naming a repository the
/// host does not know is refused with ``ContractError/planRejected(reason:)`` and
/// writes nothing — it never silently creates an unobserved record.
public struct WorkstreamPlan: Codable, Equatable, Sendable {
  public let name: String
  public let repository: RepositoryKey
  public let spec: String

  public init(name: String, repository: RepositoryKey, spec: String) {
    self.name = name
    self.repository = repository
    self.spec = spec
  }
}

/// A client's RESUME CONTEXT: the durable cursor a feed request wants to continue
/// STRICTLY AFTER, together with the ``StoreGenerationId`` that cursor is a coordinate
/// in. Mirror of `@sprinter/contract`'s `ResumeContext`.
///
/// It is the optional half of BOTH feed payloads (``EventsPayload``,
/// ``ExecutionEventsPayload``): absent means "replay from the ORIGIN", present means
/// "resume", and there is no third state.
///
/// **Why one value and not two optional fields.** A durable offset is meaningless
/// outside the generation it was minted in — the daemon's store never migrates, so a
/// schema bump drops it and restarts offsets at `1`, and once the new log outgrows a
/// stale mark the numbers alone look perfectly resumable. Two INDEPENDENT optionals
/// would make "a cursor with no generation" representable, leaving the daemon to reject
/// it at runtime — and a runtime rejection is only as good as the branch it sits on.
/// That is exactly how the guard was bypassable: an offset of `0` reads as "the origin"
/// numerically, so the generation comparison was skipped for it, and a dead generation
/// paired with `sinceOffset: 0` was accepted as a first connect. This client can reach
/// that shape — ``ResumePoint`` records ``ContiguousOffsetTracker/contiguous``, which
/// stays `0` for an attempt whose first applied delta arrived out of order.
///
/// Pairing them removes the question. The PRESENCE of this value — never the value of
/// an offset — is what marks a request as a resume, so the daemon compares the
/// generation unconditionally, `sinceOffset: 0` included.
public struct ResumeContext: Codable, Equatable, Sendable {
  /// The durable offset to resume STRICTLY AFTER. `0` is legal and means "everything in
  /// THIS generation"; it is not an exemption from the generation check.
  public let sinceOffset: Int
  /// The ``Snapshot/generation`` the cursor was minted under, retained alongside the
  /// state the client is folding onto.
  public let generation: StoreGenerationId

  public init(sinceOffset: Int, generation: StoreGenerationId) {
    self.sinceOffset = sinceOffset
    self.generation = generation
  }
}

/// Payload of `events` (streams the ``OffsetEvent`` envelope) — the OPTIONAL
/// ``ResumeContext`` (CE2.0). A request with no `resume` (`nil`) replays from the log
/// ORIGIN; present resumes STRICTLY AFTER `resume.sinceOffset`, and the daemon refuses
/// it with ``ContractError/resyncRequired(sinceOffset:maxOffset:generation:)`` unless
/// its generation is the daemon's current one.
///
/// The wire field is `Schema.optionalKey`, so the KEY is OMITTED when `nil` (never
/// `null`) — Swift synthesized `Codable` matches this exactly. The payload OBJECT itself
/// is still sent PRESENT (an empty `{}` on a first connect, which has no resume
/// context): ``RpcBackend/events()`` encodes an empty ``EventsPayload`` so the request
/// carries `"payload": {}`, matching the canonical Effect client — the payload
/// schema is a `Struct`, so an omitted `payload` key would fail to decode.
public struct EventsPayload: Codable, Equatable, Sendable {
  public let resume: ResumeContext?
  public init(resume: ResumeContext? = nil) {
    self.resume = resume
  }
}

/// Payload of `createWorkstreamFromPlan` (success: ``WorkstreamId``; error:
/// ``ContractError/planRejected(reason:)``).
public struct CreateWorkstreamFromPlanPayload: Codable, Equatable, Sendable {
  public let plan: WorkstreamPlan
  public init(plan: WorkstreamPlan) { self.plan = plan }
}

/// Payload of `control` (error: ``ContractError/workstreamNotFound(id:)``).
public struct ControlPayload: Codable, Equatable, Sendable {
  public let workstreamId: WorkstreamId
  public let action: ControlAction

  public init(workstreamId: WorkstreamId, action: ControlAction) {
    self.workstreamId = workstreamId
    self.action = action
  }
}

/// Payload of `retryIssue` (error: ``ContractError/issueNotFound(id:)``).
public struct RetryIssuePayload: Codable, Equatable, Sendable {
  public let issueId: IssueId
  public init(issueId: IssueId) { self.issueId = issueId }
}

/// Payload of `executionEvents` (streams the ``OffsetExecutionEvent`` envelope) — the execution
/// id plus the OPTIONAL ``ResumeContext``. A request with no `resume` (`nil`) replays the
/// execution's durable transcript from the ORIGIN; present resumes STRICTLY AFTER that
/// durable per-execution offset.
///
/// The per-execution transcript log is dropped and restarted by a schema-version bump
/// exactly as the work-graph log is, so its cursor is generation-scoped in exactly the
/// same way and carries the SAME ``ResumeContext`` as ``EventsPayload`` — the guard is
/// not weaker for the execution channel, and it is not weaker STRUCTURALLY: a cursor here
/// can no more travel without its generation than one on `events` can. The optional wire
/// key is OMITTED when `nil` (never `null`), which Swift synthesized `Codable` matches
/// exactly.
public struct ExecutionEventsPayload: Codable, Equatable, Sendable {
  public let executionId: ExecutionId
  public let resume: ResumeContext?
  public init(executionId: ExecutionId, resume: ResumeContext? = nil) {
    self.executionId = executionId
    self.resume = resume
  }
}

/// Payload of `executionSend`.
public struct ExecutionSendPayload: Codable, Equatable, Sendable {
  public let executionId: ExecutionId
  public let input: ExecutionInput

  public init(executionId: ExecutionId, input: ExecutionInput) {
    self.executionId = executionId
    self.input = input
  }
}

/// Payload of `interrupt`.
public struct InterruptPayload: Codable, Equatable, Sendable {
  public let executionId: ExecutionId
  public init(executionId: ExecutionId) { self.executionId = executionId }
}

/// Payload of `answerUiRequest`.
public struct AnswerUiRequestPayload: Codable, Equatable, Sendable {
  public let executionId: ExecutionId
  public let response: UiResponse

  public init(executionId: ExecutionId, response: UiResponse) {
    self.executionId = executionId
    self.response = response
  }
}

/// The contract's owned, neutral error channel — mirror of the five
/// `Schema.TaggedErrorClass` errors. Each carries the same `_tag` discriminant the
/// TS contract emits, so the mirror decodes the RPC error payloads directly.
public enum ContractError: Codable, Equatable, Sendable, Error {
  case workstreamNotFound(id: WorkstreamId)
  case issueNotFound(id: IssueId)
  case executionNotFound(id: ExecutionId)
  case planRejected(reason: String)
  /// A resume cursor (`events`' or `executionEvents`') does NOT belong to the daemon's
  /// CURRENT store generation, so there is no incremental resume from it.
  ///
  /// The daemon's store never migrates: a schema-version bump DROPS the database and
  /// recreates it, restarting the durable log offsets at `1` and destroying every row
  /// the client's retained state was built from. The app and the daemon run together
  /// locally, so this lands on a LIVE client — one holding a cursor from the previous
  /// generation and a snapshot full of entities that no longer exist. The delta stream
  /// cannot repair that: deltas are upsert-only (there is no `*Removed`), so nothing
  /// streamed can ever REMOVE a stale entity.
  ///
  /// The daemon detects it by IDENTITY, not by arithmetic: a cursor beyond the log's
  /// extent is a symptom, but once a new generation's log outgrows a stale cursor the
  /// numbers alone look perfectly resumable. So every cursor-bearing request carries the
  /// ``StoreGenerationId`` it was minted under (read off ``Snapshot/generation``) as one
  /// inseparable ``ResumeContext``, and a resume whose generation is stale is refused —
  /// whatever the offsets say, `sinceOffset: 0` included.
  ///
  /// The only correct response is to throw the retained state away and re-hydrate:
  /// ``WorkGraphResync`` clears its retained snapshot AND its resume cursor and falls
  /// back to the subscribe-around-`snapshot` path it uses on a first connect.
  ///
  /// - `sinceOffset`: the cursor the client sent, echoed back.
  /// - `maxOffset`: the extent of the daemon's log (`0` when empty). NOT necessarily
  ///   exceeded — a generation refusal can carry a cursor well within it.
  /// - `generation`: the daemon's CURRENT generation, so the failure is diagnosable
  ///   rather than merely a refusal.
  case resyncRequired(sinceOffset: Int, maxOffset: Int, generation: StoreGenerationId)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case id
    case reason
    case sinceOffset
    case maxOffset
    case generation
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "WorkstreamNotFound":
      self = .workstreamNotFound(id: try container.decode(WorkstreamId.self, forKey: .id))
    case "IssueNotFound":
      self = .issueNotFound(id: try container.decode(IssueId.self, forKey: .id))
    case "ExecutionNotFound":
      self = .executionNotFound(id: try container.decode(ExecutionId.self, forKey: .id))
    case "PlanRejected":
      self = .planRejected(reason: try container.decode(String.self, forKey: .reason))
    case "ResyncRequired":
      self = .resyncRequired(
        sinceOffset: try container.decode(Int.self, forKey: .sinceOffset),
        maxOffset: try container.decode(Int.self, forKey: .maxOffset),
        generation: try container.decode(StoreGenerationId.self, forKey: .generation))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown ContractError tag: \(tag)"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .workstreamNotFound(let id):
      try container.encode("WorkstreamNotFound", forKey: .tag)
      try container.encode(id, forKey: .id)
    case .issueNotFound(let id):
      try container.encode("IssueNotFound", forKey: .tag)
      try container.encode(id, forKey: .id)
    case .executionNotFound(let id):
      try container.encode("ExecutionNotFound", forKey: .tag)
      try container.encode(id, forKey: .id)
    case .planRejected(let reason):
      try container.encode("PlanRejected", forKey: .tag)
      try container.encode(reason, forKey: .reason)
    case .resyncRequired(let sinceOffset, let maxOffset, let generation):
      try container.encode("ResyncRequired", forKey: .tag)
      try container.encode(sinceOffset, forKey: .sinceOffset)
      try container.encode(maxOffset, forKey: .maxOffset)
      try container.encode(generation, forKey: .generation)
    }
  }
}
