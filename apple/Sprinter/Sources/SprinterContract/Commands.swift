/// The command payloads the RPC procedures carry and the contract's owned error
/// types — mirrored from `@sprinter/contract`'s procedure surface.
///
/// Each `payload` struct mirrors one procedure's request shape; the streamed and
/// request/response success values are owned types (``Snapshot``, the
/// ``OffsetEvent`` envelope over ``WorkGraphEvent``, ``SessionEvent``, a bare
/// ``WorkstreamId``) already mirrored elsewhere in this module.

/// The lifecycle action a `control` command applies to a workstream.
public enum ControlAction: String, Codable, CaseIterable, Sendable {
  case start
  case pause
  case resume
  case cancel
}

/// The plan the `createWorkstreamFromPlan` command turns into a new workstream.
public struct WorkstreamPlan: Codable, Equatable, Sendable {
  public let name: String
  public let repo: String
  public let spec: String

  public init(name: String, repo: String, spec: String) {
    self.name = name
    self.repo = repo
    self.spec = spec
  }
}

/// Payload of `events` (streams the ``OffsetEvent`` envelope) — the OPTIONAL
/// `sinceOffset` resume cursor (CE2.0). A request with no `sinceOffset`
/// (`nil`) replays from the log ORIGIN; present resumes STRICTLY AFTER that offset.
/// The wire is `Schema.optionalKey(NonNegativeInt)`, so the KEY is OMITTED when `nil`
/// (never `null`) — Swift synthesized `Codable` matches this exactly. The payload
/// OBJECT itself is still sent PRESENT (an empty `{}` when there is no cursor):
/// ``RpcBackend/events()`` encodes an empty ``EventsPayload`` so the request carries
/// `"payload": {}`, matching the canonical Effect client — the payload
/// schema is a `Struct`, so an omitted `payload` key would fail to decode.
public struct EventsPayload: Codable, Equatable, Sendable {
  public let sinceOffset: Int?
  public init(sinceOffset: Int? = nil) { self.sinceOffset = sinceOffset }
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

/// Payload of `sessionEvents` (streams the ``OffsetSessionEvent`` envelope) — the session
/// id plus the OPTIONAL `sinceOffset` resume cursor. A request with no
/// `sinceOffset` (`nil`) replays the session's durable transcript from the ORIGIN; present
/// resumes STRICTLY AFTER that durable per-session offset. The wire is
/// `Schema.optionalKey(NonNegativeInt)`, so the KEY is OMITTED when `nil` (never `null`) —
/// Swift synthesized `Codable` matches this exactly (mirrors ``EventsPayload``).
public struct SessionEventsPayload: Codable, Equatable, Sendable {
  public let sessionId: SessionId
  public let sinceOffset: Int?
  public init(sessionId: SessionId, sinceOffset: Int? = nil) {
    self.sessionId = sessionId
    self.sinceOffset = sinceOffset
  }
}

/// Payload of `sessionSend`.
public struct SessionSendPayload: Codable, Equatable, Sendable {
  public let sessionId: SessionId
  public let input: SessionInput

  public init(sessionId: SessionId, input: SessionInput) {
    self.sessionId = sessionId
    self.input = input
  }
}

/// Payload of `interrupt`.
public struct InterruptPayload: Codable, Equatable, Sendable {
  public let sessionId: SessionId
  public init(sessionId: SessionId) { self.sessionId = sessionId }
}

/// Payload of `answerUiRequest`.
public struct AnswerUiRequestPayload: Codable, Equatable, Sendable {
  public let sessionId: SessionId
  public let response: UiResponse

  public init(sessionId: SessionId, response: UiResponse) {
    self.sessionId = sessionId
    self.response = response
  }
}

/// The contract's owned, neutral error channel — mirror of the five
/// `Schema.TaggedErrorClass` errors. Each carries the same `_tag` discriminant the
/// TS contract emits, so the mirror decodes the RPC error payloads directly.
public enum ContractError: Codable, Equatable, Sendable, Error {
  case workstreamNotFound(id: WorkstreamId)
  case issueNotFound(id: IssueId)
  case sessionNotFound(id: SessionId)
  case planRejected(reason: String)
  /// The `events` request's `sinceOffset` cursor cannot belong to the daemon's
  /// CURRENT store generation, so there is no incremental resume from it.
  ///
  /// The daemon's store never migrates: a schema-version bump DROPS the database and
  /// recreates it, restarting the durable event log's offsets at `1` and destroying
  /// every row the client's retained state was built from. The app and the daemon run
  /// together locally, so this lands on a LIVE client — one holding a cursor from the
  /// previous generation and a snapshot full of entities that no longer exist. The
  /// delta stream cannot repair that: deltas are upsert-only (there is no `*Removed`),
  /// so nothing streamed can ever REMOVE a stale entity.
  ///
  /// The only correct response is to throw the retained state away and re-hydrate:
  /// ``WorkGraphResync`` clears its retained snapshot AND its resume cursor and falls
  /// back to the subscribe-around-`snapshot` path it uses on a first connect.
  ///
  /// - `sinceOffset`: the cursor the client sent, echoed back.
  /// - `maxOffset`: the extent of the daemon's log (`0` when empty) that it exceeded.
  case resyncRequired(sinceOffset: Int, maxOffset: Int)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case id
    case reason
    case sinceOffset
    case maxOffset
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "WorkstreamNotFound":
      self = .workstreamNotFound(id: try container.decode(WorkstreamId.self, forKey: .id))
    case "IssueNotFound":
      self = .issueNotFound(id: try container.decode(IssueId.self, forKey: .id))
    case "SessionNotFound":
      self = .sessionNotFound(id: try container.decode(SessionId.self, forKey: .id))
    case "PlanRejected":
      self = .planRejected(reason: try container.decode(String.self, forKey: .reason))
    case "ResyncRequired":
      self = .resyncRequired(
        sinceOffset: try container.decode(Int.self, forKey: .sinceOffset),
        maxOffset: try container.decode(Int.self, forKey: .maxOffset))
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
    case .sessionNotFound(let id):
      try container.encode("SessionNotFound", forKey: .tag)
      try container.encode(id, forKey: .id)
    case .planRejected(let reason):
      try container.encode("PlanRejected", forKey: .tag)
      try container.encode(reason, forKey: .reason)
    case .resyncRequired(let sinceOffset, let maxOffset):
      try container.encode("ResyncRequired", forKey: .tag)
      try container.encode(sinceOffset, forKey: .sinceOffset)
      try container.encode(maxOffset, forKey: .maxOffset)
    }
  }
}
