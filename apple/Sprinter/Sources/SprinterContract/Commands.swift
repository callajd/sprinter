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
/// `sinceOffset` resume cursor (contract v3 / CE2.0). Absent (`nil`) replays from the log ORIGIN
/// (backward-compatible); present resumes STRICTLY AFTER that offset. The wire is
/// `Schema.optionalKey(NonNegativeInt)`, so the key is OMITTED when `nil` (never
/// `null`) — Swift synthesized `Codable` matches this exactly.
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

/// Payload of `sessionEvents` (streams ``SessionEvent``).
public struct SessionEventsPayload: Codable, Equatable, Sendable {
  public let sessionId: SessionId
  public init(sessionId: SessionId) { self.sessionId = sessionId }
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

/// The contract's owned, neutral error channel — mirror of the four
/// `Schema.TaggedErrorClass` errors. Each carries the same `_tag` discriminant the
/// TS contract emits, so the mirror decodes the RPC error payloads directly.
public enum ContractError: Codable, Equatable, Sendable, Error {
  case workstreamNotFound(id: WorkstreamId)
  case issueNotFound(id: IssueId)
  case sessionNotFound(id: SessionId)
  case planRejected(reason: String)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case id
    case reason
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
    }
  }
}

/// The contract version this mirror tracks (INV-CONTRACT).
///
/// The TS contract carries `CONTRACT_VERSION` as a compile-time group annotation
/// (not an in-band wire field); the Swift mirror tracks it as its own constant.
/// A contract bump ripples here and to the goldens + decode tests — see the
/// regeneration procedure in `docs/contract-mirror.md`.
public enum SprinterContract {
  /// The mirrored contract version (`v3`).
  ///
  /// `v2` (CE5) batched the distinct terminal `cancelled` ``WorkStatus`` (CE5.1)
  /// and the reconciliation-key `id` on ``SessionEvent``.`notice` /
  /// ``TranscriptEntry``.`noticeEntry` (CE5.2). `v3` (CE2.0) makes the `events`
  /// cursor usable end-to-end as ONE change: the OPTIONAL `sinceOffset` resume
  /// cursor on ``EventsPayload`` (request) AND the ``OffsetEvent`` envelope on the
  /// streamed response, so each item carries the durable offset the client feeds
  /// back as that cursor — rippled here and to the goldens.
  public static let version = 3
}
