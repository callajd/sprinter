/// The neutral session I/O types — ``Usage``, ``SessionInput``, ``UiAnswer`` and
/// ``UiResponse`` — plus the session enums, mirrored from `@sprinter/domain`'s
/// session model. Each `String`-raw enum's raw value is the exact wire token.

/// The kinds of UI request an agent can raise mid-session.
public enum UiRequestKind: String, Codable, CaseIterable, Sendable {
  case select
  case confirm
  case input
  case editor
}

/// Severity of a ``SessionEvent`` notice / ``TranscriptEntry`` notice entry.
public enum NoticeLevel: String, Codable, CaseIterable, Sendable {
  case info
  case warn
  case error
}

/// How a ``SessionInput`` is driven into a session.
public enum SessionInputMode: String, Codable, CaseIterable, Sendable {
  case prompt
  case steer
  case followUp
}

/// Token accounting reported when a turn completes. `cacheReadTokens` /
/// `cacheWriteTokens` are optional-key fields.
public struct Usage: Codable, Equatable, Sendable {
  public let inputTokens: Int
  public let outputTokens: Int
  public let cacheReadTokens: Int?
  public let cacheWriteTokens: Int?

  public init(
    inputTokens: Int,
    outputTokens: Int,
    cacheReadTokens: Int?,
    cacheWriteTokens: Int?
  ) {
    self.inputTokens = inputTokens
    self.outputTokens = outputTokens
    self.cacheReadTokens = cacheReadTokens
    self.cacheWriteTokens = cacheWriteTokens
  }
}

/// Input driven INTO a session. `images` is an optional-key field; `mode`
/// distinguishes a fresh prompt, a mid-turn steer, or a follow-up.
public struct SessionInput: Codable, Equatable, Sendable {
  public let text: String
  public let images: [String]?
  public let mode: SessionInputMode

  public init(text: String, images: [String]?, mode: SessionInputMode) {
    self.text = text
    self.images = images
    self.mode = mode
  }
}

/// The answer to a raised UI request — mirror of the contract's `UiAnswer`
/// tagged union.
public enum UiAnswer: Codable, Equatable, Sendable {
  case value(value: String)
  case confirmed(confirmed: Bool)
  case cancelled

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case value
    case confirmed
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "Value":
      self = .value(value: try container.decode(String.self, forKey: .value))
    case "Confirmed":
      self = .confirmed(confirmed: try container.decode(Bool.self, forKey: .confirmed))
    case "Cancelled":
      self = .cancelled
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown UiAnswer tag: \(tag)"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .value(let value):
      try container.encode("Value", forKey: .tag)
      try container.encode(value, forKey: .value)
    case .confirmed(let confirmed):
      try container.encode("Confirmed", forKey: .tag)
      try container.encode(confirmed, forKey: .confirmed)
    case .cancelled:
      try container.encode("Cancelled", forKey: .tag)
    }
  }
}

/// A response to an outstanding UI request, keyed by the request it answers.
public struct UiResponse: Codable, Equatable, Sendable {
  public let requestId: String
  public let answer: UiAnswer

  public init(requestId: String, answer: UiAnswer) {
    self.requestId = requestId
    self.answer = answer
  }
}
