/// The neutral, maximally reactive session-event stream type — mirror of the
/// contract's `SessionEvent` tagged union (D17 / INV-REACTIVE). It carries turn
/// lifecycle, fine-grained message/reasoning and tool deltas, UI requests,
/// resilience signals, and the durable `EntryAppended` record.
///
/// Decoding and encoding are split into per-category helpers so each stays small
/// and auditable (the union is wide); an unknown `_tag` is a decode failure.
/// Optional-key fields (`usage`, `text`, `reasoning`, `options`, and a `Notice`'s
/// reconciliation-key `id`) round-trip as Swift optionals via `decodeIfPresent` /
/// `encodeIfPresent`.
public enum SessionEvent: Codable, Equatable, Sendable {
  case turnStarted
  case turnCompleted(usage: Usage?)
  case messageStarted(messageId: String)
  case messageDelta(messageId: String, text: String?, reasoning: String?)
  case messageCompleted(messageId: String)
  case toolStarted(id: String, name: String, input: JSONValue)
  case toolProgress(id: String, partial: JSONValue)
  case toolCompleted(id: String, output: JSONValue, isError: Bool)
  case sessionIdle
  case retryScheduled(attempt: Int, delayMs: Int, error: String)
  case contextCompacted
  case uiRequestRaised(id: String, kind: UiRequestKind, prompt: String, options: [String]?)
  case notice(id: String?, level: NoticeLevel, message: String)
  case statusChanged(key: String, text: String)
  case entryAppended(entry: TranscriptEntry)

  fileprivate enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case usage
    case messageId
    case text
    case reasoning
    case id
    case name
    case input
    case partial
    case output
    case isError
    case attempt
    case delayMs
    case error
    case kind
    case prompt
    case options
    case level
    case message
    case key
    case entry
  }
}

// ── Decoding ─────────────────────────────────────────────────────────────────

extension SessionEvent {
  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    guard
      let event =
        try Self.decodeLifecycle(tag: tag, container: container)
        ?? Self.decodeMessage(tag: tag, container: container)
        ?? Self.decodeTool(tag: tag, container: container)
        ?? Self.decodeSignal(tag: tag, container: container)
    else {
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown SessionEvent tag: \(tag)"
      )
    }
    self = event
  }

  private static func decodeLifecycle(
    tag: String,
    container: KeyedDecodingContainer<CodingKeys>
  ) throws -> SessionEvent? {
    switch tag {
    case "TurnStarted": return .turnStarted
    case "TurnCompleted":
      return .turnCompleted(
        usage: try container.decodeIfPresent(Usage.self, forKey: .usage))
    case "SessionIdle": return .sessionIdle
    case "ContextCompacted": return .contextCompacted
    default: return nil
    }
  }

  private static func decodeMessage(
    tag: String,
    container: KeyedDecodingContainer<CodingKeys>
  ) throws -> SessionEvent? {
    switch tag {
    case "MessageStarted":
      return .messageStarted(
        messageId: try container.decode(String.self, forKey: .messageId))
    case "MessageDelta":
      return .messageDelta(
        messageId: try container.decode(String.self, forKey: .messageId),
        text: try container.decodeIfPresent(String.self, forKey: .text),
        reasoning: try container.decodeIfPresent(String.self, forKey: .reasoning))
    case "MessageCompleted":
      return .messageCompleted(
        messageId: try container.decode(String.self, forKey: .messageId))
    default: return nil
    }
  }

  private static func decodeTool(
    tag: String,
    container: KeyedDecodingContainer<CodingKeys>
  ) throws -> SessionEvent? {
    switch tag {
    case "ToolStarted":
      return .toolStarted(
        id: try container.decode(String.self, forKey: .id),
        name: try container.decode(String.self, forKey: .name),
        input: try container.decode(JSONValue.self, forKey: .input))
    case "ToolProgress":
      return .toolProgress(
        id: try container.decode(String.self, forKey: .id),
        partial: try container.decode(JSONValue.self, forKey: .partial))
    case "ToolCompleted":
      return .toolCompleted(
        id: try container.decode(String.self, forKey: .id),
        output: try container.decode(JSONValue.self, forKey: .output),
        isError: try container.decode(Bool.self, forKey: .isError))
    default: return nil
    }
  }

  private static func decodeSignal(
    tag: String,
    container: KeyedDecodingContainer<CodingKeys>
  ) throws -> SessionEvent? {
    switch tag {
    case "RetryScheduled":
      return .retryScheduled(
        attempt: try container.decode(Int.self, forKey: .attempt),
        delayMs: try container.decode(Int.self, forKey: .delayMs),
        error: try container.decode(String.self, forKey: .error))
    case "UiRequestRaised":
      return .uiRequestRaised(
        id: try container.decode(String.self, forKey: .id),
        kind: try container.decode(UiRequestKind.self, forKey: .kind),
        prompt: try container.decode(String.self, forKey: .prompt),
        options: try container.decodeIfPresent([String].self, forKey: .options))
    case "Notice":
      // `id` is the OPTIONAL NoticeId reconciliation key — absent for content-derived
      // notices with no stable cross-emission identity (mirrors `Schema.optionalKey`).
      return .notice(
        id: try container.decodeIfPresent(String.self, forKey: .id),
        level: try container.decode(NoticeLevel.self, forKey: .level),
        message: try container.decode(String.self, forKey: .message))
    case "StatusChanged":
      return .statusChanged(
        key: try container.decode(String.self, forKey: .key),
        text: try container.decode(String.self, forKey: .text))
    case "EntryAppended":
      return .entryAppended(
        entry: try container.decode(TranscriptEntry.self, forKey: .entry))
    default: return nil
    }
  }
}

// ── Encoding ─────────────────────────────────────────────────────────────────

extension SessionEvent {
  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .turnStarted, .turnCompleted, .sessionIdle, .contextCompacted:
      try encodeLifecycle(into: &container)
    case .messageStarted, .messageDelta, .messageCompleted:
      try encodeMessage(into: &container)
    case .toolStarted, .toolProgress, .toolCompleted:
      try encodeTool(into: &container)
    case .retryScheduled, .uiRequestRaised, .notice, .statusChanged, .entryAppended:
      try encodeSignal(into: &container)
    }
  }

  private func encodeLifecycle(into container: inout KeyedEncodingContainer<CodingKeys>) throws {
    switch self {
    case .turnStarted: try container.encode("TurnStarted", forKey: .tag)
    case .turnCompleted(let usage):
      try container.encode("TurnCompleted", forKey: .tag)
      try container.encodeIfPresent(usage, forKey: .usage)
    case .sessionIdle: try container.encode("SessionIdle", forKey: .tag)
    case .contextCompacted: try container.encode("ContextCompacted", forKey: .tag)
    default: break
    }
  }

  private func encodeMessage(into container: inout KeyedEncodingContainer<CodingKeys>) throws {
    switch self {
    case .messageStarted(let messageId):
      try container.encode("MessageStarted", forKey: .tag)
      try container.encode(messageId, forKey: .messageId)
    case .messageDelta(let messageId, let text, let reasoning):
      try container.encode("MessageDelta", forKey: .tag)
      try container.encode(messageId, forKey: .messageId)
      try container.encodeIfPresent(text, forKey: .text)
      try container.encodeIfPresent(reasoning, forKey: .reasoning)
    case .messageCompleted(let messageId):
      try container.encode("MessageCompleted", forKey: .tag)
      try container.encode(messageId, forKey: .messageId)
    default: break
    }
  }

  private func encodeTool(into container: inout KeyedEncodingContainer<CodingKeys>) throws {
    switch self {
    case .toolStarted(let id, let name, let input):
      try container.encode("ToolStarted", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(name, forKey: .name)
      try container.encode(input, forKey: .input)
    case .toolProgress(let id, let partial):
      try container.encode("ToolProgress", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(partial, forKey: .partial)
    case .toolCompleted(let id, let output, let isError):
      try container.encode("ToolCompleted", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(output, forKey: .output)
      try container.encode(isError, forKey: .isError)
    default: break
    }
  }

  private func encodeSignal(into container: inout KeyedEncodingContainer<CodingKeys>) throws {
    switch self {
    case .retryScheduled(let attempt, let delayMs, let error):
      try container.encode("RetryScheduled", forKey: .tag)
      try container.encode(attempt, forKey: .attempt)
      try container.encode(delayMs, forKey: .delayMs)
      try container.encode(error, forKey: .error)
    case .uiRequestRaised(let id, let kind, let prompt, let options):
      try container.encode("UiRequestRaised", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(kind, forKey: .kind)
      try container.encode(prompt, forKey: .prompt)
      try container.encodeIfPresent(options, forKey: .options)
    case .notice(let id, let level, let message):
      try container.encode("Notice", forKey: .tag)
      try container.encodeIfPresent(id, forKey: .id)
      try container.encode(level, forKey: .level)
      try container.encode(message, forKey: .message)
    case .statusChanged(let key, let text):
      try container.encode("StatusChanged", forKey: .tag)
      try container.encode(key, forKey: .key)
      try container.encode(text, forKey: .text)
    case .entryAppended(let entry):
      try container.encode("EntryAppended", forKey: .tag)
      try container.encode(entry, forKey: .entry)
    default: break
    }
  }
}
