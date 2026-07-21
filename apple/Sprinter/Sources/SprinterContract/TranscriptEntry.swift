/// A durable, transcript-grade record — mirror of the contract's `TranscriptEntry`
/// tagged union, carried by the `EntryAppended` ``ExecutionEvent`` so a client
/// reconciles live deltas into the persisted record (INV-REACTIVE).
///
/// Tool `input`/`output` are arbitrary JSON (``JSONValue``); `reasoning` is an
/// optional-key field, so it is a Swift optional decoded via `decodeIfPresent`.
public enum TranscriptEntry: Codable, Equatable, Sendable {
  case userMessage(id: String, text: String)
  case assistantMessage(id: String, text: String, reasoning: String?)
  case toolCall(id: String, name: String, input: JSONValue)
  case toolResult(id: String, output: JSONValue, isError: Bool)
  case noticeEntry(id: String, level: NoticeLevel, message: String)

  private enum CodingKeys: String, CodingKey {
    case tag = "_tag"
    case id
    case text
    case reasoning
    case name
    case input
    case output
    case isError
    case level
    case message
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .tag)
    switch tag {
    case "UserMessage":
      self = .userMessage(
        id: try container.decode(String.self, forKey: .id),
        text: try container.decode(String.self, forKey: .text)
      )
    case "AssistantMessage":
      self = .assistantMessage(
        id: try container.decode(String.self, forKey: .id),
        text: try container.decode(String.self, forKey: .text),
        reasoning: try container.decodeIfPresent(String.self, forKey: .reasoning)
      )
    case "ToolCall":
      self = .toolCall(
        id: try container.decode(String.self, forKey: .id),
        name: try container.decode(String.self, forKey: .name),
        input: try container.decode(JSONValue.self, forKey: .input)
      )
    case "ToolResult":
      self = .toolResult(
        id: try container.decode(String.self, forKey: .id),
        output: try container.decode(JSONValue.self, forKey: .output),
        isError: try container.decode(Bool.self, forKey: .isError)
      )
    case "NoticeEntry":
      self = .noticeEntry(
        id: try container.decode(String.self, forKey: .id),
        level: try container.decode(NoticeLevel.self, forKey: .level),
        message: try container.decode(String.self, forKey: .message)
      )
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .tag,
        in: container,
        debugDescription: "Unknown TranscriptEntry tag: \(tag)"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .userMessage(let id, let text):
      try container.encode("UserMessage", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(text, forKey: .text)
    case .assistantMessage(let id, let text, let reasoning):
      try container.encode("AssistantMessage", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(text, forKey: .text)
      try container.encodeIfPresent(reasoning, forKey: .reasoning)
    case .toolCall(let id, let name, let input):
      try container.encode("ToolCall", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(name, forKey: .name)
      try container.encode(input, forKey: .input)
    case .toolResult(let id, let output, let isError):
      try container.encode("ToolResult", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(output, forKey: .output)
      try container.encode(isError, forKey: .isError)
    case .noticeEntry(let id, let level, let message):
      try container.encode("NoticeEntry", forKey: .tag)
      try container.encode(id, forKey: .id)
      try container.encode(level, forKey: .level)
      try container.encode(message, forKey: .message)
    }
  }
}
