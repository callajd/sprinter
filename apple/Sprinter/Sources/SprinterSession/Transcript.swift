import SprinterContract

/// A message as the session view renders it (BE3.1): a user prompt or an
/// assistant turn, coalesced from the live `MessageStarted`/`MessageDelta`/
/// `MessageCompleted` stream and reconciled against the durable
/// `EntryAppended` record (D17).
///
/// `text`/`reasoning` accrete from streamed deltas while the turn is live; a
/// durable `AssistantMessage`/`UserMessage` entry, when it arrives, is the
/// canonical record and replaces the accreted value.
public struct TranscriptMessage: Identifiable, Equatable, Sendable {
  /// Whether the message is the user's prompt or the assistant's reply.
  public enum Role: Equatable, Sendable {
    case user
    case assistant
  }

  /// The wire message id — stable across the deltas and the durable entry that
  /// share it, so they reconcile into one message.
  public let id: String
  public let role: Role
  /// The visible message text, accreted from deltas or set from the durable entry.
  public var text: String
  /// The assistant's reasoning trace, when present (optional-key field).
  public var reasoning: String?
  /// Whether the turn has completed (a `MessageCompleted` or a durable entry).
  public var isComplete: Bool

  public init(id: String, role: Role, text: String, reasoning: String?, isComplete: Bool) {
    self.id = id
    self.role = role
    self.text = text
    self.reasoning = reasoning
    self.isComplete = isComplete
  }
}

/// A tool invocation as the session view renders it: the call (name + input)
/// paired with its result (output + error flag), coalesced from the
/// `ToolStarted`/`ToolCompleted` stream and reconciled against the durable
/// `ToolCall`/`ToolResult` entries by shared id.
public struct TranscriptToolCall: Identifiable, Equatable, Sendable {
  public let id: String
  /// The tool name (from `ToolStarted`/`ToolCall`; empty until it is known when a
  /// result arrives out of order).
  public var name: String
  /// The tool input payload (opaque JSON).
  public var input: JSONValue
  /// The tool output payload, once the call completes; `nil` while in-flight.
  public var output: JSONValue?
  /// Whether the completed call reported an error.
  public var isError: Bool
  /// Whether the call has produced a result (a `ToolCompleted` or `ToolResult`).
  public var isComplete: Bool

  public init(
    id: String,
    name: String,
    input: JSONValue,
    output: JSONValue?,
    isError: Bool,
    isComplete: Bool
  ) {
    self.id = id
    self.name = name
    self.input = input
    self.output = output
    self.isError = isError
    self.isComplete = isComplete
  }
}

/// A notice surfaced in the transcript — a `Notice` event or a durable
/// `NoticeEntry`. Its ``id`` is the wire reconciliation key (`NoticeId`, CE5.2): a
/// live `Notice` and the durable `NoticeEntry` of the SAME logical event share it,
/// so the two reconcile onto one item instead of double-rendering; distinct notices
/// carry distinct keys and stay distinct.
public struct TranscriptNotice: Identifiable, Equatable, Sendable {
  public let id: String
  public let level: NoticeLevel
  public let message: String

  public init(id: String, level: NoticeLevel, message: String) {
    self.id = id
    self.level = level
    self.message = message
  }
}

/// The current value of a named status line (a `StatusChanged` event). A status
/// is a current-value signal (e.g. "Thinking…"), so successive changes to the
/// same `key` update in place rather than accumulating.
public struct TranscriptStatus: Identifiable, Equatable, Sendable {
  public var id: String { key }
  public let key: String
  public let text: String

  public init(key: String, text: String) {
    self.key = key
    self.text = text
  }
}

/// A scheduled retry surfaced in the transcript (a `RetryScheduled` resilience
/// signal). Point-in-time, keyed by arrival sequence.
public struct TranscriptRetry: Identifiable, Equatable, Sendable {
  public let id: String
  public let attempt: Int
  public let delayMs: Int
  public let error: String

  public init(id: String, attempt: Int, delayMs: Int, error: String) {
    self.id = id
    self.attempt = attempt
    self.delayMs = delayMs
    self.error = error
  }
}

/// A context-compaction marker in the transcript (a `ContextCompacted` signal).
/// Point-in-time, keyed by arrival sequence.
public struct TranscriptMarker: Identifiable, Equatable, Sendable {
  public let id: String

  public init(id: String) {
    self.id = id
  }
}

/// One rendered item in the session transcript. Its ``id`` is the reconciliation
/// key — messages coalesce by wire message id, tool calls by tool id, statuses by
/// status key; point-in-time signals (notice/retry/compaction) carry a unique
/// arrival-sequence id so they never merge.
public enum TranscriptItem: Identifiable, Equatable, Sendable {
  case message(TranscriptMessage)
  case toolCall(TranscriptToolCall)
  case notice(TranscriptNotice)
  case status(TranscriptStatus)
  case retry(TranscriptRetry)
  case compaction(TranscriptMarker)

  public var id: String {
    switch self {
    case .message(let message): return "message:\(message.id)"
    case .toolCall(let call): return "tool:\(call.id)"
    case .notice(let notice): return "notice:\(notice.id)"
    case .status(let status): return "status:\(status.key)"
    case .retry(let retry): return "retry:\(retry.id)"
    case .compaction(let marker): return "compaction:\(marker.id)"
    }
  }
}

/// The view-facing projection of a session's live feed: the ordered transcript
/// items plus the turn-lifecycle state a view chrome reads (is a turn running,
/// and the last reported token usage).
public struct Transcript: Equatable, Sendable {
  /// The transcript items in first-appearance order.
  public let items: [TranscriptItem]
  /// Whether a turn is currently running (between `TurnStarted` and its
  /// `TurnCompleted`/`SessionIdle`).
  public let isTurnActive: Bool
  /// Token accounting from the most recent completed turn, when reported.
  public let lastUsage: Usage?

  public init(items: [TranscriptItem], isTurnActive: Bool, lastUsage: Usage?) {
    self.items = items
    self.isTurnActive = isTurnActive
    self.lastUsage = lastUsage
  }

  /// The empty transcript — a session with no events yet.
  public static let empty = Transcript(items: [], isTurnActive: false, lastUsage: nil)
}

/// The terminal-distinguishable lifecycle of a session view (D9 / the carried BE2
/// single-consumer constraint): whether the feed has started, is live, ended
/// cleanly, or dropped abnormally.
public enum SessionLifecycle: Equatable, Sendable {
  /// The feed has never been started.
  case idle
  /// The live feed is currently subscribed.
  case live
  /// The feed finished cleanly (the session ended) or was intentionally stopped.
  case ended
  /// The feed terminated abnormally — a transport drop or a failure `Exit`
  /// (`InteractiveSession.terminationError` carries the cause).
  case dropped
}
