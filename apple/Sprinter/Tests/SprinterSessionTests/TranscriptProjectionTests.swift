import SprinterContract
import Testing

@testable import SprinterSession

@Suite("Transcript projection")
struct TranscriptProjectionTests {
  /// An empty feed projects to the empty transcript.
  @Test("an empty feed projects to the empty transcript")
  func emptyFeed() {
    #expect(TranscriptProjection.project([]) == Transcript.empty)
  }

  /// Streamed message deltas coalesce into one assistant message: text and
  /// reasoning accrete across deltas and `MessageCompleted` marks it complete.
  @Test("streamed message deltas coalesce into one assistant message")
  func messageDeltasCoalesce() throws {
    let transcript = TranscriptProjection.project([
      .messageStarted(messageId: "m1"),
      .messageDelta(messageId: "m1", text: "Hel", reasoning: "think"),
      .messageDelta(messageId: "m1", text: "lo", reasoning: "ing"),
      .messageCompleted(messageId: "m1")
    ])

    #expect(transcript.items.count == 1)
    let message = try requireMessage(transcript.items.first)
    #expect(message.id == "m1")
    #expect(message.role == .assistant)
    #expect(message.text == "Hello")
    #expect(message.reasoning == "thinking")
    #expect(message.isComplete)
  }

  /// The durable `EntryAppended` assistant message is canonical: it reconciles onto
  /// the SAME item its live deltas built and replaces the accreted value (D17).
  @Test("a durable assistant entry reconciles onto and replaces its live deltas")
  func durableEntryReplacesDeltas() throws {
    let transcript = TranscriptProjection.project([
      .messageStarted(messageId: "m1"),
      .messageDelta(messageId: "m1", text: "Partial", reasoning: nil),
      .entryAppended(
        entry: .assistantMessage(id: "m1", text: "Final answer", reasoning: "final reasoning"))
    ])

    // Still one item — the durable entry merged onto the delta-built message.
    #expect(transcript.items.count == 1)
    let message = try requireMessage(transcript.items.first)
    #expect(message.text == "Final answer")
    #expect(message.reasoning == "final reasoning")
    #expect(message.isComplete)
  }

  /// A stale delta arriving AFTER the message was finalized by its durable entry is
  /// ignored, not appended onto the canonical text (defensive against out-of-order
  /// wire delivery, matching the tool path's `toolResultBeforeCall`).
  @Test("a delta after the message is finalized is ignored, not appended")
  func messageDeltaAfterFinalizedIsIgnored() throws {
    let transcript = TranscriptProjection.project([
      .entryAppended(entry: .assistantMessage(id: "m1", text: "Final answer", reasoning: "r")),
      .messageDelta(messageId: "m1", text: " CORRUPTION", reasoning: " noise")
    ])

    #expect(transcript.items.count == 1)
    let message = try requireMessage(transcript.items.first)
    #expect(message.text == "Final answer")
    #expect(message.reasoning == "r")
    #expect(message.isComplete)
  }

  /// A durable user message appears as a `.user` transcript message.
  @Test("a durable user-message entry projects to a user message")
  func durableUserMessage() throws {
    let transcript = TranscriptProjection.project([
      .entryAppended(entry: .userMessage(id: "u1", text: "do the thing"))
    ])
    let message = try requireMessage(transcript.items.first)
    #expect(message.role == .user)
    #expect(message.text == "do the thing")
    #expect(message.isComplete)
  }

  /// A tool call pairs its start (name + input) with its completion (output + error
  /// flag) into one item; an intermediate `ToolProgress` preview does not add one.
  @Test("a tool call pairs start and completion into one item")
  func toolCallPairs() throws {
    let input = JSONValue.object(["path": .string("/x")])
    let output = JSONValue.string("done")
    let transcript = TranscriptProjection.project([
      .toolStarted(id: "t1", name: "read", input: input),
      .toolProgress(id: "t1", partial: .string("half")),
      .toolCompleted(id: "t1", output: output, isError: false)
    ])

    #expect(transcript.items.count == 1)
    let call = try requireTool(transcript.items.first)
    #expect(call.id == "t1")
    #expect(call.name == "read")
    #expect(call.input == input)
    #expect(call.output == output)
    #expect(!call.isError)
    #expect(call.isComplete)
  }

  /// A durable `ToolResult` arriving before its `ToolCall` still reconciles onto one
  /// item — the later call fills in the name/input.
  @Test("an out-of-order durable tool result reconciles onto one tool item")
  func toolResultBeforeCall() throws {
    let transcript = TranscriptProjection.project([
      .entryAppended(entry: .toolResult(id: "t1", output: .string("out"), isError: true)),
      .entryAppended(entry: .toolCall(id: "t1", name: "write", input: .string("in")))
    ])

    #expect(transcript.items.count == 1)
    let call = try requireTool(transcript.items.first)
    #expect(call.name == "write")
    #expect(call.input == .string("in"))
    #expect(call.output == .string("out"))
    #expect(call.isError)
    #expect(call.isComplete)
  }

  /// Notices, statuses, retries and compaction all surface; a status collapses by
  /// key (latest wins) while notices/retries are distinct point-in-time items.
  @Test("notices, statuses, retries and compaction surface; status collapses by key")
  func signalsSurface() {
    let transcript = TranscriptProjection.project([
      .notice(id: "n1", level: .info, message: "first"),
      .statusChanged(key: "phase", text: "planning"),
      .retryScheduled(attempt: 1, delayMs: 500, error: "rate limit"),
      .statusChanged(key: "phase", text: "executing"),
      .notice(id: "n2", level: .warn, message: "second"),
      .contextCompacted
    ])

    let notices = transcript.items.compactMap { item -> TranscriptNotice? in
      if case .notice(let notice) = item { return notice }
      return nil
    }
    #expect(notices.map(\.message) == ["first", "second"])
    #expect(notices.map(\.level) == [.info, .warn])

    let statuses = transcript.items.compactMap { item -> TranscriptStatus? in
      if case .status(let status) = item { return status }
      return nil
    }
    // One status item (collapsed by key), holding the latest value.
    #expect(statuses.count == 1)
    #expect(statuses.first?.text == "executing")

    #expect(transcript.items.contains { if case .retry = $0 { return true } else { return false } })
    #expect(
      transcript.items.contains { if case .compaction = $0 { return true } else { return false } })

    // Every item id is unique (the point-in-time items get distinct sequence ids).
    #expect(Set(transcript.items.map(\.id)).count == transcript.items.count)
  }

  /// A durable `NoticeEntry` also surfaces as a notice item.
  @Test("a durable notice entry surfaces as a notice item")
  func durableNoticeEntry() {
    let transcript = TranscriptProjection.project([
      .entryAppended(entry: .noticeEntry(id: "n-boom", level: .error, message: "boom"))
    ])
    let notices = transcript.items.compactMap { item -> TranscriptNotice? in
      if case .notice(let notice) = item { return notice }
      return nil
    }
    #expect(notices.map(\.message) == ["boom"])
    #expect(notices.first?.level == .error)
  }

  /// CE5.2: a live `Notice` and the durable `NoticeEntry` of the SAME logical event
  /// share a reconciliation key (`NoticeId`), so they reconcile onto ONE item — the
  /// durable value is canonical — rather than double-rendering. Two notices with
  /// distinct keys stay distinct.
  @Test("a live Notice and its durable NoticeEntry reconcile by shared key to one item")
  func noticeReconciliationKey() {
    let transcript = TranscriptProjection.project([
      .notice(id: "retry-5", level: .warn, message: "retrying"),
      .notice(id: "other", level: .info, message: "unrelated"),
      .entryAppended(entry: .noticeEntry(id: "retry-5", level: .error, message: "gave up"))
    ])
    let notices = transcript.items.compactMap { item -> TranscriptNotice? in
      if case .notice(let notice) = item { return notice }
      return nil
    }
    // The shared-key live+durable pair collapsed to one item (durable value wins);
    // the distinct-key notice remains its own item.
    #expect(notices.count == 2)
    let reconciled = notices.first { $0.id == "retry-5" }
    #expect(reconciled?.level == .error)
    #expect(reconciled?.message == "gave up")
    #expect(notices.contains { $0.id == "other" })
  }

  /// Turn lifecycle drives the transcript chrome (not items): a running turn sets
  /// `isTurnActive`, and `TurnCompleted` reports usage and clears it.
  @Test("turn lifecycle drives isTurnActive and lastUsage, not items")
  func turnLifecycleChrome() {
    let usage = Usage(
      inputTokens: 10, outputTokens: 20, cacheReadTokens: nil, cacheWriteTokens: nil)

    let active = TranscriptProjection.project([.turnStarted])
    #expect(active.isTurnActive)
    #expect(active.items.isEmpty)

    let completed = TranscriptProjection.project([.turnStarted, .turnCompleted(usage: usage)])
    #expect(!completed.isTurnActive)
    #expect(completed.lastUsage == usage)

    let idle = TranscriptProjection.project([.turnStarted, .sessionIdle])
    #expect(!idle.isTurnActive)
  }

  /// A `UiRequestRaised` is surfaced inline (via outstanding requests), never as a
  /// transcript item.
  @Test("a UI request is not a transcript item")
  func uiRequestNotAnItem() {
    let transcript = TranscriptProjection.project([
      .uiRequestRaised(id: "r1", kind: .confirm, prompt: "ok?", options: nil)
    ])
    #expect(transcript.items.isEmpty)
  }

  /// First-appearance order is preserved across interleaved item kinds.
  @Test("first-appearance order is preserved across interleaved kinds")
  func orderingPreserved() {
    let transcript = TranscriptProjection.project([
      .messageStarted(messageId: "m1"),
      .toolStarted(id: "t1", name: "read", input: .null),
      .messageDelta(messageId: "m1", text: "hi", reasoning: nil),
      .toolCompleted(id: "t1", output: .null, isError: false)
    ])
    #expect(transcript.items.map(\.id) == ["message:m1", "tool:t1"])
  }

  private func requireMessage(_ item: TranscriptItem?) throws -> TranscriptMessage {
    guard case .message(let message)? = item else {
      Issue.record("expected a message item, got \(String(describing: item))")
      throw ProjectionTestError.wrongItem
    }
    return message
  }

  private func requireTool(_ item: TranscriptItem?) throws -> TranscriptToolCall {
    guard case .toolCall(let call)? = item else {
      Issue.record("expected a tool item, got \(String(describing: item))")
      throw ProjectionTestError.wrongItem
    }
    return call
  }

  private enum ProjectionTestError: Error {
    case wrongItem
  }
}
