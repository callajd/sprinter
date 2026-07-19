import SprinterContract

/// Projects a session's ``SessionEvent`` feed into the view-facing ``Transcript``
/// (BE3.1 / D17) — the pure, `Sendable`, offline-testable transform the session
/// view model renders.
///
/// It folds the maximally-reactive feed into an ordered transcript: fine-grained
/// `MessageDelta`/`ToolStarted`/`ToolCompleted` deltas accrete into messages and
/// tool calls, and the durable `EntryAppended` record reconciles onto the SAME
/// items by shared id — a durable entry is the canonical value and replaces the
/// accreted deltas (INV-REACTIVE: render live, reconcile into the transcript-grade
/// record). Turn-lifecycle events drive the transcript's `isTurnActive`/`lastUsage`
/// chrome rather than an item; `UiRequestRaised` is surfaced inline through
/// ``InteractiveSession/outstandingRequests``, not the transcript, so it is folded
/// here as a no-op.
///
/// Every projection is a fresh full transcript derived from the whole feed, so
/// wiring it to an `@Observable` feed makes the view update as events arrive with
/// no incremental bookkeeping of its own.
public enum TranscriptProjection {
  /// Rebuilds the transcript from a session's event feed.
  public static func project(_ events: [SessionEvent]) -> Transcript {
    var builder = Builder()
    for event in events {
      builder.ingest(event)
    }
    return builder.transcript()
  }

  /// An **incremental memo** over an append-only event feed (CE3.2): it continues the
  /// left-fold from the last projected prefix instead of re-folding the whole feed on
  /// every SwiftUI re-read, so N re-reads of an N-event feed cost O(N) rather than
  /// O(N²).
  ///
  /// The continued fold is byte-for-byte equal to a from-scratch ``project(_:)`` for
  /// ANY event sequence — including the out-of-order/duplicate deltas the CE2 design
  /// tolerates — because ``project(_:)`` is a deterministic left-fold and the session
  /// feed only ever GROWS by appending: folding `events[0..<k]` then continuing with
  /// `events[k...]` yields the identical ``Builder`` as folding `events[0..<N]`. A
  /// re-read at the same count returns the cached transcript; a feed that ever shrank
  /// (it does not, for the append-only feed) resets and re-folds, keeping the memo
  /// total. It carries mutable fold state, so it is a `class` a view model holds and
  /// drives on its own actor.
  public final class Memo {
    private var builder = Builder()
    private var foldedCount = 0
    private var cached: Transcript = .empty

    public init() {}

    /// The transcript for `events`, continuing the fold from the last call. `events`
    /// must be an append-only growth of the prior call's feed (the session feed's
    /// invariant); a shorter feed resets the memo and re-folds from scratch.
    public func project(_ events: [SessionEvent]) -> Transcript {
      if events.count == foldedCount { return cached }
      if events.count < foldedCount {
        builder = Builder()
        foldedCount = 0
      }
      for index in foldedCount..<events.count {
        builder.ingest(events[index])
      }
      foldedCount = events.count
      cached = builder.transcript()
      return cached
    }
  }

  /// The mutable fold state — an ordered, id-keyed item map plus the turn chrome.
  private struct Builder {
    private var order: [String] = []
    private var items: [String: TranscriptItem] = [:]
    private var isTurnActive = false
    private var lastUsage: Usage?
    private var sequence = 0

    func transcript() -> Transcript {
      Transcript(
        items: order.compactMap { items[$0] },
        isTurnActive: isTurnActive,
        lastUsage: lastUsage)
    }

    /// Folds one event into the transcript, dispatched by category so each handler
    /// stays small and auditable (the union is wide).
    mutating func ingest(_ event: SessionEvent) {
      switch event {
      case .turnStarted, .turnCompleted, .sessionIdle:
        ingestLifecycle(event)
      case .messageStarted, .messageDelta, .messageCompleted:
        ingestMessage(event)
      case .toolStarted, .toolProgress, .toolCompleted:
        ingestTool(event)
      case .uiRequestRaised, .notice, .statusChanged, .retryScheduled, .contextCompacted:
        ingestSignal(event)
      case .entryAppended(let entry):
        ingest(entry)
      }
    }

    private mutating func ingestLifecycle(_ event: SessionEvent) {
      switch event {
      case .turnStarted:
        isTurnActive = true
      case .turnCompleted(let usage):
        isTurnActive = false
        if let usage { lastUsage = usage }
      case .sessionIdle:
        isTurnActive = false
      default:
        break
      }
    }

    private mutating func ingestMessage(_ event: SessionEvent) {
      switch event {
      case .messageStarted(let messageId):
        upsertMessage(messageId) { _ in }
      case .messageDelta(let messageId, let text, let reasoning):
        upsertMessage(messageId) { message in
          // A delta arriving AFTER the message was finalized — by its durable
          // `AssistantMessage` entry or a `messageCompleted` — is stale, out-of-order
          // wire noise; ignore it rather than append onto the canonical text (the
          // same defensiveness the tool path already has for out-of-order results).
          guard !message.isComplete else { return }
          if let text { message.text += text }
          if let reasoning { message.reasoning = (message.reasoning ?? "") + reasoning }
        }
      case .messageCompleted(let messageId):
        upsertMessage(messageId) { message in message.isComplete = true }
      default:
        break
      }
    }

    private mutating func ingestTool(_ event: SessionEvent) {
      switch event {
      case .toolStarted(let id, let name, let input):
        upsertTool(id) { call in
          call.name = name
          call.input = input
        }
      case .toolProgress:
        // A partial-output preview: the item model tracks the call and its final
        // result, not intermediate previews, so this is folded without effect.
        break
      case .toolCompleted(let id, let output, let isError):
        upsertTool(id) { call in
          call.output = output
          call.isError = isError
          call.isComplete = true
        }
      default:
        break
      }
    }

    private mutating func ingestSignal(_ event: SessionEvent) {
      switch event {
      case .uiRequestRaised:
        // Surfaced inline via `InteractiveSession.outstandingRequests`, not the
        // transcript.
        break
      case .notice(let id, let level, let message):
        appendNotice(id: id, level: level, message: message)
      case .statusChanged(let key, let text):
        upsert("status:\(key)") { _ in .status(TranscriptStatus(key: key, text: text)) }
      case .retryScheduled(let attempt, let delayMs, let error):
        let id = nextSequence()
        upsert("retry:\(id)") { _ in
          .retry(TranscriptRetry(id: id, attempt: attempt, delayMs: delayMs, error: error))
        }
      case .contextCompacted:
        let id = nextSequence()
        upsert("compaction:\(id)") { _ in .compaction(TranscriptMarker(id: id)) }
      default:
        break
      }
    }

    /// Reconciles a durable transcript entry onto the same items its live deltas
    /// built — the durable value is canonical and replaces the accreted deltas.
    private mutating func ingest(_ entry: TranscriptEntry) {
      switch entry {
      case .userMessage(let id, let text):
        upsert("message:\(id)") { _ in
          .message(
            TranscriptMessage(id: id, role: .user, text: text, reasoning: nil, isComplete: true))
        }
      case .assistantMessage(let id, let text, let reasoning):
        upsert("message:\(id)") { _ in
          .message(
            TranscriptMessage(
              id: id, role: .assistant, text: text, reasoning: reasoning, isComplete: true))
        }
      case .toolCall(let id, let name, let input):
        upsertTool(id) { call in
          call.name = name
          call.input = input
        }
      case .toolResult(let id, let output, let isError):
        upsertTool(id) { call in
          call.output = output
          call.isError = isError
          call.isComplete = true
        }
      case .noticeEntry(let id, let level, let message):
        appendNotice(id: id, level: level, message: message)
      }
    }

    // MARK: - Item helpers

    /// Inserts (first appearance) or updates the item at `key`, preserving order.
    private mutating func upsert(_ key: String, _ transform: (TranscriptItem?) -> TranscriptItem) {
      let existing = items[key]
      if existing == nil { order.append(key) }
      items[key] = transform(existing)
    }

    /// Creates or mutates the assistant/user message coalesced under `messageId`.
    private mutating func upsertMessage(
      _ messageId: String,
      _ mutate: (inout TranscriptMessage) -> Void
    ) {
      upsert("message:\(messageId)") { existing in
        var message =
          Self.message(existing)
          ?? TranscriptMessage(
            id: messageId, role: .assistant, text: "", reasoning: nil, isComplete: false)
        mutate(&message)
        return .message(message)
      }
    }

    /// Creates or mutates the tool call coalesced under tool `id`.
    private mutating func upsertTool(_ id: String, _ mutate: (inout TranscriptToolCall) -> Void) {
      upsert("tool:\(id)") { existing in
        var call =
          Self.tool(existing)
          ?? TranscriptToolCall(
            id: id, name: "", input: .null, output: nil, isError: false, isComplete: false)
        mutate(&call)
        return .toolCall(call)
      }
    }

    /// Reconciles a notice onto its item by the wire reconciliation key (`NoticeId`):
    /// a live `Notice` and the durable `NoticeEntry` of the SAME logical event share
    /// the key, so they render as ONE item rather than double-rendering (CE5.2 /
    /// INV-REACTIVE — mirroring how message/tool ids coalesce their live deltas and
    /// durable entries). Distinct notices carry distinct keys and stay distinct.
    ///
    /// The key is OPTIONAL on a live `Notice`: a content-derived notice with no stable
    /// cross-emission identity (`id == nil`) has no durable counterpart to reconcile
    /// with, so it takes a fresh arrival-sequence key and stays distinct from every
    /// other occurrence — never collapsing two separate occurrences onto one item. A
    /// notice WITH an `id` (and every durable `NoticeEntry`, whose id is required)
    /// keys by it so the live+durable pair reconciles.
    private mutating func appendNotice(id: String?, level: NoticeLevel, message: String) {
      // Keyed (caller `id`) and id-less (arrival-sequence) notices live in DISJOINT
      // key namespaces, so a caller id that happens to be a bare decimal (a `NoticeId`
      // carries no format constraint) can never collide with a sequence value and
      // silently collapse a keyed notice and an unrelated id-less one onto one item.
      let key = id.map { "key:\($0)" } ?? "seq:\(nextSequence())"
      upsert("notice:\(key)") { _ in
        .notice(TranscriptNotice(id: key, level: level, message: message))
      }
    }

    /// A monotonically increasing arrival sequence for point-in-time items, so each
    /// notice/retry/compaction is a distinct, stably-ordered item.
    private mutating func nextSequence() -> String {
      sequence += 1
      return String(sequence)
    }

    private static func message(_ item: TranscriptItem?) -> TranscriptMessage? {
      if case .message(let message)? = item { return message }
      return nil
    }

    private static func tool(_ item: TranscriptItem?) -> TranscriptToolCall? {
      if case .toolCall(let call)? = item { return call }
      return nil
    }
  }
}
