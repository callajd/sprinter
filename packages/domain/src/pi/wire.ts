/**
 * The `PiAgentRunner` adapter's FOREIGN wire schema (D16 / FE2.2).
 *
 * This is Pi's protocol, mirrored as OUR owned Effect `Schema` — it is NOT the
 * client contract. Every type keeps the Pi qualifier (`PiRpcCommand`,
 * `PiRpcResponse`, `PiAgentSessionEvent`, …) and this module lives in a clearly
 * foreign location (`packages/domain/src/pi/`), deliberately kept OUT of the
 * `@sprinter/domain` barrel so it never leaks into the client-facing RPC surface
 * (FE2.3's `RpcGroup` must not import it). The adapter translates these foreign
 * shapes → our neutral `SessionEvent`/`SessionInput`/`UiResponse` at the
 * boundary; that translation lands in Track A (INV-NAMING, INV-PORT).
 *
 * It is AUTHORED AGAINST, and never imports (D12), the Pi source of truth:
 *   - `~/code/pi/packages/coding-agent/src/modes/rpc/rpc-types.ts`
 *     (`RpcCommand` / `RpcResponse` / `RpcSessionState` /
 *      `RpcExtensionUIRequest` / `RpcExtensionUIResponse`)
 *   - `~/code/pi/packages/coding-agent/src/core/agent-session.ts`
 *     (`AgentSessionEvent`, `SessionStats`)
 *   - `~/code/pi/packages/coding-agent/src/core/session-manager.ts`
 *     (`SessionEntry` and its variants)
 *   - `~/code/pi/packages/agent/src/types.ts` (`AgentEvent`)
 *   - `~/code/pi/packages/ai/src/types.ts` (`Message` content, `Usage`,
 *     `AssistantMessageEvent`)
 * There is ZERO dependency on any `@earendil-works/pi-*` package.
 *
 * It mirrors the SUBSET we use. Effect `Schema.Struct` strips unknown keys on
 * decode, so each foreign message is modelled with the fields the adapter reads;
 * Pi's many extra fields pass through harmlessly. This subset is versioned with
 * the Pi version it mirrors — **pi v0.80.10** — and drift is caught by the live
 * `pi --mode rpc` decode test (INV-CONTRACT).
 */
import { Schema } from "effect";

/** The Pi version this wire schema was authored against and validated with (INV-CONTRACT). */
export const PI_WIRE_VERSION = "0.80.10" as const;

/** A JSON string field that carries no leading/trailing constraints beyond being a string. */
const Str = Schema.String;

/** A non-negative integer — counts, attempts, delays, content indices. */
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** An arbitrary numeric quantity (tokens, costs, timestamps in ms). */
const Num = Schema.Number;

/** Opaque foreign payload we mirror but do not narrow (tool args/results, model, details). */
const Opaque = Schema.Unknown;

/**
 * Build a struct discriminated by a foreign `type` tag. Pi's wire uses a plain
 * `type` string discriminant (not Effect's `_tag`), so foreign unions are
 * assembled from `Schema.Union` over these tagged structs.
 */
const tagged = <Tag extends string, Fields extends Schema.Struct.Fields>(
  type: Tag,
  fields: Fields,
) => Schema.Struct({ type: Schema.Literal(type), ...fields });

/** Build a struct discriminated by a foreign `method` tag (extension UI requests). */
const method = <M extends string, Fields extends Schema.Struct.Fields>(m: M, fields: Fields) =>
  Schema.Struct({
    type: Schema.Literal("extension_ui_request"),
    method: Schema.Literal(m),
    ...fields,
  });

// ============================================================================
// Shared leaf types (from `@earendil-works/pi-ai` `types.ts`)
// ============================================================================

/** Pi's reasoning effort levels (`ThinkingLevel`). */
export const PiThinkingLevel = Schema.Literals([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);
export type PiThinkingLevel = (typeof PiThinkingLevel)["Type"];

/** How queued messages drain (`steeringMode` / `followUpMode`). */
export const PiQueueMode = Schema.Literals(["all", "one-at-a-time"]);
export type PiQueueMode = (typeof PiQueueMode)["Type"];

/** Terminal reason for an assistant turn (`StopReason`). */
export const PiStopReason = Schema.Literals(["stop", "length", "toolUse", "error", "aborted"]);
export type PiStopReason = (typeof PiStopReason)["Type"];

/** A text content block. */
export const PiTextContent = tagged("text", {
  text: Str,
  textSignature: Schema.optionalKey(Str),
});

/** A reasoning/thinking content block. */
export const PiThinkingContent = tagged("thinking", {
  thinking: Str,
  thinkingSignature: Schema.optionalKey(Str),
  redacted: Schema.optionalKey(Schema.Boolean),
});

/** An image content block (base64 data). */
export const PiImageContent = tagged("image", {
  data: Str,
  mimeType: Str,
});
export type PiImageContent = (typeof PiImageContent)["Type"];

/** An assistant-issued tool call. */
export const PiToolCall = tagged("toolCall", {
  id: Str,
  name: Str,
  arguments: Schema.Record(Str, Opaque),
  thoughtSignature: Schema.optionalKey(Str),
});

/** Token accounting reported on an assistant message (`Usage`). */
export const PiUsage = Schema.Struct({
  input: Num,
  output: Num,
  cacheRead: Num,
  cacheWrite: Num,
  cacheWrite1h: Schema.optionalKey(Num),
  reasoning: Schema.optionalKey(Num),
  totalTokens: Num,
  cost: Schema.Struct({
    input: Num,
    output: Num,
    cacheRead: Num,
    cacheWrite: Num,
    total: Num,
  }),
});
export type PiUsage = (typeof PiUsage)["Type"];

/** Content of a user message: raw text, or an array of text/image blocks. */
const PiUserContent = Schema.Union([
  Str,
  Schema.Array(Schema.Union([PiTextContent, PiImageContent])),
]);

/** A user message. */
export const PiUserMessage = Schema.Struct({
  role: Schema.Literal("user"),
  content: PiUserContent,
  timestamp: Num,
});

/** An assistant message (mirrors the fields the adapter reads plus the required envelope). */
export const PiAssistantMessage = Schema.Struct({
  role: Schema.Literal("assistant"),
  content: Schema.Array(Schema.Union([PiTextContent, PiThinkingContent, PiToolCall])),
  api: Str,
  provider: Str,
  model: Str,
  responseModel: Schema.optionalKey(Str),
  responseId: Schema.optionalKey(Str),
  usage: PiUsage,
  stopReason: PiStopReason,
  errorMessage: Schema.optionalKey(Str),
  timestamp: Num,
});
export type PiAssistantMessage = (typeof PiAssistantMessage)["Type"];

/** A tool-result message. */
export const PiToolResultMessage = Schema.Struct({
  role: Schema.Literal("toolResult"),
  toolCallId: Str,
  toolName: Str,
  content: Schema.Array(Schema.Union([PiTextContent, PiImageContent])),
  details: Schema.optionalKey(Opaque),
  addedToolNames: Schema.optionalKey(Schema.Array(Str)),
  isError: Schema.Boolean,
  timestamp: Num,
});

/** A message on the wire (`Message` / `AgentMessage`). */
export const PiAgentMessage = Schema.Union([
  PiUserMessage,
  PiAssistantMessage,
  PiToolResultMessage,
]);
export type PiAgentMessage = (typeof PiAgentMessage)["Type"];

// ============================================================================
// Streaming assistant-message events (`AssistantMessageEvent`)
// ============================================================================

/**
 * Fine-grained streaming protocol carried by `message_update`. The adapter reads
 * `text_delta`/`thinking_delta` `delta`s to synthesize our `MessageDelta`.
 */
export const PiAssistantMessageEvent = Schema.Union([
  tagged("start", { partial: PiAssistantMessage }),
  tagged("text_start", { contentIndex: NonNegativeInt, partial: PiAssistantMessage }),
  tagged("text_delta", { contentIndex: NonNegativeInt, delta: Str, partial: PiAssistantMessage }),
  tagged("text_end", { contentIndex: NonNegativeInt, content: Str, partial: PiAssistantMessage }),
  tagged("thinking_start", { contentIndex: NonNegativeInt, partial: PiAssistantMessage }),
  tagged("thinking_delta", {
    contentIndex: NonNegativeInt,
    delta: Str,
    partial: PiAssistantMessage,
  }),
  tagged("thinking_end", {
    contentIndex: NonNegativeInt,
    content: Str,
    partial: PiAssistantMessage,
  }),
  tagged("toolcall_start", { contentIndex: NonNegativeInt, partial: PiAssistantMessage }),
  tagged("toolcall_delta", {
    contentIndex: NonNegativeInt,
    delta: Str,
    partial: PiAssistantMessage,
  }),
  tagged("toolcall_end", {
    contentIndex: NonNegativeInt,
    toolCall: PiToolCall,
    partial: PiAssistantMessage,
  }),
  tagged("done", {
    reason: Schema.Literals(["stop", "length", "toolUse"]),
    message: PiAssistantMessage,
  }),
  tagged("error", { reason: Schema.Literals(["aborted", "error"]), error: PiAssistantMessage }),
]);
export type PiAssistantMessageEvent = (typeof PiAssistantMessageEvent)["Type"];

// ============================================================================
// Session entries (`SessionEntry`, carried by `entry_appended`)
// ============================================================================

/** Fields shared by every persisted session entry (`SessionEntryBase`). */
const sessionEntryBase = {
  id: Str,
  parentId: Schema.NullOr(Str),
  timestamp: Str,
};

/** Content of a custom-message entry. */
const PiCustomMessageContent = Schema.Union([
  Str,
  Schema.Array(Schema.Union([PiTextContent, PiImageContent])),
]);

/** A durable session entry appended to the transcript (`SessionEntry`). */
export const PiSessionEntry = Schema.Union([
  tagged("message", { ...sessionEntryBase, message: PiAgentMessage }),
  tagged("thinking_level_change", { ...sessionEntryBase, thinkingLevel: Str }),
  tagged("model_change", { ...sessionEntryBase, provider: Str, modelId: Str }),
  tagged("compaction", {
    ...sessionEntryBase,
    summary: Str,
    firstKeptEntryId: Str,
    tokensBefore: Num,
    details: Schema.optionalKey(Opaque),
    fromHook: Schema.optionalKey(Schema.Boolean),
  }),
  tagged("branch_summary", {
    ...sessionEntryBase,
    fromId: Str,
    summary: Str,
    details: Schema.optionalKey(Opaque),
    fromHook: Schema.optionalKey(Schema.Boolean),
  }),
  tagged("custom", { ...sessionEntryBase, customType: Str, data: Schema.optionalKey(Opaque) }),
  tagged("custom_message", {
    ...sessionEntryBase,
    customType: Str,
    content: PiCustomMessageContent,
    details: Schema.optionalKey(Opaque),
    display: Schema.Boolean,
  }),
  tagged("label", { ...sessionEntryBase, targetId: Str, label: Schema.optionalKey(Str) }),
  tagged("session_info", { ...sessionEntryBase, name: Schema.optionalKey(Str) }),
]);
export type PiSessionEntry = (typeof PiSessionEntry)["Type"];

// ============================================================================
// Streaming session events (`AgentSessionEvent` = `AgentEvent` + extensions)
// ============================================================================

/** Reason a compaction ran (`compaction_start` / `compaction_end`). */
const PiCompactionReason = Schema.Literals(["manual", "threshold", "overflow"]);

/**
 * The streaming event union emitted on stdout during a run. Mirrors Pi's
 * `AgentSessionEvent`: the core `AgentEvent` (agent/turn/message/tool lifecycle,
 * with the session-level `agent_end` override carrying `messages`/`willRetry`)
 * plus the session extensions (settle, queue, compaction, retry, entry append,
 * session-info/thinking-level changes).
 */
export const PiAgentSessionEvent = Schema.Union([
  // ── Agent lifecycle ───────────────────────────────────────────────
  tagged("agent_start", {}),
  tagged("agent_end", { messages: Schema.Array(PiAgentMessage), willRetry: Schema.Boolean }),
  // ── Turn lifecycle ────────────────────────────────────────────────
  tagged("turn_start", {}),
  tagged("turn_end", {
    message: PiAgentMessage,
    toolResults: Schema.Array(PiToolResultMessage),
  }),
  // ── Message lifecycle ─────────────────────────────────────────────
  tagged("message_start", { message: PiAgentMessage }),
  tagged("message_update", {
    message: PiAgentMessage,
    assistantMessageEvent: PiAssistantMessageEvent,
  }),
  tagged("message_end", { message: PiAgentMessage }),
  // ── Tool execution lifecycle ──────────────────────────────────────
  tagged("tool_execution_start", { toolCallId: Str, toolName: Str, args: Opaque }),
  tagged("tool_execution_update", {
    toolCallId: Str,
    toolName: Str,
    args: Opaque,
    partialResult: Opaque,
  }),
  tagged("tool_execution_end", {
    toolCallId: Str,
    toolName: Str,
    result: Opaque,
    isError: Schema.Boolean,
  }),
  // ── Session-level extensions ──────────────────────────────────────
  tagged("agent_settled", {}),
  tagged("queue_update", {
    steering: Schema.Array(Str),
    followUp: Schema.Array(Str),
  }),
  tagged("compaction_start", { reason: PiCompactionReason }),
  tagged("compaction_end", {
    reason: PiCompactionReason,
    result: Schema.optionalKey(Opaque),
    aborted: Schema.Boolean,
    willRetry: Schema.Boolean,
    errorMessage: Schema.optionalKey(Str),
  }),
  tagged("entry_appended", { entry: PiSessionEntry }),
  tagged("session_info_changed", { name: Schema.optionalKey(Str) }),
  tagged("thinking_level_changed", { level: PiThinkingLevel }),
  tagged("auto_retry_start", {
    attempt: NonNegativeInt,
    maxAttempts: NonNegativeInt,
    // Pi types `delayMs` as plain `number` (`baseDelayMs * 2**(attempt-1)`), which
    // is fractional if `baseDelayMs` is — mirror it faithfully, not as an int.
    delayMs: Num,
    errorMessage: Str,
  }),
  tagged("auto_retry_end", {
    success: Schema.Boolean,
    attempt: NonNegativeInt,
    finalError: Schema.optionalKey(Str),
  }),
]);
export type PiAgentSessionEvent = (typeof PiAgentSessionEvent)["Type"];

// ============================================================================
// Extension UI requests (`RpcExtensionUIRequest`, emitted on stdout)
// ============================================================================

/**
 * Interactive UI requests raised mid-session. The adapter translates the
 * response-bearing methods (`select`/`confirm`/`input`/`editor`) into our
 * neutral `UiRequestRaised`; the fire-and-forget methods (`notify`/`setStatus`/
 * `setWidget`/`setTitle`/`set_editor_text`) map to notices/status.
 */
export const PiRpcExtensionUIRequest = Schema.Union([
  method("select", {
    id: Str,
    title: Str,
    options: Schema.Array(Str),
    timeout: Schema.optionalKey(Num),
  }),
  method("confirm", { id: Str, title: Str, message: Str, timeout: Schema.optionalKey(Num) }),
  method("input", {
    id: Str,
    title: Str,
    placeholder: Schema.optionalKey(Str),
    timeout: Schema.optionalKey(Num),
  }),
  method("editor", { id: Str, title: Str, prefill: Schema.optionalKey(Str) }),
  method("notify", {
    id: Str,
    message: Str,
    notifyType: Schema.optionalKey(Schema.Literals(["info", "warning", "error"])),
  }),
  method("setStatus", { id: Str, statusKey: Str, statusText: Schema.optionalKey(Str) }),
  method("setWidget", {
    id: Str,
    widgetKey: Str,
    widgetLines: Schema.optionalKey(Schema.Array(Str)),
    widgetPlacement: Schema.optionalKey(Schema.Literals(["aboveEditor", "belowEditor"])),
  }),
  method("setTitle", { id: Str, title: Str }),
  method("set_editor_text", { id: Str, text: Str }),
]);
export type PiRpcExtensionUIRequest = (typeof PiRpcExtensionUIRequest)["Type"];

/** An extension error surfaced on stdout (`extension_error`). */
export const PiExtensionError = tagged("extension_error", {
  extensionPath: Str,
  event: Str,
  error: Str,
});

// ============================================================================
// RPC responses (`RpcResponse`, emitted on stdout)
// ============================================================================

/** Session state returned by `get_state` (`RpcSessionState`), subset we consume. */
export const PiRpcSessionState = Schema.Struct({
  model: Schema.optionalKey(Opaque),
  thinkingLevel: PiThinkingLevel,
  isStreaming: Schema.Boolean,
  isCompacting: Schema.Boolean,
  steeringMode: PiQueueMode,
  followUpMode: PiQueueMode,
  sessionFile: Schema.optionalKey(Str),
  sessionId: Str,
  sessionName: Schema.optionalKey(Str),
  autoCompactionEnabled: Schema.Boolean,
  messageCount: NonNegativeInt,
  pendingMessageCount: NonNegativeInt,
});
export type PiRpcSessionState = (typeof PiRpcSessionState)["Type"];

/** Optional correlation id echoed back on responses. */
const OptId = Schema.optionalKey(Str);

/**
 * The response envelope, subset we consume: the async command acks
 * (`prompt`/`steer`/`follow_up`/`abort`, and `new_session` with `cancelled`),
 * the `get_state` snapshot, and the universal error response.
 */
export const PiRpcResponse = Schema.Union([
  Schema.Struct({
    id: OptId,
    type: Schema.Literal("response"),
    command: Schema.Literals(["prompt", "steer", "follow_up", "abort"]),
    success: Schema.Literal(true),
  }),
  Schema.Struct({
    id: OptId,
    type: Schema.Literal("response"),
    command: Schema.Literal("new_session"),
    success: Schema.Literal(true),
    data: Schema.Struct({ cancelled: Schema.Boolean }),
  }),
  Schema.Struct({
    id: OptId,
    type: Schema.Literal("response"),
    command: Schema.Literal("get_state"),
    success: Schema.Literal(true),
    data: PiRpcSessionState,
  }),
  Schema.Struct({
    id: OptId,
    type: Schema.Literal("response"),
    command: Str,
    success: Schema.Literal(false),
    error: Str,
  }),
]);
export type PiRpcResponse = (typeof PiRpcResponse)["Type"];

// ============================================================================
// RPC commands (`RpcCommand`, sent on stdin) — subset the adapter drives
// ============================================================================

/** A prompt/steer/follow-up carries a message plus optional image attachments. */
const promptLike = {
  id: OptId,
  message: Str,
  images: Schema.optionalKey(Schema.Array(PiImageContent)),
};

/**
 * The command subset the adapter SENDS: the three input modes
 * (`prompt`/`steer`/`follow_up`) our `SessionInput` maps to, plus `abort`
 * (interrupt), `get_state` (handshake), and `new_session`.
 */
export const PiRpcCommand = Schema.Union([
  tagged("prompt", {
    ...promptLike,
    streamingBehavior: Schema.optionalKey(Schema.Literals(["steer", "followUp"])),
  }),
  tagged("steer", promptLike),
  tagged("follow_up", promptLike),
  tagged("abort", { id: OptId }),
  tagged("get_state", { id: OptId }),
  tagged("new_session", { id: OptId, parentSession: Schema.optionalKey(Str) }),
]);
export type PiRpcCommand = (typeof PiRpcCommand)["Type"];

/** The response to an extension UI request (`RpcExtensionUIResponse`, sent on stdin). */
export const PiRpcExtensionUIResponse = Schema.Union([
  tagged("extension_ui_response", { id: Str, value: Str }),
  tagged("extension_ui_response", { id: Str, confirmed: Schema.Boolean }),
  tagged("extension_ui_response", { id: Str, cancelled: Schema.Literal(true) }),
]);
export type PiRpcExtensionUIResponse = (typeof PiRpcExtensionUIResponse)["Type"];

// ============================================================================
// Top-level wire directions
// ============================================================================

/**
 * Anything Pi emits on stdout that the adapter drives: a response envelope, a
 * streaming session event, an extension UI request, or an extension error. The
 * live decode test feeds captured NDJSON lines through this to catch drift
 * (INV-CONTRACT) — but only WITHIN the mirrored subset: a line for a command or
 * event we deliberately do not mirror (e.g. a `set_model`/`bash` success, or a
 * future event type) is not covered here and would fail this decode. That is
 * intended for the adapter's controlled command set; it is not a whole-protocol
 * validator.
 */
export const PiServerMessage = Schema.Union([
  PiRpcResponse,
  PiAgentSessionEvent,
  PiRpcExtensionUIRequest,
  PiExtensionError,
]);
export type PiServerMessage = (typeof PiServerMessage)["Type"];

/** Anything the adapter sends to Pi on stdin: a command or a UI response. */
export const PiClientMessage = Schema.Union([PiRpcCommand, PiRpcExtensionUIResponse]);
export type PiClientMessage = (typeof PiClientMessage)["Type"];
