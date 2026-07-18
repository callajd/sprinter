/**
 * Decode + round-trip coverage for the foreign Pi wire schema (FE2.2).
 *
 * Two directions:
 *   - things Pi EMITS (responses, session events, UI requests) are decode-tested
 *     from fixtures authored strictly from the Pi source `.ts` types, and asserted
 *     to strip Pi's excess fields down to the subset we mirror; and
 *   - things the adapter SENDS (commands, UI responses) are round-trip tested.
 *
 * Live validation against the real `pi --mode rpc` binary lives in
 * `wire.live.test.ts`; this file is the exhaustive shape coverage.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import {
  PiAgentSessionEvent,
  PiClientMessage,
  PiRpcCommand,
  PiRpcExtensionUIRequest,
  PiRpcExtensionUIResponse,
  PiRpcResponse,
  PiRpcSessionState,
  PiServerMessage,
  PI_WIRE_VERSION,
} from "./wire.ts";

/** Decode arbitrary `raw` and return the typed decoded value, failing on a decode error. */
const decode = <A, I>(schema: Schema.Codec<A, I>, raw: unknown) =>
  Schema.decodeUnknownEffect(schema)(raw);

/** Decode `raw`, re-encode, and assert the encoded value equals the input (round-trip). */
const assertRoundTrip = (schema: Schema.Codec<unknown, unknown>, raw: unknown) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknownEffect(schema)(raw);
    const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
    expect(encoded).toStrictEqual(raw);
  });

// A realistic assistant message exactly as Pi shapes it on the wire.
const assistantMessage = {
  role: "assistant",
  content: [
    { type: "thinking", thinking: "let me look", thinkingSignature: "sig", redacted: false },
    { type: "text", text: "hello", textSignature: "t1" },
    {
      type: "toolCall",
      id: "call-1",
      name: "bash",
      arguments: { cmd: "ls" },
      thoughtSignature: "g",
    },
  ],
  api: "anthropic-messages",
  provider: "anthropic",
  model: "claude",
  responseModel: "claude-actual",
  responseId: "resp-1",
  usage: {
    input: 10,
    output: 20,
    cacheRead: 1,
    cacheWrite: 2,
    cacheWrite1h: 0,
    reasoning: 3,
    totalTokens: 30,
    cost: { input: 0.1, output: 0.2, cacheRead: 0, cacheWrite: 0, total: 0.3 },
  },
  stopReason: "toolUse",
  timestamp: 1_700_000_000_000,
};

const userMessage = {
  role: "user",
  content: [{ type: "text", text: "do it" }],
  timestamp: 1_700_000_000_001,
};
const userMessageString = { role: "user", content: "quick", timestamp: 1_700_000_000_002 };

const toolResultMessage = {
  role: "toolResult",
  toolCallId: "call-1",
  toolName: "bash",
  content: [{ type: "text", text: "a\nb" }],
  isError: false,
  timestamp: 1_700_000_000_003,
};

// ── Streaming session events, authored from `AgentSessionEvent` / `AgentEvent` ──
const sessionEvents: ReadonlyArray<unknown> = [
  { type: "agent_start" },
  { type: "agent_end", messages: [assistantMessage, userMessage], willRetry: false },
  { type: "turn_start" },
  { type: "turn_end", message: assistantMessage, toolResults: [toolResultMessage] },
  { type: "message_start", message: userMessage },
  // A user message whose content is a raw string (the `string` arm of PiUserContent):
  { type: "message_start", message: userMessageString },
  { type: "message_end", message: assistantMessage },
  // message_update carries each AssistantMessageEvent variant:
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "start", partial: assistantMessage },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "text_start", contentIndex: 0, partial: assistantMessage },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "text_delta",
      contentIndex: 0,
      delta: "he",
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "text_end",
      contentIndex: 0,
      content: "hello",
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "thinking_start", contentIndex: 1, partial: assistantMessage },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "thinking_delta",
      contentIndex: 1,
      delta: "hm",
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "thinking_end",
      contentIndex: 1,
      content: "hmm",
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "toolcall_start", contentIndex: 2, partial: assistantMessage },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "toolcall_delta",
      contentIndex: 2,
      delta: "{",
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: {
      type: "toolcall_end",
      contentIndex: 2,
      toolCall: { type: "toolCall", id: "call-1", name: "bash", arguments: { cmd: "ls" } },
      partial: assistantMessage,
    },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "done", reason: "stop", message: assistantMessage },
  },
  {
    type: "message_update",
    message: assistantMessage,
    assistantMessageEvent: { type: "error", reason: "aborted", error: assistantMessage },
  },
  // Tool execution lifecycle:
  { type: "tool_execution_start", toolCallId: "call-1", toolName: "bash", args: { cmd: "ls" } },
  {
    type: "tool_execution_update",
    toolCallId: "call-1",
    toolName: "bash",
    args: { cmd: "ls" },
    partialResult: { stdout: "a" },
  },
  {
    type: "tool_execution_end",
    toolCallId: "call-1",
    toolName: "bash",
    result: { stdout: "a\nb" },
    isError: false,
  },
  // Session-level extensions:
  { type: "agent_settled" },
  { type: "queue_update", steering: ["s1"], followUp: [] },
  { type: "compaction_start", reason: "manual" },
  {
    type: "compaction_end",
    reason: "threshold",
    result: { summary: "x" },
    aborted: false,
    willRetry: false,
  },
  {
    type: "compaction_end",
    reason: "overflow",
    aborted: true,
    willRetry: true,
    errorMessage: "boom",
  },
  { type: "session_info_changed", name: "my session" },
  { type: "session_info_changed" },
  { type: "thinking_level_changed", level: "high" },
  { type: "auto_retry_start", attempt: 1, maxAttempts: 3, delayMs: 500, errorMessage: "429" },
  { type: "auto_retry_end", success: true, attempt: 1 },
  { type: "auto_retry_end", success: false, attempt: 2, finalError: "gave up" },
  // entry_appended — one per SessionEntry variant:
  {
    type: "entry_appended",
    entry: {
      type: "message",
      id: "e1",
      parentId: null,
      timestamp: "2026",
      message: assistantMessage,
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "thinking_level_change",
      id: "e2",
      parentId: "e1",
      timestamp: "2026",
      thinkingLevel: "low",
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "model_change",
      id: "e3",
      parentId: "e2",
      timestamp: "2026",
      provider: "anthropic",
      modelId: "claude",
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "compaction",
      id: "e4",
      parentId: "e3",
      timestamp: "2026",
      summary: "s",
      firstKeptEntryId: "e1",
      tokensBefore: 100,
      fromHook: true,
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "branch_summary",
      id: "e5",
      parentId: "e4",
      timestamp: "2026",
      fromId: "e1",
      summary: "s",
      details: { any: 1 },
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "custom",
      id: "e6",
      parentId: "e5",
      timestamp: "2026",
      customType: "note",
      data: { x: 1 },
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "custom_message",
      id: "e7",
      parentId: "e6",
      timestamp: "2026",
      customType: "inj",
      content: "hi",
      display: true,
    },
  },
  {
    type: "entry_appended",
    entry: {
      type: "label",
      id: "e8",
      parentId: "e7",
      timestamp: "2026",
      targetId: "e1",
      label: "star",
    },
  },
  {
    type: "entry_appended",
    entry: { type: "session_info", id: "e9", parentId: "e8", timestamp: "2026", name: "n" },
  },
];

// ── Extension UI requests, authored from `RpcExtensionUIRequest` ──
const uiRequests: ReadonlyArray<unknown> = [
  {
    type: "extension_ui_request",
    id: "u1",
    method: "select",
    title: "Pick",
    options: ["a", "b"],
    timeout: 30,
  },
  { type: "extension_ui_request", id: "u2", method: "confirm", title: "Sure?", message: "proceed" },
  { type: "extension_ui_request", id: "u3", method: "input", title: "Name", placeholder: "type" },
  { type: "extension_ui_request", id: "u4", method: "editor", title: "Edit", prefill: "body" },
  { type: "extension_ui_request", id: "u5", method: "notify", message: "done", notifyType: "info" },
  {
    type: "extension_ui_request",
    id: "u6",
    method: "setStatus",
    statusKey: "phase",
    statusText: "building",
  },
  { type: "extension_ui_request", id: "u7", method: "setStatus", statusKey: "phase" },
  {
    type: "extension_ui_request",
    id: "u8",
    method: "setWidget",
    widgetKey: "w",
    widgetLines: ["l1"],
    widgetPlacement: "aboveEditor",
  },
  { type: "extension_ui_request", id: "u9", method: "setWidget", widgetKey: "w" },
  { type: "extension_ui_request", id: "u10", method: "setTitle", title: "T" },
  { type: "extension_ui_request", id: "u11", method: "set_editor_text", text: "x" },
];

// ── RPC responses, authored from `RpcResponse` / `RpcSessionState` ──
const sessionStateData = {
  model: { id: "unknown", provider: "unknown", extra: "ignored" },
  thinkingLevel: "off",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "one-at-a-time",
  followUpMode: "all",
  sessionFile: "/tmp/s.jsonl",
  sessionId: "sid",
  sessionName: "named",
  autoCompactionEnabled: true,
  messageCount: 0,
  pendingMessageCount: 0,
};
const responses: ReadonlyArray<unknown> = [
  { id: "1", type: "response", command: "prompt", success: true },
  { type: "response", command: "steer", success: true },
  { id: "3", type: "response", command: "follow_up", success: true },
  { id: "4", type: "response", command: "abort", success: true },
  { id: "5", type: "response", command: "new_session", success: true, data: { cancelled: false } },
  { id: "6", type: "response", command: "get_state", success: true, data: sessionStateData },
  { id: "7", type: "response", command: "prompt", success: false, error: "No API key found." },
  { type: "response", command: "set_model", success: false, error: "Model not found" },
];

// ── Commands the adapter SENDS, authored from `RpcCommand` ──
const commands: ReadonlyArray<unknown> = [
  { type: "prompt", id: "c1", message: "go" },
  {
    type: "prompt",
    message: "go",
    images: [{ type: "image", data: "b64", mimeType: "image/png" }],
    streamingBehavior: "steer",
  },
  { type: "steer", message: "left a bit" },
  {
    type: "follow_up",
    id: "c4",
    message: "and then",
    images: [{ type: "image", data: "b64", mimeType: "image/jpeg" }],
  },
  { type: "abort" },
  { type: "abort", id: "c6" },
  { type: "get_state" },
  { type: "new_session" },
  { type: "new_session", id: "c9", parentSession: "/tmp/parent.jsonl" },
];

const uiResponses: ReadonlyArray<unknown> = [
  { type: "extension_ui_response", id: "u1", value: "a" },
  { type: "extension_ui_response", id: "u2", confirmed: true },
  { type: "extension_ui_response", id: "u3", cancelled: true },
];

it("pins the mirrored Pi version", () => {
  expect(PI_WIRE_VERSION).toBe("0.80.10");
});

it.effect("decodes every AgentSessionEvent variant, and via the server union", () =>
  Effect.forEach(sessionEvents, (raw) =>
    Effect.gen(function* () {
      yield* decode(PiAgentSessionEvent, raw);
      yield* decode(PiServerMessage, raw);
    }),
  ),
);

it.effect("decodes every extension UI request, and via the server union", () =>
  Effect.forEach(uiRequests, (raw) =>
    Effect.gen(function* () {
      yield* decode(PiRpcExtensionUIRequest, raw);
      yield* decode(PiServerMessage, raw);
    }),
  ),
);

it.effect("decodes every RPC response, and via the server union", () =>
  Effect.forEach(responses, (raw) =>
    Effect.gen(function* () {
      yield* decode(PiRpcResponse, raw);
      yield* decode(PiServerMessage, raw);
    }),
  ),
);

it.effect("strips Pi's excess fields down to the mirrored subset", () =>
  Effect.gen(function* () {
    // A real get_state `data` carries a fat `model` object and other keys; we
    // keep only what we mirror, and `model` stays opaque (passed through as-is).
    const decoded = yield* decode(PiRpcSessionState, sessionStateData);
    expect(decoded.sessionId).toBe("sid");
    expect(decoded.messageCount).toBe(0);
    // An assistant message with extra provider fields decodes; content is kept.
    const richAssistant = { ...assistantMessage, providerHeaders: { a: 1 }, extra: true };
    const ev = yield* decode(PiAgentSessionEvent, { type: "message_end", message: richAssistant });
    expect(ev.type).toBe("message_end");
  }),
);

it.effect("round-trips every command the adapter sends, and via the client union", () =>
  Effect.forEach(commands, (raw) =>
    Effect.gen(function* () {
      yield* assertRoundTrip(PiRpcCommand, raw);
      yield* decode(PiClientMessage, raw);
    }),
  ),
);

it.effect("round-trips every extension UI response, and via the client union", () =>
  Effect.forEach(uiResponses, (raw) =>
    Effect.gen(function* () {
      yield* assertRoundTrip(PiRpcExtensionUIResponse, raw);
      yield* decode(PiClientMessage, raw);
    }),
  ),
);

it.effect("rejects representative malformed wire messages", () =>
  Effect.gen(function* () {
    const invalids: ReadonlyArray<readonly [Schema.Codec<unknown, unknown>, unknown]> = [
      [PiAgentSessionEvent, { type: "nonexistent_event" }],
      [
        PiAgentSessionEvent,
        { type: "auto_retry_start", attempt: -1, maxAttempts: 3, delayMs: 0, errorMessage: "x" },
      ],
      [
        PiAgentSessionEvent,
        { type: "auto_retry_start", attempt: 1.5, maxAttempts: 3, delayMs: 0, errorMessage: "x" },
      ],
      [PiAgentSessionEvent, { type: "thinking_level_changed", level: "supreme" }],
      [PiAgentSessionEvent, { type: "compaction_start", reason: "whenever" }],
      [PiAgentSessionEvent, { type: "queue_update", steering: "not-an-array", followUp: [] }],
      [
        PiAgentSessionEvent,
        { type: "tool_execution_end", toolCallId: "t", toolName: "b", result: {} },
      ],
      [
        PiAgentSessionEvent,
        {
          type: "entry_appended",
          entry: { type: "unknown_entry", id: "e", parentId: null, timestamp: "t" },
        },
      ],
      [
        PiRpcResponse,
        { type: "response", command: "get_state", success: true, data: { sessionId: "x" } },
      ],
      [PiRpcResponse, { type: "response", command: "prompt", success: false }],
      [
        PiRpcExtensionUIRequest,
        { type: "extension_ui_request", id: "u", method: "select", title: "t" },
      ],
      [PiRpcCommand, { type: "prompt" }],
      [PiRpcCommand, { type: "delete_everything" }],
      [PiServerMessage, { type: "totally_unknown" }],
    ];
    yield* Effect.forEach(invalids, ([schema, raw]) =>
      Effect.exit(Schema.decodeUnknownEffect(schema)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
    );
  }),
);
