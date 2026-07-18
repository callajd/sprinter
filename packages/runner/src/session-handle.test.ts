/**
 * `SessionHandle` + boundary-translation coverage (AE1.2).
 *
 * Two layers of test:
 *   - PURE translation/encoding: `translateServerEvent` over representative Pi
 *     wire events → expected owned `SessionEvent`, and `encodeInput` /
 *     `encodeUiResponse` for the inbound direction. This is the coverage-critical
 *     boundary table (INV-BOUNDARY / INV-COV).
 *   - HANDLE wiring: the same fake `ChildProcessSpawner` pattern as AE1.1 stands
 *     in for `pi`, so the tests are deterministic and offline. They prove the
 *     handle consumes the transport's single-consumer event stream, fans out
 *     translated events, drives commands, and resolves its terminal `result`.
 *
 * A live `pi --mode rpc` integration stays out of scope for the same reason as
 * AE1.1 (the binary is a tracked provisioning deferral in `docs/decisions.md`).
 */
import { it } from "@effect/vitest";
import { Cause, Effect, Fiber, Layer, Queue, Ref, Schema, Sink, Stream } from "effect";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import type { SessionEvent } from "@sprinter/domain";
import { PiClientMessage } from "@sprinter/domain/pi/wire";
import type {
  PiAgentMessage,
  PiAgentSessionEvent,
  PiAssistantMessage,
} from "@sprinter/domain/pi/wire";
import type { PiServerEvent } from "./pi-transport.ts";
import {
  encodeInput,
  encodeUiResponse,
  make,
  SessionResult,
  translateServerEvent,
} from "./session-handle.ts";

// ============================================================================
// Fixtures
// ============================================================================

/** A cost breakdown, as `pi` shapes it on an assistant message's usage. */
const cost = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 };

/** Build a full Pi assistant message with the fields the translation reads. */
const assistantMessage = (fields?: {
  readonly content?: PiAssistantMessage["content"];
  readonly responseId?: string;
  readonly timestamp?: number;
  readonly usage?: Partial<PiAssistantMessage["usage"]>;
}): PiAssistantMessage => ({
  role: "assistant",
  content: fields?.content ?? [],
  api: "anthropic",
  provider: "anthropic",
  model: "claude",
  usage: {
    input: 10,
    output: 20,
    cacheRead: 3,
    cacheWrite: 4,
    totalTokens: 30,
    cost,
    ...fields?.usage,
  },
  stopReason: "stop",
  timestamp: fields?.timestamp ?? 1000,
  ...(fields?.responseId !== undefined ? { responseId: fields.responseId } : {}),
});

/** Translate one Pi event and assert the exact owned events it yields. */
const expectTranslation = (event: PiServerEvent, expected: ReadonlyArray<SessionEvent>): void => {
  expect(translateServerEvent(event)).toEqual(expected);
};

// ============================================================================
// Translation — turn & session lifecycle
// ============================================================================

it("drops agent-level lifecycle events with no neutral counterpart", () => {
  expectTranslation({ type: "agent_start" }, []);
  expectTranslation({ type: "agent_end", messages: [], willRetry: false }, []);
  expectTranslation({ type: "queue_update", steering: [], followUp: [] }, []);
  expectTranslation({ type: "compaction_start", reason: "threshold" }, []);
});

it("translates turn lifecycle, carrying usage from the assistant turn message", () => {
  expectTranslation({ type: "turn_start" }, [{ _tag: "TurnStarted" }]);
  expectTranslation({ type: "turn_end", message: assistantMessage(), toolResults: [] }, [
    {
      _tag: "TurnCompleted",
      usage: { inputTokens: 10, outputTokens: 20, cacheReadTokens: 3, cacheWriteTokens: 4 },
    },
  ]);
  // A turn that ends on a non-assistant message carries no usage.
  expectTranslation(
    {
      type: "turn_end",
      message: { role: "user", content: "hi", timestamp: 1 },
      toolResults: [],
    },
    [{ _tag: "TurnCompleted" }],
  );
});

it("translates agent settle and context compaction", () => {
  expectTranslation({ type: "agent_settled" }, [{ _tag: "SessionIdle" }]);
  expectTranslation(
    { type: "compaction_end", reason: "threshold", aborted: false, willRetry: false },
    [{ _tag: "ContextCompacted" }],
  );
  // An aborted compaction did not compact — nothing is emitted.
  expectTranslation(
    { type: "compaction_end", reason: "threshold", aborted: true, willRetry: false },
    [],
  );
});

// ============================================================================
// Translation — message streaming
// ============================================================================

it("translates assistant message start/end to owned start/completed, keyed by responseId", () => {
  const message = assistantMessage({ responseId: "resp-1" });
  expectTranslation({ type: "message_start", message }, [
    { _tag: "MessageStarted", messageId: "resp-1" },
  ]);
  expectTranslation({ type: "message_end", message }, [
    { _tag: "MessageCompleted", messageId: "resp-1" },
  ]);
});

it("falls back to the message timestamp when no responseId is present", () => {
  const message = assistantMessage({ timestamp: 4242 });
  expectTranslation({ type: "message_start", message }, [
    { _tag: "MessageStarted", messageId: "4242" },
  ]);
});

it("does not emit message start/end framing for non-assistant messages", () => {
  const message = { role: "user" as const, content: "hi", timestamp: 1 };
  expectTranslation({ type: "message_start", message }, []);
  expectTranslation({ type: "message_end", message }, []);
});

it("translates streaming text/thinking deltas to MessageDelta", () => {
  const message = assistantMessage({ responseId: "resp-2" });
  expectTranslation(
    {
      type: "message_update",
      message,
      assistantMessageEvent: {
        type: "text_delta",
        contentIndex: 0,
        delta: "hel",
        partial: message,
      },
    },
    [{ _tag: "MessageDelta", messageId: "resp-2", text: "hel" }],
  );
  expectTranslation(
    {
      type: "message_update",
      message,
      assistantMessageEvent: {
        type: "thinking_delta",
        contentIndex: 0,
        delta: "hmm",
        partial: message,
      },
    },
    [{ _tag: "MessageDelta", messageId: "resp-2", reasoning: "hmm" }],
  );
  // Non-delta streaming framing carries no neutral delta.
  expectTranslation(
    {
      type: "message_update",
      message,
      assistantMessageEvent: { type: "text_start", contentIndex: 0, partial: message },
    },
    [],
  );
});

// ============================================================================
// Translation — tool streaming
// ============================================================================

it("translates the tool execution lifecycle", () => {
  expectTranslation(
    { type: "tool_execution_start", toolCallId: "t1", toolName: "bash", args: { cmd: "ls" } },
    [{ _tag: "ToolStarted", id: "t1", name: "bash", input: { cmd: "ls" } }],
  );
  expectTranslation(
    {
      type: "tool_execution_update",
      toolCallId: "t1",
      toolName: "bash",
      args: {},
      partialResult: "partial",
    },
    [{ _tag: "ToolProgress", id: "t1", partial: "partial" }],
  );
  expectTranslation(
    {
      type: "tool_execution_end",
      toolCallId: "t1",
      toolName: "bash",
      result: "done",
      isError: false,
    },
    [{ _tag: "ToolCompleted", id: "t1", output: "done", isError: false }],
  );
});

// ============================================================================
// Translation — durable transcript entries
// ============================================================================

const entry = (id: string, message: PiAgentMessage): PiAgentSessionEvent => ({
  type: "entry_appended",
  entry: { type: "message", id, parentId: null, timestamp: "2024", message },
});

it("translates message session entries into durable EntryAppended records", () => {
  expectTranslation(entry("e1", { role: "user", content: "do it", timestamp: 1 }), [
    { _tag: "EntryAppended", entry: { _tag: "UserMessage", id: "e1", text: "do it" } },
  ]);

  // A user message whose content is text/image blocks collects the text.
  expectTranslation(
    entry("e1b", {
      role: "user",
      content: [
        { type: "text", text: "look at " },
        { type: "image", data: "AA", mimeType: "image/png" },
        { type: "text", text: "this" },
      ],
      timestamp: 1,
    }),
    [{ _tag: "EntryAppended", entry: { _tag: "UserMessage", id: "e1b", text: "look at this" } }],
  );

  const withReasoning = assistantMessage({
    content: [
      { type: "thinking", thinking: "plan" },
      { type: "text", text: "on it" },
    ],
  });
  expectTranslation(entry("e2", withReasoning), [
    {
      _tag: "EntryAppended",
      entry: { _tag: "AssistantMessage", id: "e2", text: "on it", reasoning: "plan" },
    },
  ]);

  // Assistant with no reasoning omits the optional field.
  expectTranslation(entry("e3", assistantMessage({ content: [{ type: "text", text: "done" }] })), [
    { _tag: "EntryAppended", entry: { _tag: "AssistantMessage", id: "e3", text: "done" } },
  ]);

  expectTranslation(
    entry("e4", {
      role: "toolResult",
      toolCallId: "t9",
      toolName: "bash",
      content: [{ type: "text", text: "output" }],
      isError: true,
      timestamp: 1,
    }),
    [
      {
        _tag: "EntryAppended",
        entry: {
          _tag: "ToolResult",
          id: "t9",
          output: [{ type: "text", text: "output" }],
          isError: true,
        },
      },
    ],
  );
});

it("emits durable ToolCall entries for an assistant message's tool-call blocks", () => {
  // An assistant entry that both says something and calls tools yields the
  // AssistantMessage entry followed by one durable ToolCall entry per block, so a
  // client reconciling from EntryAppended has the ToolCall its ToolResult refers to.
  const withToolCalls = assistantMessage({
    content: [
      { type: "text", text: "running it" },
      { type: "toolCall", id: "call-1", name: "bash", arguments: { cmd: "ls" } },
      { type: "toolCall", id: "call-2", name: "read", arguments: { path: "a.ts" } },
    ],
  });
  expectTranslation(entry("e6", withToolCalls), [
    { _tag: "EntryAppended", entry: { _tag: "AssistantMessage", id: "e6", text: "running it" } },
    {
      _tag: "EntryAppended",
      entry: { _tag: "ToolCall", id: "call-1", name: "bash", input: { cmd: "ls" } },
    },
    {
      _tag: "EntryAppended",
      entry: { _tag: "ToolCall", id: "call-2", name: "read", input: { path: "a.ts" } },
    },
  ]);
});

it("drops non-message session entries", () => {
  expectTranslation(
    {
      type: "entry_appended",
      entry: {
        type: "model_change",
        id: "e5",
        parentId: null,
        timestamp: "2024",
        provider: "anthropic",
        modelId: "claude",
      },
    },
    [],
  );
});

// ============================================================================
// Translation — status, retries, and extension surfaces
// ============================================================================

it("translates session-info and thinking-level changes to StatusChanged", () => {
  expectTranslation({ type: "session_info_changed", name: "my session" }, [
    { _tag: "StatusChanged", key: "session_name", text: "my session" },
  ]);
  // No name → nothing to report.
  expectTranslation({ type: "session_info_changed" }, []);
  expectTranslation({ type: "thinking_level_changed", level: "high" }, [
    { _tag: "StatusChanged", key: "thinking_level", text: "high" },
  ]);
});

it("translates auto-retry into RetryScheduled, truncating a fractional delay", () => {
  expectTranslation(
    { type: "auto_retry_start", attempt: 2, maxAttempts: 5, delayMs: 1500.7, errorMessage: "429" },
    [{ _tag: "RetryScheduled", attempt: 2, delayMs: 1500, error: "429" }],
  );
  // A successful retry-end is silent; a failed one always surfaces a give-up
  // Notice — using pi's finalError when present, else a synthesized message so a
  // client watching for give-up never misses it.
  expectTranslation({ type: "auto_retry_end", success: true, attempt: 2 }, []);
  expectTranslation({ type: "auto_retry_end", success: false, attempt: 5, finalError: "gave up" }, [
    { _tag: "Notice", id: "auto-retry-5", level: "error", message: "gave up" },
  ]);
  expectTranslation({ type: "auto_retry_end", success: false, attempt: 5 }, [
    {
      _tag: "Notice",
      id: "auto-retry-5",
      level: "error",
      message: "retry failed after 5 attempt(s)",
    },
  ]);
});

it("translates interactive UI requests to UiRequestRaised", () => {
  expectTranslation(
    {
      type: "extension_ui_request",
      method: "select",
      id: "u1",
      title: "Pick",
      options: ["a", "b"],
    },
    [{ _tag: "UiRequestRaised", id: "u1", kind: "select", prompt: "Pick", options: ["a", "b"] }],
  );
  expectTranslation(
    {
      type: "extension_ui_request",
      method: "confirm",
      id: "u2",
      title: "Sure?",
      message: "Delete all?",
    },
    [{ _tag: "UiRequestRaised", id: "u2", kind: "confirm", prompt: "Delete all?" }],
  );
  expectTranslation({ type: "extension_ui_request", method: "input", id: "u3", title: "Name?" }, [
    { _tag: "UiRequestRaised", id: "u3", kind: "input", prompt: "Name?" },
  ]);
  expectTranslation({ type: "extension_ui_request", method: "editor", id: "u4", title: "Edit" }, [
    { _tag: "UiRequestRaised", id: "u4", kind: "editor", prompt: "Edit" },
  ]);
});

it("translates fire-and-forget UI methods to notices and status, dropping the rest", () => {
  expectTranslation(
    {
      type: "extension_ui_request",
      method: "notify",
      id: "n1",
      message: "heads up",
      notifyType: "warning",
    },
    [{ _tag: "Notice", id: "n1", level: "warn", message: "heads up" }],
  );
  expectTranslation({ type: "extension_ui_request", method: "notify", id: "n2", message: "fyi" }, [
    { _tag: "Notice", id: "n2", level: "info", message: "fyi" },
  ]);
  expectTranslation(
    {
      type: "extension_ui_request",
      method: "notify",
      id: "n3",
      message: "boom",
      notifyType: "error",
    },
    [{ _tag: "Notice", id: "n3", level: "error", message: "boom" }],
  );
  expectTranslation(
    {
      type: "extension_ui_request",
      method: "setStatus",
      id: "s1",
      statusKey: "branch",
      statusText: "main",
    },
    [{ _tag: "StatusChanged", key: "branch", text: "main" }],
  );
  expectTranslation(
    { type: "extension_ui_request", method: "setStatus", id: "s2", statusKey: "branch" },
    [{ _tag: "StatusChanged", key: "branch", text: "" }],
  );
  expectTranslation(
    { type: "extension_ui_request", method: "setTitle", id: "t1", title: "Sprinter" },
    [{ _tag: "StatusChanged", key: "title", text: "Sprinter" }],
  );
  expectTranslation(
    { type: "extension_ui_request", method: "setWidget", id: "w1", widgetKey: "k" },
    [],
  );
  expectTranslation(
    { type: "extension_ui_request", method: "set_editor_text", id: "x1", text: "hi" },
    [],
  );
});

it("translates extension errors to an error Notice", () => {
  expectTranslation(
    { type: "extension_error", extensionPath: "/ext/a", event: "onStart", error: "kaboom" },
    [
      {
        _tag: "Notice",
        id: "extension-error-/ext/a-onStart",
        level: "error",
        message: "extension /ext/a failed handling onStart: kaboom",
      },
    ],
  );
});

// ============================================================================
// Encoding — owned inputs → Pi commands
// ============================================================================

it("encodes each SessionInput mode into its Pi command", () => {
  expect(encodeInput({ text: "hello", mode: "prompt" })).toEqual({
    type: "prompt",
    message: "hello",
  });
  expect(encodeInput({ text: "left not right", mode: "steer" })).toEqual({
    type: "steer",
    message: "left not right",
  });
  expect(encodeInput({ text: "and also", mode: "followUp" })).toEqual({
    type: "follow_up",
    message: "and also",
  });
});

it("encodes input images, recovering the media type from a data URL", () => {
  expect(
    encodeInput({
      text: "look",
      mode: "prompt",
      images: ["data:image/jpeg;base64,QUJD", "rawbytes"],
    }),
  ).toEqual({
    type: "prompt",
    message: "look",
    images: [
      { type: "image", data: "QUJD", mimeType: "image/jpeg" },
      { type: "image", data: "rawbytes", mimeType: "image/png" },
    ],
  });
});

it("encodes each UiResponse answer into its Pi extension UI response", () => {
  expect(encodeUiResponse({ requestId: "u1", answer: { _tag: "Value", value: "text" } })).toEqual({
    type: "extension_ui_response",
    id: "u1",
    value: "text",
  });
  expect(
    encodeUiResponse({ requestId: "u2", answer: { _tag: "Confirmed", confirmed: true } }),
  ).toEqual({
    type: "extension_ui_response",
    id: "u2",
    confirmed: true,
  });
  expect(encodeUiResponse({ requestId: "u3", answer: { _tag: "Cancelled" } })).toEqual({
    type: "extension_ui_response",
    id: "u3",
    cancelled: true,
  });
});

// ============================================================================
// Handle wiring — fake `pi` process
// ============================================================================

/** A fake `ChildProcessSpawner` plus the handles the tests drive it through. */
const makeFakePi = Effect.gen(function* () {
  const stdoutRaw = yield* Queue.make<unknown, Cause.Done>();
  const stdinBytes = yield* Queue.make<Uint8Array, Cause.Done>();
  const killed = yield* Ref.make(false);

  const spawner = ChildProcessSpawner.make(() =>
    Effect.acquireRelease(
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(4321),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          stdin: Sink.forEach<Uint8Array, boolean, never, never>((chunk) =>
            Queue.offer(stdinBytes, chunk),
          ),
          stdout: Stream.fromQueue(stdoutRaw).pipe(
            Stream.pipeThroughChannel(Ndjson.encode()),
            Stream.orDie,
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        }),
      ),
      () => Ref.set(killed, true),
    ),
  );

  return {
    layer: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    stdoutRaw,
    stdinBytes,
    killed,
  } as const;
});

/** Decode exactly `count` client messages from the captured stdin bytes. */
const takeClientMessages = (stdinBytes: Queue.Dequeue<Uint8Array, Cause.Done>, count: number) =>
  Effect.gen(function* () {
    const messages: Array<PiClientMessage> = [];
    while (messages.length < count) {
      const bytes = yield* Queue.take(stdinBytes);
      const lines = new TextDecoder()
        .decode(bytes)
        .split("\n")
        .filter((line) => line.length > 0);
      for (const line of lines) {
        const parsed = yield* Effect.try((): unknown => JSON.parse(line));
        messages.push(yield* Schema.decodeUnknownEffect(PiClientMessage)(parsed));
      }
    }
    return messages;
  });

/** Index into an array, throwing (failing the test) if the element is absent. */
const at = <A>(xs: ReadonlyArray<A>, index: number): A => {
  const value = xs[index];
  if (value === undefined) throw new Error(`expected element at index ${index}`);
  return value;
};

it.effect("fans out translated SessionEvents to a subscriber, in order", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        expect(handle.pid).toBe(ChildProcessSpawner.ProcessId(4321));

        const collecting = yield* Effect.forkChild(
          Stream.runCollect(Stream.take(handle.events, 2)),
        );
        yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
        yield* Queue.offer(fake.stdoutRaw, { type: "agent_settled" });

        const events = yield* Fiber.join(collecting);
        expect(events).toEqual([{ _tag: "TurnStarted" }, { _tag: "SessionIdle" }]);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("drives a SessionInput into the session as the matching Pi command", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        const fiber = yield* Effect.forkChild(handle.send({ text: "go", mode: "prompt" }));

        const command = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        expect(command.type).toBe("prompt");
        if (command.type === "prompt") expect(command.message).toBe("go");

        yield* Queue.offer(fake.stdoutRaw, {
          id: command.id,
          type: "response",
          command: "prompt",
          success: true,
        });
        yield* Fiber.join(fiber);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("interrupts the in-flight turn with an abort command", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        const fiber = yield* Effect.forkChild(handle.interrupt);

        const command = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        expect(command.type).toBe("abort");

        yield* Queue.offer(fake.stdoutRaw, {
          id: command.id,
          type: "response",
          command: "abort",
          success: true,
        });
        yield* Fiber.join(fiber);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("answers an outstanding UI request fire-and-forget", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        yield* handle.answerUi({ requestId: "u1", answer: { _tag: "Confirmed", confirmed: true } });

        const message = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        expect(message.type).toBe("extension_ui_response");
        expect(message.id).toBe("u1");
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("resolves result as Completed and ends events when pi output closes", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        const collecting = yield* Effect.forkChild(Stream.runCollect(handle.events));
        yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
        yield* Queue.end(fake.stdoutRaw);

        const events = yield* Fiber.join(collecting);
        expect(events).toEqual([{ _tag: "TurnStarted" }]);

        const result = yield* handle.result;
        expect(result).toEqual(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" }));
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect(
  "replays recent events and the terminal to a subscriber that attaches after the end",
  () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* make();
          // Emit events and close BEFORE anyone subscribes.
          yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
          yield* Queue.offer(fake.stdoutRaw, { type: "agent_settled" });
          yield* Queue.end(fake.stdoutRaw);

          // `result` resolves only after the pump published the terminal, so once it
          // resolves the whole stream (tail + end) is already in the replay window.
          const result = yield* handle.result;
          expect(result).toEqual(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" }));

          // A subscriber attaching now must still replay the retained tail and see
          // the end (the load-bearing sliding-PubSub replay contract) — not hang.
          const events = yield* Stream.runCollect(handle.events);
          expect(events).toEqual([{ _tag: "TurnStarted" }, { _tag: "SessionIdle" }]);
        }),
      ).pipe(Effect.provide(fake.layer));
    }),
);

it.effect("resolves result as Failed and fails events on a transport error", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* make();
        const collecting = yield* Effect.forkChild(Effect.exit(Stream.runCollect(handle.events)));
        // An undecodable stdout line fails the transport (stream error).
        yield* Queue.offer(fake.stdoutRaw, { type: "totally_unknown_event" });

        const exit = yield* Fiber.join(collecting);
        expect(exit._tag).toBe("Failure");

        const result = yield* handle.result;
        expect(result._tag).toBe("Failed");
        if (result._tag === "Failed") expect(result.error.length).toBeGreaterThan(0);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);
