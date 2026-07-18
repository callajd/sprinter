import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import {
  SessionEvent,
  SessionInput,
  TranscriptEntry,
  UiAnswer,
  UiResponse,
  Usage,
} from "./session.ts";

/** Decode `raw`, re-encode, and assert the encoded value equals the input (round-trip). */
const assertRoundTrip = (schema: Schema.Codec<unknown, unknown>, raw: unknown) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknownEffect(schema)(raw);
    const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
    expect(encoded).toStrictEqual(raw);
  });

const entries: ReadonlyArray<unknown> = [
  { _tag: "UserMessage", id: "m1", text: "do the thing" },
  { _tag: "AssistantMessage", id: "m2", text: "on it", reasoning: "planning" },
  { _tag: "AssistantMessage", id: "m3", text: "done" },
  { _tag: "ToolCall", id: "t1", name: "bash", input: { cmd: "ls" } },
  { _tag: "ToolResult", id: "t1", output: { stdout: "a\nb" }, isError: false },
  { _tag: "NoticeEntry", id: "notice-1", level: "warn", message: "retrying" },
];

const events: ReadonlyArray<unknown> = [
  { _tag: "TurnStarted" },
  { _tag: "TurnCompleted" }, // usage omitted — a turn can end without a usage report
  { _tag: "TurnCompleted", usage: { inputTokens: 10, outputTokens: 20 } },
  {
    _tag: "TurnCompleted",
    usage: { inputTokens: 10, outputTokens: 20, cacheReadTokens: 5, cacheWriteTokens: 1 },
  },
  { _tag: "MessageStarted", messageId: "m1" },
  { _tag: "MessageDelta", messageId: "m1", text: "hel" },
  { _tag: "MessageDelta", messageId: "m1", reasoning: "thinking" },
  { _tag: "MessageDelta", messageId: "m1", text: "lo", reasoning: "more" },
  { _tag: "MessageCompleted", messageId: "m1" },
  { _tag: "ToolStarted", id: "t1", name: "bash", input: { cmd: "ls" } },
  { _tag: "ToolProgress", id: "t1", partial: { stdout: "a" } },
  { _tag: "ToolCompleted", id: "t1", output: { stdout: "a\nb" }, isError: false },
  { _tag: "SessionIdle" },
  { _tag: "RetryScheduled", attempt: 1, delayMs: 500, error: "429 rate limited" },
  { _tag: "ContextCompacted" },
  { _tag: "UiRequestRaised", id: "u1", kind: "confirm", prompt: "Proceed?" },
  {
    _tag: "UiRequestRaised",
    id: "u2",
    kind: "select",
    prompt: "Pick one",
    options: ["a", "b"],
  },
  { _tag: "Notice", id: "notice-1", level: "info", message: "started" },
  { _tag: "StatusChanged", key: "phase", text: "implementing" },
  { _tag: "EntryAppended", entry: { _tag: "UserMessage", id: "m1", text: "hi" } },
  {
    _tag: "EntryAppended",
    entry: { _tag: "ToolResult", id: "t1", output: [1, 2, 3], isError: true },
  },
];

it.effect("round-trips every SessionEvent variant", () =>
  Effect.forEach(events, (raw) => assertRoundTrip(SessionEvent, raw)),
);

it.effect("round-trips every TranscriptEntry variant", () =>
  Effect.forEach(entries, (raw) => assertRoundTrip(TranscriptEntry, raw)),
);

it.effect("round-trips Usage, SessionInput, UiResponse, and UiAnswer", () =>
  Effect.gen(function* () {
    yield* assertRoundTrip(Usage, { inputTokens: 3, outputTokens: 4 });
    yield* assertRoundTrip(SessionInput, { text: "go", mode: "prompt" });
    yield* assertRoundTrip(SessionInput, {
      text: "look at this",
      images: ["img-1", "img-2"],
      mode: "followUp",
    });
    yield* assertRoundTrip(SessionInput, { text: "left a bit", mode: "steer" });
    yield* assertRoundTrip(UiAnswer, { _tag: "Value", value: "option-a" });
    yield* assertRoundTrip(UiAnswer, { _tag: "Confirmed", confirmed: true });
    yield* assertRoundTrip(UiAnswer, { _tag: "Cancelled" });
    yield* assertRoundTrip(UiResponse, {
      requestId: "u1",
      answer: { _tag: "Confirmed", confirmed: false },
    });
  }),
);

it.effect("rejects representative invalid session inputs", () =>
  Effect.gen(function* () {
    const invalids: ReadonlyArray<readonly [Schema.Codec<unknown, unknown>, unknown]> = [
      [SessionEvent, { _tag: "Nonexistent" }],
      [SessionEvent, { _tag: "TurnCompleted", usage: { inputTokens: -1, outputTokens: 0 } }],
      [SessionEvent, { _tag: "MessageStarted", messageId: "" }],
      [SessionEvent, { _tag: "RetryScheduled", attempt: 1.5, delayMs: 0, error: "x" }],
      [SessionEvent, { _tag: "UiRequestRaised", id: "u1", kind: "toast", prompt: "?" }],
      [SessionEvent, { _tag: "Notice", id: "n1", level: "fatal", message: "x" }],
      // Notice/NoticeEntry require a non-empty reconciliation `id` (NoticeId, CE5.2).
      [SessionEvent, { _tag: "Notice", id: "", level: "info", message: "x" }],
      [SessionEvent, { _tag: "Notice", level: "info", message: "x" }],
      [TranscriptEntry, { _tag: "NoticeEntry", id: "", level: "info", message: "x" }],
      [SessionEvent, { _tag: "EntryAppended", entry: { _tag: "UnknownEntry" } }],
      [TranscriptEntry, { _tag: "ToolResult", id: "t1", output: {} }],
      [SessionInput, { text: "go", mode: "resume" }],
      [SessionInput, { mode: "prompt" }],
      [UiAnswer, { _tag: "Value", value: 5 }],
      [UiResponse, { requestId: "", answer: { _tag: "Cancelled" } }],
    ];
    yield* Effect.forEach(invalids, ([schema, raw]) =>
      Effect.exit(Schema.decodeUnknownEffect(schema)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
    );
  }),
);
