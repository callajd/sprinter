/**
 * The owned, provider-neutral execution model (D16 / D17).
 *
 * These are OUR types with plain names — `ExecutionEvent` (never Pi's own
 * qualified agent-event name), `ExecutionInput`, `UiResponse`. They must not leak
 * any Pi concept: nothing here imports or names a `Rpc*` type or a Pi-specific
 * field, and there is zero dependency on any `@earendil-works/pi-*` package. Our
 * shape MIRRORS Pi's already-general abstraction (Pi generalizes across model
 * providers) but we own it as our Effect `Schema`; the FE2.2/Track A adapter
 * translates Pi's foreign agent-event type ({@link ./pi/wire.ts}) → our
 * `ExecutionEvent` at the boundary (INV-NAMING, INV-PORT).
 *
 * {@link ExecutionEvent} is maximally reactive (D17 / INV-REACTIVE): the union
 * carries BOTH fine-grained streaming deltas (message/tool progress) AND the
 * durable transcript entries, so one stream both renders live and reconciles
 * into the transcript-grade record.
 */
import { Schema } from "effect";
import { NonNegativeInt } from "./numeric.ts";

/**
 * Arbitrary JSON carried through tool boundaries (input / output / partial).
 * The execution model is agnostic to a tool's payload shape, so it stays
 * `unknown` here and is narrowed by the tool that owns it.
 */
const JsonValue = Schema.Unknown;

/** Token accounting reported when a turn completes. */
export const Usage = Schema.Struct({
  inputTokens: NonNegativeInt,
  outputTokens: NonNegativeInt,
  cacheReadTokens: Schema.optionalKey(NonNegativeInt),
  cacheWriteTokens: Schema.optionalKey(NonNegativeInt),
});
export type Usage = (typeof Usage)["Type"];

/** The kinds of UI request an agent can raise mid-execution. */
export const UiRequestKind = Schema.Literals(["select", "confirm", "input", "editor"]);
export type UiRequestKind = (typeof UiRequestKind)["Type"];

/** Severity of an {@link ExecutionEvent} `Notice`. */
export const NoticeLevel = Schema.Literals(["info", "warn", "error"]);
export type NoticeLevel = (typeof NoticeLevel)["Type"];

/**
 * The wire-level reconciliation key shared by a notice's live (`Notice`
 * {@link ExecutionEvent}) and durable (`NoticeEntry` {@link TranscriptEntry})
 * emissions.
 *
 * A single logical notice can surface BOTH live (rendered as it happens) and
 * durably (reconciled into the transcript-grade record) — INV-REACTIVE. Without a
 * shared key the two would double-render. The contract is: **a producer that emits
 * one logical notice both live and durable MUST stamp both with the same `id`**, so
 * a consumer reconciles them onto one rendered item (exactly as message/tool ids
 * coalesce their deltas and durable entries). Producers derive `id` from the
 * notice's stable identity, so the same logical notice always yields the same key.
 *
 * The key is REQUIRED on a durable `NoticeEntry` (it is transcript-grade) but
 * OPTIONAL on a live `Notice`: a live notice carries it only when it has a stable
 * cross-emission identity worth reconciling on. A content-derived notice with no such
 * identity (e.g. one keyed only by a non-occurrence-unique attribute) OMITS it, and
 * the consumer falls back to arrival-sequence keying so distinct occurrences stay
 * distinct rather than silently collapsing. NOTE for a future durable-notice
 * producer: it MUST reproduce the EXACT same key derivation as the live producer of
 * the same logical notice, or the live+durable pair will not share a key and will
 * double-render.
 */
export const NoticeId = Schema.NonEmptyString;
export type NoticeId = (typeof NoticeId)["Type"];

/**
 * A durable, transcript-grade record appended to an execution's transcript. Carried
 * by the `EntryAppended` {@link ExecutionEvent} so a client reconciles live deltas
 * into the persisted record (INV-REACTIVE).
 */
export const TranscriptEntry = Schema.TaggedUnion({
  UserMessage: { id: Schema.NonEmptyString, text: Schema.String },
  AssistantMessage: {
    id: Schema.NonEmptyString,
    text: Schema.String,
    reasoning: Schema.optionalKey(Schema.String),
  },
  ToolCall: { id: Schema.NonEmptyString, name: Schema.NonEmptyString, input: JsonValue },
  ToolResult: { id: Schema.NonEmptyString, output: JsonValue, isError: Schema.Boolean },
  // `id` is the reconciliation key (see {@link NoticeId}): a durable notice shares
  // it with the live `Notice` of the same logical event so they render once.
  NoticeEntry: { id: NoticeId, level: NoticeLevel, message: Schema.String },
});
export type TranscriptEntry = (typeof TranscriptEntry)["Type"];

/**
 * The neutral execution-event stream type — transcript-grade and maximally
 * reactive. It carries the full reactive flow: turn lifecycle, fine-grained
 * message/reasoning and tool deltas, UI requests, retries/compaction/notices,
 * AND the durable `EntryAppended` record (D17 / INV-REACTIVE).
 *
 * Grounded in Pi's real emissions but OWNED and neutral — plainly named
 * `ExecutionEvent`, not a Pi-qualified name (INV-NAMING; Pi's own vocabulary
 * stays behind the adapter, in {@link ./pi/wire.ts}).
 */
export const ExecutionEvent = Schema.TaggedUnion({
  // ── Turn lifecycle ─────────────────────────────────────────────────────────────────────────
  TurnStarted: {},
  // `usage` is OPTIONAL: a turn can end without a usage report — an interrupted,
  // aborted, or failed turn need not carry one, so the Pi adapter must be free to
  // translate such a turn without fabricating usage. FE2.2 confirms against real
  // `pi --mode rpc` output and may tighten if Pi always emits it.
  TurnCompleted: { usage: Schema.optionalKey(Usage) },

  // ── Message streaming (deltas) ─────────────────────────────────────────────────────────────
  MessageStarted: { messageId: Schema.NonEmptyString },
  // A fine-grained delta of streamed assistant `text` and/or `reasoning` content.
  // Both are optional as an intentional inclusive superset: the sole producer (the
  // Pi adapter) emits content-bearing deltas, so a contentless delta does not occur
  // in practice; we do NOT enforce "at least one" with a TS-only runtime refinement
  // because the hand-written Swift mirror (FE2.4) could not replicate it, which would
  // split contract semantics across the two implementations. A consumer treats a
  // contentless delta as a no-op.
  MessageDelta: {
    messageId: Schema.NonEmptyString,
    text: Schema.optionalKey(Schema.String),
    reasoning: Schema.optionalKey(Schema.String),
  },
  MessageCompleted: { messageId: Schema.NonEmptyString },

  // ── Tool streaming (deltas) ────────────────────────────────────────────────────────────────
  ToolStarted: { id: Schema.NonEmptyString, name: Schema.NonEmptyString, input: JsonValue },
  ToolProgress: { id: Schema.NonEmptyString, partial: JsonValue },
  ToolCompleted: { id: Schema.NonEmptyString, output: JsonValue, isError: Schema.Boolean },

  // ── Execution state & resilience ───────────────────────────────────────────────────────────
  ExecutionIdle: {},
  RetryScheduled: {
    attempt: NonNegativeInt,
    delayMs: NonNegativeInt,
    error: Schema.String,
  },
  ContextCompacted: {},

  // ── Interactive UI request ─────────────────────────────────────────────────────────────────
  UiRequestRaised: {
    id: Schema.NonEmptyString,
    kind: UiRequestKind,
    prompt: Schema.String,
    options: Schema.optionalKey(Schema.Array(Schema.String)),
  },

  // ── Status / notices ───────────────────────────────────────────────────────────────────────
  // `id` is the OPTIONAL reconciliation key (see {@link NoticeId}). Present when the
  // notice has a stable cross-emission identity — a live notice shares it with the
  // durable `NoticeEntry` of the same logical event so they render once. Omitted for
  // content-derived notices with no such identity (no durable counterpart), so the
  // consumer keys them by arrival sequence and distinct occurrences stay distinct.
  Notice: { id: Schema.optionalKey(NoticeId), level: NoticeLevel, message: Schema.String },
  StatusChanged: { key: Schema.NonEmptyString, text: Schema.String },

  // ── Durable transcript entry ───────────────────────────────────────────────────────────────
  EntryAppended: { entry: TranscriptEntry },
});
export type ExecutionEvent = (typeof ExecutionEvent)["Type"];

/**
 * Input driven INTO an execution. `mode` distinguishes a fresh `prompt`, a `steer`
 * (mid-turn redirection), or a `followUp` after the agent goes idle.
 */
export const ExecutionInput = Schema.Struct({
  text: Schema.String,
  images: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  mode: Schema.Literals(["prompt", "steer", "followUp"]),
});
export type ExecutionInput = (typeof ExecutionInput)["Type"];

/**
 * The answer to a `UiRequestRaised`: a free-form `Value`, a `Confirmed` boolean,
 * or a `Cancelled` (the user dismissed the request).
 */
export const UiAnswer = Schema.TaggedUnion({
  Value: { value: Schema.String },
  Confirmed: { confirmed: Schema.Boolean },
  Cancelled: {},
});
export type UiAnswer = (typeof UiAnswer)["Type"];

/** A response to an outstanding UI request, keyed by the request it answers. */
export const UiResponse = Schema.Struct({
  requestId: Schema.NonEmptyString,
  answer: UiAnswer,
});
export type UiResponse = (typeof UiResponse)["Type"];
