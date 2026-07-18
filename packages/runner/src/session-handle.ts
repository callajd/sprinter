/**
 * `SessionHandle` — the owned, provider-neutral surface of one live Pi session
 * (Track A, task AE1.2). This is where the owned/foreign boundary is enforced
 * (INV-BOUNDARY): the {@link PiTransport} substrate (AE1.1) traffics in Pi wire
 * types, and this module translates them into our neutral session model so
 * NOTHING above the runner ever sees a Pi type. The handle's public surface
 * (`events` / `send` / `interrupt` / `result` / `answerUi`) is expressed ONLY in
 * `SessionEvent` / `SessionInput` / `UiResponse` from `@sprinter/domain`.
 *
 * It is built ON the merged `PiTransport` (`make` / `events` / `request` /
 * `send`) — it does NOT reimplement the spawn/NDJSON/correlation substrate.
 *
 * Wiring-constraint carried from AE1.1 (#18): the transport's `events` stream is
 * single-consumer and unbounded by design (bounding it there would stall the
 * shared stdout pump and block response correlation). This handle is that single
 * consumer: it drains `events` PROMPTLY into a bounded, sliding-overflow `PubSub`
 * and owns the bounding / backpressure / fan-out policy for delivering
 * `SessionEvent` to clients (see {@link make}).
 */
import { Cause, Deferred, Effect, Exit, Option, PubSub, Schema, Stream, Take } from "effect";
import type { PlatformError } from "effect/PlatformError";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import type { NoticeLevel, SessionEvent, SessionInput, Usage, UiResponse } from "@sprinter/domain";
import type {
  PiAgentMessage,
  PiAssistantMessage,
  PiAssistantMessageEvent,
  PiImageContent,
  PiRpcCommand,
  PiRpcExtensionUIRequest,
  PiRpcExtensionUIResponse,
  PiSessionEntry,
} from "@sprinter/domain/pi/wire";
import { make as makeTransport } from "./pi-transport.ts";
import type { PiProcessConfig, PiServerEvent } from "./pi-transport.ts";
import type { PiRpcError, PiTransportError } from "./pi-transport.ts";

// ============================================================================
// Terminal outcome
// ============================================================================

/**
 * The neutral terminal outcome of a session, resolved by {@link SessionHandle.result}.
 * `Completed` when `pi`'s output ended cleanly; `Failed` when the transport tore
 * down with an error (the message is the transport's neutral detail — never a Pi
 * wire value).
 */
export const SessionResult = Schema.TaggedUnion({
  Completed: {},
  Failed: { error: Schema.String },
});
export type SessionResult = (typeof SessionResult)["Type"];

// ============================================================================
// Public handle
// ============================================================================

/**
 * One live session, expressed entirely in owned neutral types. Obtained from
 * {@link make}; `Scope`-managed (the underlying `pi` process is killed on scope
 * close). {@link events} is a fan-out stream — each subscription is independent.
 */
export interface SessionHandle {
  /** The operating-system process id of the underlying `pi` process. */
  readonly pid: ChildProcessSpawner.ProcessId;
  /**
   * The live stream of owned {@link SessionEvent}s — maximally reactive (D17):
   * fine-grained message/tool deltas AND the durable `EntryAppended` entries.
   * Fails with {@link PiTransportError} if the transport tears down; ends when
   * `pi`'s output closes. Each subscription is a fresh, independent consumer.
   */
  readonly events: Stream.Stream<SessionEvent, PiTransportError>;
  /** Drive a {@link SessionInput} (prompt / steer / followUp) into the session. */
  readonly send: (input: SessionInput) => Effect.Effect<void, PiRpcError | PiTransportError>;
  /** Abort the in-flight turn. */
  readonly interrupt: Effect.Effect<void, PiRpcError | PiTransportError>;
  /** Answer an outstanding UI request raised via a `UiRequestRaised` event. */
  readonly answerUi: (response: UiResponse) => Effect.Effect<void>;
  /** Resolve the session's terminal outcome (awaits completion). */
  readonly result: Effect.Effect<SessionResult>;
}

// ============================================================================
// Translation: Pi server events → owned SessionEvent (INV-BOUNDARY)
// ============================================================================

/** A Pi user message (the `role: "user"` variant of {@link PiAgentMessage}). */
type PiUserMessage = Extract<PiAgentMessage, { readonly role: "user" }>;

/**
 * Pi wire messages carry no top-level message id; only assistant messages carry
 * an optional provider `responseId`, and every message carries a `timestamp`.
 * We derive a stable, non-fabricated correlation id: the `responseId` when
 * present, else the message `timestamp` (stable across a message's streaming
 * updates in Pi). This is a real Pi value, never a synthesized one.
 */
const messageId = (message: PiAgentMessage): string =>
  message.role === "assistant" && message.responseId !== undefined && message.responseId.length > 0
    ? message.responseId
    : String(message.timestamp);

/** Concatenate the `text` blocks of an assistant message's content. */
const assistantText = (message: PiAssistantMessage): string =>
  message.content.reduce((acc, block) => (block.type === "text" ? acc + block.text : acc), "");

/** Concatenate the `thinking` blocks of an assistant message's content. */
const assistantReasoning = (message: PiAssistantMessage): string =>
  message.content.reduce(
    (acc, block) => (block.type === "thinking" ? acc + block.thinking : acc),
    "",
  );

/** Extract the text of a user message (raw string, or the `text` content blocks). */
const userText = (message: PiUserMessage): string =>
  typeof message.content === "string"
    ? message.content
    : message.content.reduce((acc, block) => (block.type === "text" ? acc + block.text : acc), "");

/** Translate Pi's token accounting into the neutral {@link Usage}. */
const usageOf = (message: PiAssistantMessage): Usage => ({
  inputTokens: message.usage.input,
  outputTokens: message.usage.output,
  cacheReadTokens: message.usage.cacheRead,
  cacheWriteTokens: message.usage.cacheWrite,
});

/** Map Pi's notify severity onto the neutral {@link NoticeLevel}. */
const notifyLevel = (kind: "info" | "warning" | "error" | undefined): NoticeLevel =>
  kind === "warning" ? "warn" : kind === "error" ? "error" : "info";

/**
 * The fine-grained streaming sub-protocol carried by `message_update`. Only the
 * content deltas synthesize a neutral `MessageDelta`; the start/end/toolcall
 * framing carries no neutral counterpart (the durable record arrives via
 * `entry_appended`).
 */
const translateAssistantEvent = (
  id: string,
  event: PiAssistantMessageEvent,
): ReadonlyArray<SessionEvent> => {
  switch (event.type) {
    case "text_delta":
      return [{ _tag: "MessageDelta", messageId: id, text: event.delta }];
    case "thinking_delta":
      return [{ _tag: "MessageDelta", messageId: id, reasoning: event.delta }];
    case "start":
    case "text_start":
    case "text_end":
    case "thinking_start":
    case "thinking_end":
    case "toolcall_start":
    case "toolcall_delta":
    case "toolcall_end":
    case "done":
    case "error":
      return [];
  }
};

/**
 * Translate a durable Pi session entry into the neutral `EntryAppended`. Only
 * `message` entries carry a neutral `TranscriptEntry`; the other entry kinds
 * (compaction, level/model/name changes, labels, custom) have no transcript-grade
 * counterpart and are dropped.
 */
const translateEntry = (entry: PiSessionEntry): ReadonlyArray<SessionEvent> => {
  if (entry.type !== "message") return [];
  const message = entry.message;
  switch (message.role) {
    case "user":
      return [
        {
          _tag: "EntryAppended",
          entry: { _tag: "UserMessage", id: entry.id, text: userText(message) },
        },
      ];
    case "assistant": {
      const reasoning = assistantReasoning(message);
      const base = {
        _tag: "AssistantMessage" as const,
        id: entry.id,
        text: assistantText(message),
      };
      // The durable transcript must record the tool calls the assistant issued —
      // they live as `toolCall` content blocks on the message (D17: EntryAppended
      // is the transcript-grade record, and a client reconciling from it would
      // otherwise get orphan ToolResult entries with no matching ToolCall). Emit
      // the AssistantMessage entry, then one durable ToolCall entry per block.
      const toolCalls = message.content.reduce<ReadonlyArray<SessionEvent>>(
        (acc, block) =>
          block.type === "toolCall"
            ? [
                ...acc,
                {
                  _tag: "EntryAppended",
                  entry: {
                    _tag: "ToolCall",
                    id: block.id,
                    name: block.name,
                    input: block.arguments,
                  },
                },
              ]
            : acc,
        [],
      );
      return [
        { _tag: "EntryAppended", entry: reasoning.length > 0 ? { ...base, reasoning } : base },
        ...toolCalls,
      ];
    }
    case "toolResult":
      return [
        {
          _tag: "EntryAppended",
          entry: {
            _tag: "ToolResult",
            id: message.toolCallId,
            output: message.content,
            isError: message.isError,
          },
        },
      ];
  }
};

/** Translate an interactive UI request into a neutral event. */
const translateUiRequest = (event: PiRpcExtensionUIRequest): ReadonlyArray<SessionEvent> => {
  switch (event.method) {
    case "select":
      return [
        {
          _tag: "UiRequestRaised",
          id: event.id,
          kind: "select",
          prompt: event.title,
          options: event.options,
        },
      ];
    case "confirm":
      return [{ _tag: "UiRequestRaised", id: event.id, kind: "confirm", prompt: event.message }];
    case "input":
      return [{ _tag: "UiRequestRaised", id: event.id, kind: "input", prompt: event.title }];
    case "editor":
      return [{ _tag: "UiRequestRaised", id: event.id, kind: "editor", prompt: event.title }];
    case "notify":
      // The notify request's own id is the notice's reconciliation key (NoticeId):
      // the same logical notify keeps its key across a live/durable pair. NOTE: a
      // future durable `NoticeEntry` producer for this notice MUST reproduce this
      // EXACT key derivation (`event.id`) or the live+durable pair won't share a key.
      return [
        {
          _tag: "Notice",
          id: event.id,
          level: notifyLevel(event.notifyType),
          message: event.message,
        },
      ];
    case "setStatus":
      return [{ _tag: "StatusChanged", key: event.statusKey, text: event.statusText ?? "" }];
    case "setTitle":
      return [{ _tag: "StatusChanged", key: "title", text: event.title }];
    case "setWidget":
    case "set_editor_text":
      return [];
  }
};

/**
 * The boundary translation: a foreign {@link PiServerEvent} → zero or more owned
 * {@link SessionEvent}s. TOTAL and exhaustive via discriminated matching on the
 * wire `type` tag — no `as` / `!` / `any` (INV-NOCAST). The switch enumerates
 * every mirrored variant with no `default`, so a newly-mirrored Pi variant is a
 * compile error here (`noImplicitReturns`) rather than a silent drop.
 */
export const translateServerEvent = (event: PiServerEvent): ReadonlyArray<SessionEvent> => {
  switch (event.type) {
    // ── Agent lifecycle: no neutral counterpart (turn lifecycle + settle carry it) ──
    case "agent_start":
    case "agent_end":
      return [];
    // ── Turn lifecycle ──────────────────────────────────────────────────────
    case "turn_start":
      return [{ _tag: "TurnStarted" }];
    case "turn_end": {
      const usage = event.message.role === "assistant" ? usageOf(event.message) : undefined;
      return [usage !== undefined ? { _tag: "TurnCompleted", usage } : { _tag: "TurnCompleted" }];
    }
    // ── Message streaming ───────────────────────────────────────────────────
    case "message_start":
      return event.message.role === "assistant"
        ? [{ _tag: "MessageStarted", messageId: messageId(event.message) }]
        : [];
    case "message_update":
      return translateAssistantEvent(messageId(event.message), event.assistantMessageEvent);
    case "message_end":
      return event.message.role === "assistant"
        ? [{ _tag: "MessageCompleted", messageId: messageId(event.message) }]
        : [];
    // ── Tool streaming ──────────────────────────────────────────────────────
    case "tool_execution_start":
      return [
        { _tag: "ToolStarted", id: event.toolCallId, name: event.toolName, input: event.args },
      ];
    case "tool_execution_update":
      return [{ _tag: "ToolProgress", id: event.toolCallId, partial: event.partialResult }];
    case "tool_execution_end":
      return [
        {
          _tag: "ToolCompleted",
          id: event.toolCallId,
          output: event.result,
          isError: event.isError,
        },
      ];
    // ── Session state & resilience ──────────────────────────────────────────
    case "agent_settled":
      return [{ _tag: "SessionIdle" }];
    case "queue_update":
    case "compaction_start":
      return [];
    case "compaction_end":
      return event.aborted ? [] : [{ _tag: "ContextCompacted" }];
    case "entry_appended":
      return translateEntry(event.entry);
    case "session_info_changed":
      return event.name !== undefined
        ? [{ _tag: "StatusChanged", key: "session_name", text: event.name }]
        : [];
    case "thinking_level_changed":
      return [{ _tag: "StatusChanged", key: "thinking_level", text: event.level }];
    case "auto_retry_start":
      return [
        {
          _tag: "RetryScheduled",
          attempt: event.attempt,
          // Pi reports `delayMs` as a possibly-fractional number; the neutral
          // model is a non-negative integer of milliseconds.
          delayMs: Math.trunc(event.delayMs),
          error: event.errorMessage,
        },
      ];
    case "auto_retry_end":
      // A failed retry-end means the agent gave up — always surface it, even when
      // Pi omits `finalError` (otherwise a client watching for give-up sees
      // nothing). A successful retry-end needs no neutral signal.
      return event.success
        ? []
        : [
            {
              _tag: "Notice",
              // No stable cross-emission identity: `auto_retry_end` carries only
              // `attempt`, which is NOT occurrence-unique — two independent retry
              // sequences can each give up at the same attempt number. So OMIT the
              // (optional) NoticeId; the consumer keys this by arrival sequence and
              // distinct give-ups stay distinct. There is no durable counterpart to
              // reconcile with. (A future durable producer would need a genuinely
              // occurrence-unique key reproduced identically on both paths.)
              level: "error",
              message: event.finalError ?? `retry failed after ${event.attempt} attempt(s)`,
            },
          ];
    // ── Interactive UI requests ─────────────────────────────────────────────
    case "extension_ui_request":
      return translateUiRequest(event);
    // ── Extension errors ────────────────────────────────────────────────────
    case "extension_error":
      return [
        {
          _tag: "Notice",
          // No stable cross-emission identity: `extension_error` carries only
          // extensionPath+event, which is NOT occurrence-unique — the same extension
          // can fail the same event more than once. So OMIT the (optional) NoticeId;
          // the consumer keys this by arrival sequence and distinct failures stay
          // distinct. There is no durable counterpart to reconcile with. (A future
          // durable producer would need an occurrence-unique key reproduced on both.)
          level: "error",
          message: `extension ${event.extensionPath} failed handling ${event.event}: ${event.error}`,
        },
      ];
  }
};

// ============================================================================
// Encoding: owned inputs → Pi commands (INV-BOUNDARY)
// ============================================================================

/**
 * Encode a base64-carrying neutral image string into Pi's image content block.
 * The neutral model carries only the payload; when it is a `data:` URL we recover
 * the media type from it, otherwise we default to PNG (the payload is passed
 * through unchanged either way).
 */
const encodeImage = (image: string): PiImageContent => {
  const match = /^data:([^;,]+);base64,(.*)$/s.exec(image);
  return match !== null
    ? { type: "image", data: match[2] ?? "", mimeType: match[1] ?? "image/png" }
    : { type: "image", data: image, mimeType: "image/png" };
};

/**
 * Encode a neutral {@link SessionInput} into the matching Pi command. The three
 * neutral modes map to Pi's `prompt` / `steer` / `follow_up`; the transport
 * assigns the correlation `id`, so none is set here.
 */
export const encodeInput = (input: SessionInput): PiRpcCommand => {
  const images = input.images?.map(encodeImage);
  const body = images !== undefined ? { message: input.text, images } : { message: input.text };
  switch (input.mode) {
    case "prompt":
      return { type: "prompt", ...body };
    case "steer":
      return { type: "steer", ...body };
    case "followUp":
      return { type: "follow_up", ...body };
  }
};

/** Encode a neutral {@link UiResponse} into Pi's extension UI response. */
export const encodeUiResponse = (response: UiResponse): PiRpcExtensionUIResponse => {
  const answer = response.answer;
  switch (answer._tag) {
    case "Value":
      return { type: "extension_ui_response", id: response.requestId, value: answer.value };
    case "Confirmed":
      return { type: "extension_ui_response", id: response.requestId, confirmed: answer.confirmed };
    case "Cancelled":
      return { type: "extension_ui_response", id: response.requestId, cancelled: true };
  }
};

// ============================================================================
// Construction
// ============================================================================

/**
 * The fan-out buffer capacity for a session's event stream. Generous: overflow
 * (which drops the OLDEST buffered events, sliding strategy) only occurs under
 * pathological subscriber slowness; a client reconciles any gap via
 * snapshot-on-connect + live-subscribe (D4).
 *
 * The same size is used as the REPLAY window: each new subscriber first receives
 * the retained recent events (up to this bound) and then the live tail, so a
 * subscriber never races the pump for events (or the terminal) produced just
 * before it attached. This is a bounded recent-history window, additive to — not
 * a replacement for — snapshot-based history (D4).
 */
const EVENTS_CAPACITY = 1024;

/** The terminal outcome of a transport-error teardown, as a neutral result. */
const failedResult = (cause: Cause.Cause<PiTransportError>): SessionResult => {
  const failure = Cause.findErrorOption(cause);
  return {
    _tag: "Failed",
    error: Option.isSome(failure) ? failure.value.detail : Cause.pretty(cause),
  };
};

/**
 * Spawn a `pi --mode rpc` session and expose it as a neutral {@link SessionHandle}.
 * Built on {@link makeTransport}; `Scope`-managed (closing the scope kills `pi`).
 *
 * The handle is the single, prompt consumer of the transport's unbounded `events`
 * stream (#18 wiring-constraint): a scoped pump drains it, translates each Pi
 * event at the boundary, and publishes into a bounded `PubSub` with the SLIDING
 * overflow strategy — so publishing never applies backpressure onto the shared
 * stdout pump (which would stall response correlation), yet the buffer stays
 * bounded. `events` fans out via independent `PubSub` subscriptions; the pump's
 * terminal (clean end or transport failure) propagates to every subscriber and
 * resolves {@link SessionHandle.result}.
 */
export const make = (
  config?: PiProcessConfig,
): Effect.Effect<SessionHandle, PlatformError, ChildProcessSpawner.ChildProcessSpawner | Scope> =>
  Effect.gen(function* () {
    const transport = yield* makeTransport(config);
    const pubsub = yield* PubSub.sliding<Take.Take<SessionEvent, PiTransportError>>({
      capacity: EVENTS_CAPACITY,
      replay: EVENTS_CAPACITY,
    });
    const result = yield* Deferred.make<SessionResult>();

    // The single, prompt consumer of the transport's `events` stream: translate
    // at the boundary and fan out. Publishing to a sliding PubSub never blocks,
    // so this never backs up the shared stdout pump.
    const pump = Effect.matchCauseEffect(
      Stream.runForEach(transport.events, (event) =>
        Effect.forEach(
          translateServerEvent(event),
          (translated) => PubSub.publish(pubsub, [translated]),
          { discard: true },
        ),
      ),
      {
        onFailure: (cause: Cause.Cause<PiTransportError>) =>
          PubSub.publish(pubsub, Exit.failCause(cause)).pipe(
            Effect.andThen(Deferred.succeed(result, failedResult(cause))),
          ),
        onSuccess: () =>
          PubSub.publish(pubsub, Exit.void).pipe(
            Effect.andThen(Deferred.succeed(result, { _tag: "Completed" })),
          ),
      },
    );
    yield* Effect.forkScoped(pump);

    const send = (input: SessionInput): Effect.Effect<void, PiRpcError | PiTransportError> =>
      Effect.asVoid(transport.request(encodeInput(input)));

    const interrupt: Effect.Effect<void, PiRpcError | PiTransportError> = Effect.asVoid(
      transport.request({ type: "abort" }),
    );

    const answerUi = (response: UiResponse): Effect.Effect<void> =>
      transport.send(encodeUiResponse(response));

    return {
      pid: transport.pid,
      events: Stream.fromPubSubTake(pubsub),
      send,
      interrupt,
      answerUi,
      result: Deferred.await(result),
    };
  });
