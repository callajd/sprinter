/**
 * The daemon↔client **RPC contract** — an `RpcGroup` (`effect/unstable/rpc`) over
 * the FE2.1 owned domain schemas (architecture §7, D8/D10/D16/D17).
 *
 * The surface speaks ONLY `@sprinter/domain`'s owned, provider-neutral types
 * (INV-PORT / INV-NAMING): no Pi concept and no FE2.2 owned-Pi wire schema
 * (`packages/domain/src/pi/`) appears here — that adapter-internal wire stays out
 * of the client contract. It is **maximally reactive** (D17 / INV-REACTIVE): the
 * work-graph `events` feed and the `sessionEvents` feed are STREAMING RPCs
 * (`RpcSchema.Stream`), not polled request/response. It is **small and stable**
 * (D10 / INV-CONTRACT): every procedure is hand-mirrored in Swift (FE2.4), and the
 * goldens (frozen from this contract) are what keep the two sides in lockstep —
 * change the wire shape without re-freezing them and the Swift decode tests fail.
 *
 * Four models (architecture §7):
 *  1. **snapshot** — request/response, hydrates full state on connect;
 *  2. **events** — streaming work-graph deltas;
 *  3. **commands** — create-workstream-from-plan, control, retry-issue;
 *  4. **session channel** — streaming `sessionEvents`, plus `sessionSend`,
 *     `interrupt`, and `answerUiRequest`.
 */
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import {
  Epic,
  Issue,
  IssueId,
  Job,
  NonNegativeInt,
  Session,
  SessionEvent,
  SessionId,
  SessionInput,
  UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";

// ── Errors (owned, neutral) ─────────────────────────────────────────────────

/** A control/command targeted a workstream the daemon does not know. */
export class WorkstreamNotFound extends Schema.TaggedErrorClass<WorkstreamNotFound>()(
  "WorkstreamNotFound",
  { id: WorkstreamId },
) {}

/** A retry targeted an issue the daemon does not know. */
export class IssueNotFound extends Schema.TaggedErrorClass<IssueNotFound>()("IssueNotFound", {
  id: IssueId,
}) {}

/** A session command or subscription targeted a session the daemon does not know. */
export class SessionNotFound extends Schema.TaggedErrorClass<SessionNotFound>()("SessionNotFound", {
  id: SessionId,
}) {}

/** The submitted plan could not be turned into a workstream. */
export class PlanRejected extends Schema.TaggedErrorClass<PlanRejected>()("PlanRejected", {
  reason: Schema.String,
}) {}

// ── Aggregate contract schemas (composed only of owned domain types) ────────

/**
 * The full owned read-model state, hydrated on connect by the `snapshot` RPC:
 * every planning node (`Workstream ⊃ Epic ⊃ Issue`) plus the execution nodes
 * (`Job`, `Session`). Composed exclusively of FE2.1 domain types (INV-PORT).
 */
export const Snapshot = Schema.Struct({
  workstreams: Schema.Array(Workstream),
  epics: Schema.Array(Epic),
  issues: Schema.Array(Issue),
  jobs: Schema.Array(Job),
  sessions: Schema.Array(Session),
});
export type Snapshot = (typeof Snapshot)["Type"];

/**
 * A single work-graph delta streamed by the `events` RPC (D17 / INV-REACTIVE).
 * Each variant carries the new owned value for one changed node; a client folds
 * these into the {@link Snapshot} it hydrated on connect. Upsert semantics — the
 * carried node replaces any prior node with the same id.
 *
 * v1 model: the work graph is **upsert-only — nodes are never removed**. A node's
 * end of life is a terminal STATUS (`done`/`cancelled`), carried as an ordinary
 * change; it stays in the snapshot. So there is deliberately no `*Removed` delta.
 * If a future model ever drops nodes from the graph, a `*Removed` variant (id
 * only) is a backward-compatible additive change.
 */
export const WorkGraphEvent = Schema.TaggedUnion({
  WorkstreamChanged: { workstream: Workstream },
  EpicChanged: { epic: Epic },
  IssueChanged: { issue: Issue },
  JobChanged: { job: Job },
  SessionChanged: { session: Session },
});
export type WorkGraphEvent = (typeof WorkGraphEvent)["Type"];

/**
 * One streamed `events` item: a {@link WorkGraphEvent} paired with its DURABLE
 * `offset` — the `event_log` row position the delta was journaled at (CE2.0). The
 * stream carries the offset so a reconnecting client can remember its
 * last-seen position and hand it back as the request's `sinceOffset` cursor. Without
 * this envelope the bare `WorkGraphEvent` gave the client no coordinate to resume
 * from, so the cursor was inert; the offset closes that loop end-to-end.
 *
 * The offset is a durable `event_log` coordinate shared by both the replay
 * (`EventLogStore.tail`) and the live tail, so a client CAN resume from any streamed
 * item's offset. The strict guarantee is scoped to the RECONNECT RESUME: a request
 * with `sinceOffset = N` replays only offsets `> N` (the durable `tail(N)`), so a
 * resume never re-delivers what the client already saw. It is NOT a within-stream
 * guarantee: a single live stream can show a harmless boundary overlap (an offset
 * repeating, or momentarily going backwards) where an eager live subscription and
 * the durable replay meet, and because the live fan-out publishes AFTER the durable
 * commit, concurrent writers can interleave so the LIVE feed order is not guaranteed
 * strictly monotonic by offset. All of this is absorbed by upsert idempotency (the
 * carried node replaces any prior of the same id), so a client folds it losslessly.
 */
export const OffsetEvent = Schema.Struct({
  offset: NonNegativeInt,
  event: WorkGraphEvent,
});
export type OffsetEvent = (typeof OffsetEvent)["Type"];

/**
 * One streamed `sessionEvents` item: a durable, transcript-grade {@link SessionEvent}
 * paired with its DURABLE per-session `offset` — the position it was journaled at in the
 * session's durable transcript log. It is the session-channel mirror of
 * {@link OffsetEvent}: the stream carries the offset so a reconnecting client can remember
 * its last-seen position and hand it back as the request's `sinceOffset` cursor to resume
 * strictly after it.
 *
 * The offset is a durable per-session transcript coordinate shared by BOTH the replay
 * (the durable log `tail`) and the live tail (new durable entries fanned out as the session
 * runs), so replay and live are one coordinate space and a client CAN resume from any
 * streamed item's offset. Only the DURABLE, transcript-grade events flow here — the
 * `EntryAppended` records the transcript folds and the reconcilable `Notice`s — NOT the
 * ephemeral streaming deltas (message/tool partials, turn lifecycle), which are not
 * journaled and carry no durable offset. The strict `> sinceOffset` guarantee is scoped to
 * the RECONNECT RESUME (the durable `tail`); a single live stream can show a harmless
 * boundary overlap where the durable replay and the live tail meet (an offset repeating),
 * absorbed by the consumer's id-keyed transcript reconciliation (a durable entry replaces
 * the item its live counterpart built) exactly as `OffsetEvent` overlap is absorbed by
 * upsert idempotency.
 */
export const OffsetSessionEvent = Schema.Struct({
  offset: NonNegativeInt,
  event: SessionEvent,
});
export type OffsetSessionEvent = (typeof OffsetSessionEvent)["Type"];

/**
 * The plan the `createWorkstreamFromPlan` command turns into a new workstream:
 * a human-readable name, the bound repository (repo-scoped per D14), and the
 * free-form spec text driving planning.
 */
export const WorkstreamPlan = Schema.Struct({
  name: Schema.NonEmptyString,
  repo: Schema.NonEmptyString,
  spec: Schema.String,
});
export type WorkstreamPlan = (typeof WorkstreamPlan)["Type"];

/** The lifecycle action a `control` command applies to a workstream. */
export const ControlAction = Schema.Literals(["start", "pause", "resume", "cancel"]);
export type ControlAction = (typeof ControlAction)["Type"];

// ── Procedures ──────────────────────────────────────────────────────────────
//
// Each procedure is a named `Rpc` so its per-procedure payload/success/error
// types are preserved (the group's `requests` map erases them to a union). The
// `events` and `sessionEvents` feeds are `stream: true`, so their success/error
// is wrapped in `RpcSchema.Stream` — they are streaming, not polled
// (INV-REACTIVE).

// (1) snapshot — request/response full-state hydration on connect.
export const snapshot = Rpc.make("snapshot", { success: Snapshot });

// (2) events — streaming work-graph deltas (INV-REACTIVE). Each streamed item is
// an {@link OffsetEvent} — the delta PLUS its durable `event_log` offset — so the
// client can track its last-seen position (CE2.0). The request payload
// carries an OPTIONAL `sinceOffset` resume cursor: an events request with NO
// `sinceOffset` (a PRESENT but empty `{}` payload) replays from the log ORIGIN,
// present resumes STRICTLY AFTER that offset, over the daemon's existing
// `resyncFrom(offset)` primitive (CE1.2). Note the payload OBJECT itself is required
// (the events payload schema is a `Struct`): the canonical client sends `{}` for `.events({})` —
// an omitted `payload` key on the wire (decoding to `undefined`) is NOT a valid v3
// request. The success offset and the request cursor are the SAME coordinate: a
// client feeds a streamed item's `offset` straight back as the next `sinceOffset`.
export const events = Rpc.make("events", {
  payload: { sinceOffset: Schema.optionalKey(NonNegativeInt) },
  success: OffsetEvent,
  stream: true,
});

// (3) commands — create-workstream-from-plan, control, retry-issue.
export const createWorkstreamFromPlan = Rpc.make("createWorkstreamFromPlan", {
  payload: { plan: WorkstreamPlan },
  success: WorkstreamId,
  error: PlanRejected,
});
export const control = Rpc.make("control", {
  payload: { workstreamId: WorkstreamId, action: ControlAction },
  error: WorkstreamNotFound,
});
export const retryIssue = Rpc.make("retryIssue", {
  payload: { issueId: IssueId },
  error: IssueNotFound,
});

// (4) session channel — streaming events + send/interrupt/answer.
//
// `sessionEvents` replays a session's DURABLE transcript then live-tails it — the
// session-channel mirror of `events`. Each streamed item is an
// {@link OffsetSessionEvent} — a durable transcript-grade {@link SessionEvent} PLUS its
// durable per-session offset — so the client can track its last-seen position and resume.
// The request payload carries an OPTIONAL `sinceOffset` resume cursor: a request with NO
// `sinceOffset` (a PRESENT but empty payload beyond `sessionId`) replays the session's
// transcript from the ORIGIN, present resumes STRICTLY AFTER that offset. A SETTLED session
// replays its durable transcript and the stream COMPLETES (no longer `SessionNotFound`); a
// LIVE session replays then tails new durable entries; a session that never existed (no
// durable transcript AND no live handle) is `SessionNotFound`. Only `sessionEvents` gains
// durable replay — `sessionSend`/`interrupt`/`answerUiRequest` stay LIVE-only (a settled
// session is read-only, so they still fail `SessionNotFound`).
export const sessionEvents = Rpc.make("sessionEvents", {
  payload: { sessionId: SessionId, sinceOffset: Schema.optionalKey(NonNegativeInt) },
  success: OffsetSessionEvent,
  error: SessionNotFound,
  stream: true,
});
export const sessionSend = Rpc.make("sessionSend", {
  payload: { sessionId: SessionId, input: SessionInput },
  error: SessionNotFound,
});
export const interrupt = Rpc.make("interrupt", {
  payload: { sessionId: SessionId },
  error: SessionNotFound,
});
export const answerUiRequest = Rpc.make("answerUiRequest", {
  payload: { sessionId: SessionId, response: UiResponse },
  error: SessionNotFound,
});

// ── The RPC group (contract) ─────────────────────────────────────────────────

/**
 * The RPC contract group — daemon↔client. Carries the four models
 * (architecture §7) as procedures.
 */
export const SprinterRpc = RpcGroup.make(
  snapshot,
  events,
  createWorkstreamFromPlan,
  control,
  retryIssue,
  sessionEvents,
  sessionSend,
  interrupt,
  answerUiRequest,
);
