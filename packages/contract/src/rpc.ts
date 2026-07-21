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
  Agent,
  Epic,
  Issue,
  IssueId,
  Job,
  NonNegativeInt,
  Session,
  SessionEvent,
  SessionId,
  SessionInput,
  StoreGenerationId,
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

/**
 * A resume cursor (the `sinceOffset` inside `events`' or `sessionEvents`'
 * {@link ResumeContext}) does NOT belong to
 * the daemon's current store generation, so there is no incremental resume from it:
 * the client must throw away everything it retained and re-hydrate from `snapshot`.
 *
 * The daemon's store never migrates (INV-FRESH): bumping its schema version DROPS
 * the database and recreates it, which restarts the durable `event_log` /
 * `session_event_log` offsets at `1` and destroys every row the client's retained
 * state was built from. The local app and the local daemon run together, so this
 * happens UNDER a live client — one that reconnects holding a cursor from the
 * previous generation and a retained snapshot full of entities that no longer exist.
 *
 * Neither of the two things a daemon could do WITHOUT this error is correct, which
 * is why it is a contract error rather than an inference:
 *
 * - Honour the cursor as a strict `> sinceOffset` tail ⇒ the client receives nothing
 *   and reports no error, staying silently blind until the new log grows past its
 *   stale high-water mark.
 * - Quietly replay from the origin instead ⇒ the client's contiguous-offset cursor is
 *   still at the stale value, so it discards every replayed offset as "already seen",
 *   never advances (not even for new live events), and re-reads the whole log on every
 *   reconnect — a worse failure than the stall, and one the client cannot detect.
 *
 * Neither can be fixed on the daemon side alone, because the real damage is the
 * CLIENT's retained state: the delta model is upsert-only (there is no `*Removed`),
 * so no stream of deltas can ever remove an entity the reset destroyed. The epoch
 * must therefore be EXPLICIT — the daemon says "resync", and the client answers by
 * dropping its retained state and cursor and taking the subscribe-around-`snapshot`
 * path it uses on a first connect.
 *
 * **The detection is an IDENTITY comparison, not an offset inference.** A cursor
 * beyond the log's extent is a SUFFICIENT symptom but never a NECESSARY one: once a
 * new generation's log outgrows a stale cursor, `sinceOffset <= maxOffset` holds and
 * an extent check sees nothing wrong. So every cursor-bearing request carries the
 * {@link StoreGenerationId} it was minted under — the one the client read off
 * {@link Snapshot.generation} — INSEPARABLY, as one {@link ResumeContext}, and the
 * daemon refuses any resume whose generation differs from its own. "Absent" is not a
 * case the daemon has to handle, because it is not a representable request: a cursor
 * without its generation cannot be expressed. The extent check remains as a cheap
 * secondary (a cursor ahead of the log is impossible even WITHIN one generation).
 *
 * - `sinceOffset` — the cursor the client sent, echoed back so the failure is
 *   self-describing in a log.
 * - `maxOffset` — the highest offset the daemon's log currently holds (`0` when it is
 *   empty). For an extent refusal this is what the cursor exceeded; for a generation
 *   refusal it is simply the current log's extent.
 * - `generation` — the daemon's CURRENT {@link StoreGenerationId}. A client cannot
 *   adopt it as a cursor context (its retained state is from the dead generation, so
 *   there is nothing valid to pair it with); it is what makes the failure diagnosable
 *   and lets a client confirm the generation moved rather than guess.
 */
export class ResyncRequired extends Schema.TaggedErrorClass<ResyncRequired>()("ResyncRequired", {
  sinceOffset: NonNegativeInt,
  maxOffset: NonNegativeInt,
  generation: StoreGenerationId,
}) {}

/**
 * A client's RESUME CONTEXT: the durable cursor it wants to continue strictly after,
 * TOGETHER with the {@link StoreGenerationId} that cursor is a coordinate in. It is
 * the optional half of both feed payloads (`events`, `sessionEvents`) — ABSENT means
 * "replay from the ORIGIN", PRESENT means "resume", and there is no third state.
 *
 * **Why one value and not two optional fields (INV-SUM).** A cursor is meaningless
 * outside the generation it was minted in: {@link ResyncRequired}'s docstring explains
 * why an offset alone cannot be validated. Modelling `sinceOffset` and `generation` as
 * two INDEPENDENT optional keys makes "a cursor with no generation" and "a generation
 * with no cursor" representable, so the daemon has to REJECT them at runtime — and a
 * runtime rejection is only as good as the branch it lives on. That is exactly how the
 * guard was bypassable: an offset of `0` reads as "the origin" numerically, so the
 * generation comparison was skipped for it, and a request carrying a DEAD generation
 * with `sinceOffset: 0` was accepted as a first connect. A client can genuinely reach
 * that shape — a contiguous-prefix cursor that never advanced past `0` because the
 * first delta of an attempt arrived out of order is still a durable resume point.
 *
 * Pairing them removes the question instead of adding a fourth check. The PRESENCE of
 * this value — not the VALUE of an offset — is what distinguishes an origin request
 * from a resume, so the generation is compared unconditionally whenever it is present,
 * `sinceOffset: 0` included, and there is no numeric special case left to bypass.
 *
 * - `sinceOffset` — the durable offset to resume STRICTLY AFTER (`0` is legal and
 *   means "everything in THIS generation"; it is not an exemption from the check).
 * - `generation` — the {@link Snapshot.generation} the client retained alongside the
 *   state it is folding onto.
 */
export const ResumeContext = Schema.Struct({
  sinceOffset: NonNegativeInt,
  generation: StoreGenerationId,
});
export type ResumeContext = typeof ResumeContext.Type;

// ── Aggregate contract schemas (composed only of owned domain types) ────────

/**
 * The full owned state, hydrated on connect by the `snapshot` RPC: every planning
 * node (`Workstream ⊃ Epic ⊃ Issue`), the execution nodes (`Job`, `Session`), and
 * the REGISTRY layer (`agents`). Composed exclusively of FE2.1 domain types
 * (INV-PORT).
 *
 * `agents` is the whole append-only registry, NOT a per-repository slice: an
 * `Agent` is global (it names no repository), so "the agents used in this repo" is
 * a fold a client computes over that repo's executions — never a list carried here
 * (INV-DERIVED). Retired and superseded revisions are included, because a
 * historical node may still resolve to one.
 *
 * `generation` is the identity of the STORE GENERATION this state was read from —
 * the coordinate space its durable offsets live in. It is carried here because a
 * snapshot is where a client's resume context BEGINS: the client retains it and
 * hands it back on every cursor-bearing `events` / `sessionEvents` request, so the
 * daemon can refuse a cursor from a generation it destroyed instead of resuming the
 * client incrementally against a log the cursor never belonged to (see
 * {@link ResyncRequired}). Nothing may parse or order it — equality only.
 */
export const Snapshot = Schema.Struct({
  workstreams: Schema.Array(Workstream),
  epics: Schema.Array(Epic),
  issues: Schema.Array(Issue),
  jobs: Schema.Array(Job),
  sessions: Schema.Array(Session),
  agents: Schema.Array(Agent),
  generation: StoreGenerationId,
});
export type Snapshot = (typeof Snapshot)["Type"];

/**
 * A single work-graph delta streamed by the `events` RPC (D17 / INV-REACTIVE).
 * Each variant carries the new owned value for one changed node; a client folds
 * these into the {@link Snapshot} it hydrated on connect. Upsert semantics — the
 * carried node replaces any prior node with the same id.
 *
 * The work graph is **upsert-only — nodes are never removed**. A node's
 * end of life is a terminal STATUS (`done`/`cancelled`), carried as an ordinary
 * change; it stays in the snapshot. So there is deliberately no `*Removed` delta.
 * If a future model ever drops nodes from the graph, a `*Removed` variant (id
 * only) is a backward-compatible additive change.
 *
 * `AgentChanged` carries the REGISTRY layer under that same upsert-only model, and
 * the fit is exact rather than incidental: the registry is APPEND-ONLY, so a delta
 * is always either a brand-new revision or a retirement stamp on an existing one —
 * never a removal. There is deliberately no `AgentRemoved`, and no delete anywhere
 * on this contract.
 */
export const WorkGraphEvent = Schema.TaggedUnion({
  WorkstreamChanged: { workstream: Workstream },
  EpicChanged: { epic: Epic },
  IssueChanged: { issue: Issue },
  JobChanged: { job: Job },
  SessionChanged: { session: Session },
  AgentChanged: { agent: Agent },
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
 * One streamed `sessionEvents` item: a {@link SessionEvent} paired with an OPTIONAL DURABLE
 * per-session `offset`. This ONE channel serves BOTH session modalities —
 * live driving AND settled-transcript replay — so `offset` is optional, the one deliberate
 * divergence from the work-graph {@link OffsetEvent} (whose offset is ALWAYS present because
 * every work-graph event is durable). Sessions carry a SUPERSET: the durable transcript
 * grade PLUS the ephemeral live deltas. Semantics of `offset`:
 *
 * - **PRESENT** ⇒ the event is DURABLE and transcript-grade (an `EntryAppended` record or a
 *   reconcilable `Notice`): it was journaled to the session's durable transcript log at this
 *   offset, it is REPLAYABLE, and it ADVANCES the reconnect resume cursor. The offset is a
 *   durable per-session coordinate shared by BOTH the replay (the durable log `tail`) and the
 *   live tail (new durable entries fanned out as the session runs), so replay and live are one
 *   coordinate space and a client CAN resume from any offset-bearing item's offset.
 * - **ABSENT** ⇒ the event is an EPHEMERAL live delta (message/tool partials, turn lifecycle,
 *   `UiRequestRaised`, status/retry/compaction): it is forwarded to the consumer to drive the
 *   LIVE modality, but it is NEVER persisted, carries no durable coordinate, and NEVER moves
 *   the resume cursor. A reconnect replays only the durable (offset-bearing) prefix, so an
 *   offset-less delta cannot be — and need not be — resumed.
 *
 * The strict `> sinceOffset` guarantee is scoped to the RECONNECT RESUME (the durable `tail`),
 * which sees only offset-bearing events; a single live stream can show a harmless boundary
 * overlap where the durable replay and the live tail meet (a durable offset repeating),
 * absorbed by the consumer's id-keyed transcript reconciliation (a durable entry replaces the
 * item its live counterpart built) exactly as `OffsetEvent` overlap is absorbed by upsert
 * idempotency.
 */
export const OffsetSessionEvent = Schema.Struct({
  offset: Schema.optionalKey(NonNegativeInt),
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
// carries an OPTIONAL {@link ResumeContext}: an events request with NO `resume` (a
// PRESENT but empty `{}` payload) replays from the log ORIGIN, present resumes
// STRICTLY AFTER `resume.sinceOffset`, over the daemon's existing
// `resyncFrom(offset)` primitive (CE1.2). Note the payload OBJECT itself is required
// (the events payload schema is a `Struct`): the canonical client sends `{}` for `.events({})` —
// an omitted `payload` key on the wire (decoding to `undefined`) is NOT a valid events
// request. The success offset and the request cursor are the SAME coordinate: a
// client feeds a streamed item's `offset` straight back as the next
// `resume.sinceOffset`.
//
// A cursor is meaningful ONLY inside the store generation it was minted in, so the
// cursor and its `generation` are ONE optional value rather than two independent
// optional keys — the absence of `resume` IS the origin request, and its presence
// always carries both coordinates. See {@link ResumeContext} for why that structure
// (not a runtime pairing check) is what makes the guard un-bypassable.
//
// The one error is {@link ResyncRequired}: the cursor does not belong to the daemon's
// CURRENT store generation (the store was dropped and recreated under the client),
// so no incremental resume exists and the client must discard its retained state and
// re-hydrate from `snapshot`. It is a typed contract error precisely because the
// daemon cannot repair it alone — see `ResyncRequired`'s docstring.
export const events = Rpc.make("events", {
  payload: {
    resume: Schema.optionalKey(ResumeContext),
  },
  success: OffsetEvent,
  error: ResyncRequired,
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
// session-channel mirror of `events`, but a UNIFIED dual-modality feed. Each
// streamed item is an {@link OffsetSessionEvent} — a {@link SessionEvent} PLUS an OPTIONAL
// durable per-session offset. Durable, transcript-grade events (`EntryAppended`/`Notice`)
// carry the offset the client feeds back to resume; ephemeral live deltas (turn lifecycle,
// message/tool partials, `UiRequestRaised`, …) ride the SAME channel offset-less so a live
// driving session still receives its full reactive flow. The request payload carries an
// OPTIONAL {@link ResumeContext}: a request with NO `resume` (a PRESENT but empty
// payload beyond `sessionId`) replays the session's DURABLE transcript from the ORIGIN,
// present resumes STRICTLY AFTER that offset (only offset-bearing events are replayable). A
// SETTLED session replays its durable transcript and the stream COMPLETES (no longer
// `SessionNotFound`); a LIVE session replays the durable prefix then tails both new durable
// entries and ephemeral deltas; a session that never existed (no durable transcript AND no
// live handle) is `SessionNotFound`. Only `sessionEvents` gains durable replay —
// `sessionSend`/`interrupt`/`answerUiRequest` stay LIVE-only (a settled session is read-only,
// so they still fail `SessionNotFound`).
//
// `session_event_log` is dropped and restarted at offset `1` by a schema-version bump
// exactly as `event_log` is, so a per-session cursor is a generation-scoped coordinate
// too — and it gets the SAME guard, structurally, not a weaker one because today's
// client happens not to resume: it carries the very same {@link ResumeContext}, so a
// cursor here can no more travel without its generation than one on `events` can, and a
// stale generation is refused with {@link ResyncRequired} at every offset. Hence the
// two-error channel — the existence question (`SessionNotFound`) and the generation
// question are independent.
//
// LATENT, NOT LIVE — stated so the guard is not mistaken for an exercised path. NO
// shipped client resumes this feed today: the Swift `RpcBackend` builds the payload
// with `sessionId` only and never a `resume`, and `InteractiveSession` has no
// `ResyncRequired` handling, so in practice the session feed is ORIGIN-ONLY and the
// generation check here has no end-to-end path to fire on. It is defined now, and
// tested TS-side, because the coordinate really is generation-scoped and retrofitting
// a cursor guard onto a wire shape already in use is the expensive order to do it in.
// A resuming client is what makes it live; until one exists, treat it as correct and
// unexercised rather than as load-bearing today.
export const sessionEvents = Rpc.make("sessionEvents", {
  payload: {
    sessionId: SessionId,
    resume: Schema.optionalKey(ResumeContext),
  },
  success: OffsetSessionEvent,
  error: Schema.Union([SessionNotFound, ResyncRequired]),
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
