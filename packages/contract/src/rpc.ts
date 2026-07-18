/**
 * The versioned daemon↔client **RPC contract v1** — an `RpcGroup`
 * (`effect/unstable/rpc`) over the FE2.1 owned domain schemas (architecture §7,
 * D8/D10/D16/D17).
 *
 * The surface speaks ONLY `@sprinter/domain`'s owned, provider-neutral types
 * (INV-PORT / INV-NAMING): no Pi concept and no FE2.2 owned-Pi wire schema
 * (`packages/domain/src/pi/`) appears here — that adapter-internal wire stays out
 * of the client contract. It is **maximally reactive** (D17 / INV-REACTIVE): the
 * work-graph `events` feed and the `sessionEvents` feed are STREAMING RPCs
 * (`RpcSchema.Stream`), not polled request/response. It is **small, stable, and
 * explicitly versioned** (D10 / INV-CONTRACT): every procedure is hand-mirrored
 * in Swift (FE2.4), so the surface is minimal and carries a {@link CONTRACT_VERSION}
 * marker on the group.
 *
 * Four models (architecture §7):
 *  1. **snapshot** — request/response, hydrates full state on connect;
 *  2. **events** — streaming work-graph deltas;
 *  3. **commands** — create-workstream-from-plan, control, retry-issue;
 *  4. **session channel** — streaming `sessionEvents`, plus `sessionSend`,
 *     `interrupt`, and `answerUiRequest`.
 */
import { Context, Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import {
  Epic,
  Issue,
  IssueId,
  Job,
  Session,
  SessionEvent,
  SessionId,
  SessionInput,
  UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";

// ── Contract version (INV-CONTRACT) ────────────────────────────────────────

/**
 * Current contract version. Bumped whenever the RPC surface changes; the Swift
 * mirror (FE2.4) and its decode tests track this bump (INV-CONTRACT).
 */
export const CONTRACT_VERSION = 1 as const;

/** Format the contract version as a `v`-prefixed tag, e.g. `v1`. */
export const contractTag = (version: number = CONTRACT_VERSION): string => `v${version}`;

/**
 * Annotation key carrying the {@link CONTRACT_VERSION} on the {@link SprinterRpc}
 * group — the explicit, in-band version marker consumers read to negotiate the
 * surface (INV-CONTRACT).
 */
export const ContractVersion = Context.Service<typeof CONTRACT_VERSION>(
  "@sprinter/contract/ContractVersion",
);

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

// ── Procedures (contract v1) ────────────────────────────────────────────────
//
// Each procedure is a named `Rpc` so its per-procedure payload/success/error
// types are preserved (the group's `requests` map erases them to a union). The
// `events` and `sessionEvents` feeds are `stream: true`, so their success/error
// is wrapped in `RpcSchema.Stream` — they are streaming, not polled
// (INV-REACTIVE).

// (1) snapshot — request/response full-state hydration on connect.
export const snapshot = Rpc.make("snapshot", { success: Snapshot });

// (2) events — streaming work-graph deltas (INV-REACTIVE).
export const events = Rpc.make("events", { success: WorkGraphEvent, stream: true });

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
export const sessionEvents = Rpc.make("sessionEvents", {
  payload: { sessionId: SessionId },
  success: SessionEvent,
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

// ── The RPC group (contract v1) ─────────────────────────────────────────────

/**
 * The versioned RPC contract group — daemon↔client v1. Carries the four models
 * (architecture §7) as procedures and the {@link CONTRACT_VERSION} annotation.
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
).annotate(ContractVersion, CONTRACT_VERSION);
