/**
 * The `SprinterRpc` server handlers (Track A, task AE4.1) — the real query,
 * events and command handlers behind the FROZEN daemon↔client contract
 * (`@sprinter/contract`, INV-CONTRACT). The contract is implemented EXACTLY as
 * frozen: no procedure, payload or schema is added, removed or altered here, and
 * the contract's own tests stay green.
 *
 * The handlers depend ONLY on ports (INV-PORT) — the {@link StateStore} (the
 * persisted work graph), the {@link JobRunner} (dispatch), and the
 * {@link WorkGraphEvents} reactive feed — and speak ONLY owned domain types
 * (INV-BOUNDARY). They are:
 *
 * - `snapshot` — full-state hydration, built by traversing the `StateStore`.
 * - `events` — the streaming work-graph delta feed of offset-stamped
 *   {@link OffsetEvent}s, returned straight off the real `PubSub` (D17 /
 *   INV-REACTIVE); mutations journal-and-publish via the journaling `StateStore`
 *   decorator (`./event-journal.ts`), never a poll loop.
 * - `createWorkstreamFromPlan` — materialize a plan into a new `Workstream`
 *   (child-lists kept consistent with FK parentage on upsert; a fresh plan has no
 *   children yet, so this is trivially satisfied — planning fills them later).
 * - `control` — start / pause / resume / cancel a workstream, driving the runner.
 * - `retryIssue` — re-dispatch an issue's Job, reusing its SAME session id.
 *
 * The session-channel procedures (`sessionEvents` / `sessionSend` / `interrupt` /
 * `answerUiRequest`) bridge a LIVE `@sprinter/runner` {@link SessionHandle}, resolved
 * through the {@link SessionRegistry} PORT (AE4.2) via the shared {@link resolveLive}
 * helper. A registry entry only lives for its session's run scope, so the helper gates
 * on DURABLE state (`StateStore`) to pick wait-vs-fail-fast:
 *
 * - a session whose durable `Session` row is still NON-TERMINAL (`starting`/`active`/
 *   `idle`) is genuinely MID-DISPATCH: the `running` delta is fanned out BEFORE
 *   `ExecutionRunner.run` registers the handle, so a client reacting to it can arrive
 *   before registration. The helper routes it to `SessionRegistry.resolve`, which
 *   bounded-waits for the handle to land rather than returning a spurious
 *   `SessionNotFound` (so the app needs no retry);
 * - a session whose durable row is TERMINAL (completed/failed/interrupted; the
 *   Inspector opens channels for SETTLED jobs by design, BE4.1) OR does not exist at
 *   all is routed to `SessionRegistry.get`, which FAILS FAST with `SessionNotFound` —
 *   no multi-second stall waiting for a registration that will never come.
 *
 * They carry ONLY owned neutral types (`SessionEvent` / `SessionInput` /
 * `UiResponse`) — no Pi wire type reaches this surface (INV-BOUNDARY) — and
 * `sessionEvents` streams live over `SessionHandle.events`, never a poll
 * (INV-REACTIVE). The handle's infrastructure failures (`PiTransportError` /
 * `PiRpcError`) are NOT contract errors, so they are turned into defects: the
 * contract's error channel is exactly `SessionNotFound`.
 *
 * Likewise the concrete LocalPi `ExecutionRunner` adapter is deferred (AE4.2/AE5):
 * these handlers drive the `JobRunner` PORT, which a runtime backs with either the
 * real adapter or a fake — this task keeps it fakeable and does not wire LocalPi.
 */
import { Context, Effect, Option, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type ControlAction,
  IssueNotFound,
  PlanRejected,
  SessionNotFound,
  type Snapshot,
  SprinterRpc,
  WorkstreamNotFound,
  type WorkstreamPlan,
} from "@sprinter/contract";
import {
  type Epic,
  type Issue,
  type IssueId,
  Job,
  type Session,
  type SessionEvent,
  type SessionId,
  type SessionInput,
  type UiResponse,
  type WorkStatus,
  type Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import type { SessionHandle } from "@sprinter/runner";
import { StateStore } from "@sprinter/state";
import { resyncEvents } from "./event-journal.ts";
import { SessionRegistry } from "./session-registry.ts";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Runner = Context.Service.Shape<typeof JobRunner>;
type Registry = Context.Service.Shape<typeof SessionRegistry>;

// ── snapshot ────────────────────────────────────────────────────────────────

/**
 * Build the contract {@link Snapshot} by traversing the persisted work graph:
 * every workstream, its epics (by FK), their issues (by FK), each issue's jobs,
 * and each job's session. Reuses the store's FK-scoped reads so the result is
 * consistent regardless of a node's cached child list.
 */
const buildSnapshot = (store: Store): Effect.Effect<Snapshot, never> =>
  Effect.gen(function* () {
    const workstreams = yield* store.workGraph.listWorkstreams;
    const epics: Array<Epic> = [];
    const issues: Array<Issue> = [];
    const jobs: Array<Job> = [];
    const sessions: Array<Session> = [];
    for (const workstream of workstreams) {
      const workstreamEpics = yield* store.workGraph.listEpics(workstream.id);
      for (const epic of workstreamEpics) {
        epics.push(epic);
        const epicIssues = yield* store.workGraph.listIssues(epic.id);
        for (const issue of epicIssues) {
          issues.push(issue);
          const issueJobs = yield* store.jobs.listJobsForIssue(issue.id);
          for (const job of issueJobs) {
            jobs.push(job);
            const session = yield* store.jobs.getSessionForJob(job.id);
            if (Option.isSome(session)) sessions.push(session.value);
          }
        }
      }
    }
    return { workstreams, epics, issues, jobs, sessions } satisfies Snapshot;
  }).pipe(Effect.orDie);

// ── createWorkstreamFromPlan ──────────────────────────────────────────────────

/** Derive a stable, url-safe workstream-id slug from the plan name. */
const slugify = (name: string): string =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

/**
 * Materialize a {@link WorkstreamPlan} into a new top-level {@link Workstream}
 * and persist it (the publishing decorator fans out a `WorkstreamChanged` delta).
 * A blank spec, or a name that yields no slug, is a {@link PlanRejected}. The
 * epic/issue breakdown is produced later by a planning Job — a fresh workstream
 * has an empty `epics` list, so FK/child-list consistency holds trivially.
 *
 * The id is derived from BOTH the plan name and its repo (a workstream is
 * repo-scoped, D14), so the same name for different repos does not collide. And
 * because `putWorkstream` is an UPSERT, a create whose id already exists would
 * silently clobber the existing workstream — resetting its `name`/`repo` and its
 * `epics` list while the epics' FK rows persist (a parentage desync). The contract
 * materializes a NEW workstream, so a colliding create is rejected, not upserted.
 */
const materialize = (
  store: Store,
  plan: WorkstreamPlan,
): Effect.Effect<WorkstreamId, PlanRejected> =>
  Effect.gen(function* () {
    if (plan.spec.trim().length === 0) {
      return yield* Effect.fail(new PlanRejected({ reason: "empty spec" }));
    }
    const slug = slugify(plan.name);
    if (slug.length === 0) {
      return yield* Effect.fail(
        new PlanRejected({ reason: "cannot derive a workstream id from the plan name" }),
      );
    }
    const repoSlug = slugify(plan.repo);
    // Both parts are slugified; `ws-<slug>[-<repoSlug>]` is non-empty by
    // construction, so the branded decode cannot fail.
    const idString = repoSlug.length > 0 ? `ws-${slug}-${repoSlug}` : `ws-${slug}`;
    const id = yield* Schema.decodeUnknownEffect(WorkstreamId)(idString).pipe(Effect.orDie);
    const existing = yield* store.workGraph.getWorkstream(id).pipe(Effect.orDie);
    if (Option.isSome(existing)) {
      return yield* Effect.fail(
        new PlanRejected({ reason: "a workstream already exists for this plan name and repo" }),
      );
    }
    const workstream: Workstream = {
      id,
      name: plan.name,
      repo: plan.repo,
      status: "pending",
      epics: [],
    };
    yield* store.workGraph.putWorkstream(workstream).pipe(Effect.orDie);
    return id;
  });

// ── command dispatch (drive the runner) ──────────────────────────────────────

/**
 * Dispatch a {@link Job} through the {@link JobRunner} as a background fiber tied
 * to the handler-layer scope. A running session can take minutes, so the command
 * RPC must not block on it; a dispatch failure is logged, never lost.
 */
const dispatchInBackground = (runner: Runner, scope: Scope, job: Job): Effect.Effect<void> =>
  runner.dispatch(job).pipe(
    Effect.scoped,
    Effect.catchCause((cause) => Effect.logError("job dispatch failed", cause)),
    Effect.forkIn(scope, { startImmediately: true }),
    Effect.asVoid,
  );

/**
 * Lifecycle status a workstream takes on for each control action. NOTE (AE4.1
 * scope): `pause`/`cancel` are status-only here — they transition the workstream
 * node but do NOT interrupt an in-flight session (that rides on the session
 * `interrupt` channel, AE4.2) nor roll status down to epics/issues/jobs. `cancel`
 * maps to the distinct terminal `cancelled` (contract v2 / CE5.1) — a cancelled
 * workstream is terminal-but-not-`done`, so it renders and reconciles apart from a
 * completed one ({@link isTerminal}).
 */
const statusFor: Record<ControlAction, WorkStatus> = {
  start: "active",
  resume: "active",
  pause: "blocked",
  cancel: "cancelled",
};

/** Dispatch every still-queued Job under a workstream (start / resume). */
const dispatchWorkstreamJobs = (
  store: Store,
  runner: Runner,
  scope: Scope,
  workstream: Workstream,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const epics = yield* store.workGraph.listEpics(workstream.id).pipe(Effect.orDie);
    for (const epic of epics) {
      const issues = yield* store.workGraph.listIssues(epic.id).pipe(Effect.orDie);
      for (const issue of issues) {
        const jobs = yield* store.jobs.listJobsForIssue(issue.id).pipe(Effect.orDie);
        for (const job of jobs) {
          if (job.status === "queued") yield* dispatchInBackground(runner, scope, job);
        }
      }
    }
  });

/**
 * Apply a {@link ControlAction} to a workstream: transition its lifecycle status
 * (persisting it, which fans out a `WorkstreamChanged` delta) and, on start /
 * resume, dispatch its queued jobs. {@link WorkstreamNotFound} on an unknown id.
 */
const controlWorkstream = (
  store: Store,
  runner: Runner,
  scope: Scope,
  workstreamId: WorkstreamId,
  action: ControlAction,
): Effect.Effect<void, WorkstreamNotFound> =>
  Effect.gen(function* () {
    const found = yield* store.workGraph.getWorkstream(workstreamId).pipe(Effect.orDie);
    if (Option.isNone(found)) {
      return yield* Effect.fail(new WorkstreamNotFound({ id: workstreamId }));
    }
    const workstream = found.value;
    yield* store.workGraph
      .putWorkstream({ ...workstream, status: statusFor[action] })
      .pipe(Effect.orDie);
    if (action === "start" || action === "resume") {
      yield* dispatchWorkstreamJobs(store, runner, scope, workstream);
    }
  });

/** A fresh implement Job for an issue that has none yet — id derived from the issue. */
const freshJobFor = (issueId: IssueId): Effect.Effect<Job> =>
  Schema.decodeUnknownEffect(Job)({
    id: `job-${issueId}`,
    issueId,
    kind: "implement",
    status: "queued",
  }).pipe(Effect.orDie);

/**
 * Re-dispatch an issue's Job. {@link IssueNotFound} on an unknown issue. Reuses
 * the issue's most recent persisted Job — which carries its `sessionId`, so the
 * `JobRunner` re-attaches to the SAME session (1 Job = 1 session) — or mints a
 * fresh implement Job when the issue has never been dispatched.
 *
 * "Most recent" is `listJobsForIssue`'s last row (ordered by id); under the current
 * one-job-per-issue `job-<issueId>` scheme that IS the issue's only Job. A retry of
 * a Job still IN FLIGHT (queued/running) is a no-op — re-dispatching it would fork a
 * second `dispatch` racing the same session rows; retry acts only on a terminal or
 * absent Job.
 */
const retry = (
  store: Store,
  runner: Runner,
  scope: Scope,
  issueId: IssueId,
): Effect.Effect<void, IssueNotFound> =>
  Effect.gen(function* () {
    const issue = yield* store.workGraph.getIssue(issueId).pipe(Effect.orDie);
    if (Option.isNone(issue)) {
      return yield* Effect.fail(new IssueNotFound({ id: issueId }));
    }
    const jobs = yield* store.jobs.listJobsForIssue(issueId).pipe(Effect.orDie);
    const existing = jobs.at(-1);
    if (existing !== undefined && (existing.status === "queued" || existing.status === "running")) {
      return;
    }
    const job = existing ?? (yield* freshJobFor(issueId));
    yield* dispatchInBackground(runner, scope, job);
  });

// ── session channel (bridge a live SessionHandle) ────────────────────────────

/**
 * True while a durable {@link Session} row is still MID-DISPATCH — i.e. genuinely
 * running (or about to), so its live {@link SessionHandle} either is registered now
 * or is about to be within the register-after-dispatch window. The complement is a
 * TERMINAL session (settled: `completed`/`failed`/`interrupted`), whose registry entry
 * has already been torn down and will never reappear. Expressed as a POSITIVE
 * allow-list of the non-terminal statuses so any future status defaults to fail-fast
 * (never re-introducing a spurious multi-second stall on a settled/unknown state).
 */
const isMidDispatchSession = (status: Session["status"]): boolean =>
  status === "starting" || status === "active" || status === "idle";

/**
 * Resolve the live {@link SessionHandle} for a `sessionId`, choosing wait-vs-fail-fast
 * on DURABLE state (the shared gate behind all four session-channel procedures):
 *
 * - read the durable `Session` row from the {@link StateStore};
 * - a row that is present AND still {@link isMidDispatchSession mid-dispatch} is bridged
 *   through `SessionRegistry.resolve`, which bounded-WAITS out the register-after-dispatch
 *   window so a client reacting to the `running` delta needs no retry;
 * - a row that is TERMINAL (settled) OR ABSENT (never existed) is resolved through
 *   `SessionRegistry.get`, which FAILS FAST with {@link SessionNotFound} — no
 *   multi-second stall on a registration that will never land (the regression this
 *   gate closes: the Inspector opens channels for SETTLED jobs by design, BE4.1).
 *
 * The `StateStore` read cannot surface a contract error, so its own `StateStoreError`
 * becomes a defect (`orDie`) — the resolved error channel stays exactly `SessionNotFound`.
 */
const resolveLive = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
): Effect.Effect<SessionHandle, SessionNotFound> =>
  store.jobs.getSession(sessionId).pipe(
    Effect.orDie,
    Effect.flatMap((session) =>
      Option.match(session, {
        // Never existed → nothing is or will be registered; fail fast.
        onNone: () => registry.get(sessionId),
        onSome: (row) =>
          // Mid-dispatch → bridge the window (bounded wait); settled → fail fast.
          isMidDispatchSession(row.status) ? registry.resolve(sessionId) : registry.get(sessionId),
      }),
    ),
  );

/**
 * Stream a live session's owned {@link SessionEvent} feed for the contract's
 * `sessionEvents` RPC (INV-REACTIVE): resolve the {@link SessionHandle} through
 * {@link resolveLive} and hand back its live `events` stream. A `SessionNotFound` from
 * the lookup surfaces as the stream's failure; the handle's `PiTransportError`
 * (transport teardown — not a contract error) becomes a defect via `Stream.orDie`,
 * so the stream's error channel is exactly `SessionNotFound`.
 */
const bridgeEvents = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
): Stream.Stream<SessionEvent, SessionNotFound> =>
  Stream.unwrap(
    resolveLive(store, registry, sessionId).pipe(
      Effect.map((handle) => Stream.orDie(handle.events)),
    ),
  );

/**
 * Drive a {@link SessionInput} into the live session for the `sessionSend` RPC:
 * resolve the {@link SessionHandle} through {@link resolveLive} ({@link SessionNotFound}
 * on a miss) and call `send`. The handle's `PiRpcError`/`PiTransportError` are
 * infrastructure failures, not contract errors, so they become defects — the error
 * channel is exactly `SessionNotFound`.
 */
const driveInput = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
  input: SessionInput,
): Effect.Effect<void, SessionNotFound> =>
  resolveLive(store, registry, sessionId).pipe(
    Effect.flatMap((handle) => handle.send(input).pipe(Effect.orDie)),
  );

/**
 * Abort the live session's in-flight turn for the `interrupt` RPC: resolve the
 * {@link SessionHandle} through {@link resolveLive} ({@link SessionNotFound} on a miss)
 * and call `interrupt`; the handle's transport failures become defects, not contract
 * errors.
 */
const abortTurn = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
): Effect.Effect<void, SessionNotFound> =>
  resolveLive(store, registry, sessionId).pipe(
    Effect.flatMap((handle) => handle.interrupt.pipe(Effect.orDie)),
  );

/**
 * Answer an outstanding UI request for the `answerUiRequest` RPC, completing the
 * `extension_ui_request` round-trip: resolve the {@link SessionHandle} through
 * {@link resolveLive} ({@link SessionNotFound} on a miss) and hand the neutral
 * {@link UiResponse} to the live session via `answerUi` (which is total — it cannot fail).
 */
const answerUi = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
  response: UiResponse,
): Effect.Effect<void, SessionNotFound> =>
  resolveLive(store, registry, sessionId).pipe(
    Effect.flatMap((handle) => handle.answerUi(response)),
  );

// ── the handler layer ─────────────────────────────────────────────────────────

/**
 * The `SprinterRpc` server-handler `Layer` (`effect/unstable/rpc`). Requires the
 * {@link StateStore}, {@link JobRunner} and {@link WorkGraphEvents} PORTS; a
 * runtime backs those with concrete adapters. The layer-build scope owns every
 * background dispatch fiber, so they are interrupted when the daemon stops.
 */
export const handlers = SprinterRpc.toLayer(
  Effect.gen(function* () {
    const store = yield* StateStore;
    const runner = yield* JobRunner;
    const feed = yield* WorkGraphEvents;
    const sessions = yield* SessionRegistry;
    const scope = yield* Effect.scope;
    return {
      snapshot: () => buildSnapshot(store),
      // Live work-graph deltas with DURABLE offset-based resync (CE1.2 / D17): the
      // feed eagerly subscribes live, replays the durable event log from the client's
      // `sinceOffset` cursor (`EventLogStore.tail`, journaled by the store decorator),
      // then streams the live tail — so a reconnecting client catches up on the whole
      // persisted history deterministically, not only the deltas after it attaches.
      // Subscribe-before-replay closes the gap; upsert-idempotent deltas (D8) absorb
      // the boundary overlap. Each streamed item is an `OffsetEvent` carrying its
      // durable offset (contract v3 / CE2.0), and replay + live offsets share one
      // coordinate space, so the client can feed a streamed item's offset back as
      // `sinceOffset`. The cursor is OPTIONAL: a request with NO `sinceOffset` (a
      // present but empty `{}` payload) replays from origin, present resumes strictly
      // after that offset, over the same `resyncFrom` primitive. The strict
      // `> sinceOffset` ordering is scoped to that resume; within one stream the
      // subscribe-before-replay boundary can overlap (harmless under upsert).
      events: ({ sinceOffset }) => resyncEvents(store, feed, sinceOffset),
      createWorkstreamFromPlan: ({ plan }) => materialize(store, plan),
      control: ({ workstreamId, action }) =>
        controlWorkstream(store, runner, scope, workstreamId, action),
      retryIssue: ({ issueId }) => retry(store, runner, scope, issueId),
      // Session channel — AE4.2. Each procedure resolves the SAME live session
      // through `resolveLive` (the durable-state gate: mid-dispatch → bounded wait,
      // settled/absent → fail fast) and bridges its neutral `SessionHandle` surface;
      // a miss is the contract's `SessionNotFound`. `sessionEvents` streams live over
      // `SessionHandle.events` (INV-REACTIVE).
      sessionEvents: ({ sessionId }) => bridgeEvents(store, sessions, sessionId),
      sessionSend: ({ sessionId, input }) => driveInput(store, sessions, sessionId, input),
      interrupt: ({ sessionId }) => abortTurn(store, sessions, sessionId),
      answerUiRequest: ({ sessionId, response }) => answerUi(store, sessions, sessionId, response),
    };
  }),
);
