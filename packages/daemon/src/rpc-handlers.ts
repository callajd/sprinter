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
 * - `events` — the streaming work-graph delta feed, returned straight off the
 *   real `PubSub` (D17 / INV-REACTIVE); mutations publish via the publishing
 *   `StateStore` decorator (`./store-publishing.ts`), never a poll loop.
 * - `createWorkstreamFromPlan` — materialize a plan into a new `Workstream`
 *   (child-lists kept consistent with FK parentage on upsert; a fresh plan has no
 *   children yet, so this is trivially satisfied — planning fills them later).
 * - `control` — start / pause / resume / cancel a workstream, driving the runner.
 * - `retryIssue` — re-dispatch an issue's Job, reusing its SAME session id.
 *
 * The session-channel procedures (`sessionEvents` / `sessionSend` / `interrupt` /
 * `answerUiRequest`) are AE4.2; a servable layer needs every handler, so they are
 * implemented here as explicit placeholders that answer with the contract's own
 * `SessionNotFound` (the daemon holds no live-session registry yet). They are the
 * ONLY deferred surface and land in AE4.2.
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
  type WorkStatus,
  type Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { StateStore } from "@sprinter/state";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Runner = Context.Service.Shape<typeof JobRunner>;

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
 * maps to `done` because the FROZEN `WorkStatus` (D-contract, INV-CONTRACT) has no
 * `cancelled` variant — a cancelled workstream is indistinguishable from a completed
 * one until a contract v2 adds one.
 */
const statusFor: Record<ControlAction, WorkStatus> = {
  start: "active",
  resume: "active",
  pause: "blocked",
  cancel: "done",
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
    const scope = yield* Effect.scope;
    return {
      snapshot: () => buildSnapshot(store),
      // Live work-graph deltas. `changes` subscribes lazily (on first pull), so a
      // delta emitted between a client's `snapshot` and its `events` stream
      // attaching can be missed: a client must treat `snapshot` as the baseline and
      // reconcile deltas onto it (D4), subscribing before/around the snapshot read.
      // A durable offset-based resync via `EventLogStore.tail` (D17 reconciliation)
      // is AE5's to wire — see the workstream ledger.
      events: () => feed.changes,
      createWorkstreamFromPlan: ({ plan }) => materialize(store, plan),
      control: ({ workstreamId, action }) =>
        controlWorkstream(store, runner, scope, workstreamId, action),
      retryIssue: ({ issueId }) => retry(store, runner, scope, issueId),
      // Session channel — AE4.2. A servable layer needs every handler; until the
      // live-session registry lands, these answer with the contract's own error.
      sessionEvents: ({ sessionId }) => Stream.fail(new SessionNotFound({ id: sessionId })),
      sessionSend: ({ sessionId }) => Effect.fail(new SessionNotFound({ id: sessionId })),
      interrupt: ({ sessionId }) => Effect.fail(new SessionNotFound({ id: sessionId })),
      answerUiRequest: ({ sessionId }) => Effect.fail(new SessionNotFound({ id: sessionId })),
    };
  }),
);
