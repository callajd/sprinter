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
 * on DURABLE state (`StateStore`) to pick wait-vs-fail-fast — keying on the durable
 * JOB status (session → job → job-status), the dispatch unit `startup-reconcile`
 * maintains (CE4.1 FIX A):
 *
 * - a session whose durable `Job` is still NON-TERMINAL (`queued`/`running`) is
 *   genuinely MID-DISPATCH: the `running` delta is fanned out BEFORE
 *   `ExecutionRunner.run` registers the handle, so a client reacting to it can arrive
 *   before registration. The helper routes it to `SessionRegistry.resolve`, which
 *   bounded-waits for the handle to land rather than returning a spurious
 *   `SessionNotFound` (so the app needs no retry);
 * - a session whose durable Job is TERMINAL (`succeeded`/`failed`/`cancelled`; the
 *   Inspector opens channels for SETTLED jobs by design, BE4.1) OR whose Job/Session
 *   row does not exist is routed to `SessionRegistry.get`, which FAILS FAST with
 *   `SessionNotFound` — no multi-second stall waiting for a registration that will
 *   never come. Keying on the Job (not the Session row) closes the crash-orphaned case:
 *   `startup-reconcile` settles a stale Job it cannot resume by writing ONLY the Job
 *   row, leaving its Session row NON-TERMINAL — a Session-row gate would stall on it.
 *
 * They carry ONLY owned neutral types (`SessionEvent` / `SessionInput` /
 * `UiResponse`) — no Pi wire type reaches this surface (INV-BOUNDARY). `sessionSend` /
 * `interrupt` / `answerUiRequest` stay LIVE-only (a settled session is read-only). Only
 * `sessionEvents` gains DURABLE replay: it replays the session's durable
 * transcript from the client's `sinceOffset` cursor then live-tails new durable entries if
 * the session is running, or COMPLETES if it has settled — the session-channel mirror of
 * the `events` offset-resync, so a SETTLED session's transcript is viewable in the Inspector
 * rather than a `SessionNotFound`. It streams off the durable log + `SessionEvents` feed,
 * never a poll (INV-REACTIVE). The handle's infrastructure failures (`PiTransportError` /
 * `PiRpcError`) are NOT contract errors, so they are turned into defects and the
 * contract's error channel carries only genuine contract outcomes: `SessionNotFound`
 * for `sessionSend`/`interrupt`/`answerUiRequest`, and `SessionNotFound |
 * ResyncRequired` for `sessionEvents` — whose `sinceOffset` is a per-session durable
 * coordinate and therefore scoped to a STORE GENERATION exactly as the work-graph
 * cursor is (both logs are dropped by a schema-version bump).
 *
 * Likewise the concrete LocalPi `ExecutionRunner` adapter is deferred (AE4.2/AE5):
 * these handlers drive the `JobRunner` PORT, which a runtime backs with either the
 * real adapter or a fake — this task keeps it fakeable and does not wire LocalPi.
 */
import { Context, Duration, Effect, Option, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type ControlAction,
  IssueNotFound,
  type OffsetSessionEvent,
  PlanRejected,
  type ResumeContext,
  ResyncRequired,
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
  type Repository,
  type Session,
  type SessionId,
  type SessionInput,
  type UiResponse,
  type WorkStatus,
  type Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { CodeHost } from "@sprinter/repository";
import type { SessionHandle } from "@sprinter/runner";
import { StateStore } from "@sprinter/state";
import { resyncEvents } from "./event-journal.ts";
import { SessionEvents } from "./session-events.ts";
import { SessionRegistry } from "./session-registry.ts";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Runner = Context.Service.Shape<typeof JobRunner>;
type Host = Context.Service.Shape<typeof CodeHost>;
type Registry = Context.Service.Shape<typeof SessionRegistry>;
type SessionFeed = Context.Service.Shape<typeof SessionEvents>;

/**
 * The `sessionEvents` error channel: two INDEPENDENT refusals — the session is not
 * one this daemon knows ({@link SessionNotFound}), or the client's resume cursor is
 * not from this store generation ({@link ResyncRequired}). Named so the stream's
 * failures widen to the whole channel rather than being inferred one branch at a time.
 */
type SessionEventsError = SessionNotFound | ResyncRequired;

// ── snapshot ────────────────────────────────────────────────────────────────

/**
 * Build the contract {@link Snapshot} by traversing the persisted work graph:
 * every workstream, its epics (by FK), their issues (by FK), each issue's jobs,
 * and each job's session. Reuses the store's FK-scoped reads so the result is
 * consistent regardless of a node's cached child list.
 *
 * The `── STATE ──` layer (`repositories`) is hydrated WHOLE for a different reason
 * than the registry: `Workstream.repositoryId` is a REFERENCE, so a client that
 * received workstreams without the repositories they name could resolve none of them.
 * Every observation is shipped as stored, INCLUDING an old one — staleness is rendered
 * from each record's `observedAt` (DE4.4), and withholding a stale record would delete
 * the very evidence that rendering needs (D7: reads never gate on staleness).
 *
 * READ ORDER IS LOAD-BEARING: workstreams FIRST, repositories LAST. These reads are not
 * one transaction, so a concurrent `createWorkstreamFromPlan` can interleave between
 * them — and it always writes the repository BEFORE the workstream that references it
 * (`Workstream.repositoryId` is a FOREIGN KEY, so it cannot do otherwise). Reading
 * repositories first would therefore admit `read repos → write R → write W → read
 * workstreams`, yielding a snapshot carrying a workstream whose repository is absent —
 * a dangling reference the Swift projection has to render around. Reading them last
 * makes that window unreachable: any workstream in the list was written before the read
 * that found it, so the repository it references was written earlier still and is in the
 * later read. (The converse skew — a repository with no workstream yet referencing it —
 * is harmless; the state layer is not required to be reachable from the work graph.)
 *
 * GROWTH CURVE, stated so it is a known cost rather than a surprise: this ships EVERY
 * repository with EVERY observed ref (up to the adapter's 100-branch page) on every
 * `snapshot()` and on every `ResyncRequired` recovery, so the payload grows as
 * `repositories × refs` and a resync storm multiplies it by the reconnect rate. It is
 * acceptable at the scale `DMR` targets — a handful of repositories on one local
 * daemon — and it is the shape a REFERENCE demands: a client cannot resolve
 * `Workstream.repositoryId` against a record it was not sent. Paginating or ref-pruning
 * the snapshot is a real change to the resume contract (a partial snapshot needs its
 * own cursor), so it belongs to a later task, not to a quiet trim here.
 *
 * The REGISTRY layer is hydrated flat and WHOLE — `listAgents`, not a per-repo or
 * per-workstream read — because an `Agent` is global and carries no repository:
 * "the agents used in this repo" is a fold the client computes over that repo's
 * executions, never a slice the daemon stores or ships (INV-DERIVED).
 *
 * The snapshot also carries the store's GENERATION — the coordinate space the state
 * was read from. This is where a client's resume context is established: it retains
 * the generation with the state and hands it back on every cursor-bearing request, so
 * a cursor minted before a drop-and-recreate is REFUSED rather than silently resumed
 * against a log it never belonged to (see `requireLiveCursor` in `./event-journal.ts`).
 */
const buildSnapshot = (store: Store): Effect.Effect<Snapshot, never> =>
  Effect.gen(function* () {
    const agents = yield* store.agents.listAgents;
    // Workstreams FIRST and repositories LAST — see the docstring: a repository always
    // predates the workstream referencing it, so this order makes a dangling
    // `repositoryId` in a snapshot unreachable rather than merely unlikely.
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
    const repositories = yield* store.repositories.listRepositories;
    return {
      repositories,
      workstreams,
      epics,
      issues,
      jobs,
      sessions,
      agents,
      generation: store.generation,
    } satisfies Snapshot;
  }).pipe(Effect.orDie);

// ── createWorkstreamFromPlan ──────────────────────────────────────────────────

/**
 * How long `createWorkstreamFromPlan` will wait for the code host to answer before
 * rejecting the plan as unreachable.
 *
 * Ten seconds is chosen against the INTERACTION, not against GitHub: a human is
 * watching a form, and a wait longer than this reads as a hang rather than as work in
 * progress. It is far above GitHub's normal repository-lookup latency (tens of
 * milliseconds), so it fires on a genuinely stuck connection and not on a slow one.
 */
export const RESOLVE_TIMEOUT = Duration.seconds(10);

/** Derive a stable, url-safe workstream-id slug from the plan name. */
const slugify = (name: string): string =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

/**
 * RESOLVE-OR-CREATE the {@link Repository} a plan names (DE1.2 D6).
 *
 * The plan carries the NATURAL key `(host, owner, name)` because the client composing
 * it has never seen a `RepositoryId`. So the key is OBSERVED through the {@link CodeHost}
 * port: the host either knows the repository — and the observation it returns is stored
 * (creating the record on a first sighting, REFRESHING it wholesale under a new
 * `observedAt` on a later one, D7) — or it does not, in which case the plan is
 * {@link PlanRejected} with a reason.
 *
 * The rejection branch writes NOTHING, and that is the point rather than an
 * optimisation: fabricating a repository row from the plan's own words would put a
 * record in the `── STATE ──` layer that no host ever confirmed — an "observation" of
 * something nobody observed — and every workstream anchored to it would then reference
 * a repository that may not exist. `Workstream.repositoryId` is a real FOREIGN KEY, so
 * this is also the only way the workstream write below can succeed at all.
 *
 * A host that could not be ASKED (a 500, a rate limit) is a different outcome from a
 * host that answered "no such repository", and it stays that way — but it is a
 * REJECTION, not a defect. Both are `PlanRejected`, and the `reason` is what keeps them
 * apart: one says the host does not know the repository, the other says the host could
 * not be reached, and only the second invites a retry. Reporting "that repository does
 * not exist" for an unreachable host would be a lie the user would act on; killing the
 * request fiber over a transient upstream 500 would be worse — a plan that was not
 * materialised because GitHub hiccupped is an ordinary, expected outcome of talking to
 * a network service, and `PlanRejected` already carries the vocabulary to say so.
 *
 * The `putRepository` failure stays a DEFECT. A `StateStoreError` is not an outcome the
 * user can act on or a plan can be corrected for — it means Sprinter's own store is
 * broken — and folding it into a rejection would advertise a user-facing reason for an
 * internal fault.
 *
 * ## The resolve is BOUNDED (`RESOLVE_TIMEOUT`)
 *
 * This is the first NETWORK call on a user-facing command — `createWorkstreamFromPlan`
 * was store-only before DE1.2 — and the adapter carries no timeout and no retry of its
 * own (`packages/repository/src/github.ts`). A hung connection (a TCP black hole, a
 * proxy that accepts and never answers) does not fail: it simply never returns, so
 * without a bound the RPC would hang FOREVER, holding the caller's request open with no
 * outcome of any kind for the user to act on.
 *
 * So the resolve is bounded, and a timeout is mapped to the SAME "could not be
 * reached — retry" rejection an unreachable host gets, because that is exactly what it
 * is: from the plan's point of view a host that never answered and a host that answered
 * 500 are one outcome — the plan was not materialised, nothing was written, and a retry
 * is the remedy. Inventing a third reason would only ask the user to distinguish two
 * situations they act on identically.
 *
 * The bound belongs HERE rather than in the adapter: it is a property of the
 * INTERACTION — a human is waiting on a synchronous RPC — not of GitHub. A background
 * reconciler calling the same port would want a different bound, and the adapter has no
 * way to know which caller it is serving (INV-PORT).
 */
const resolveRepository = (
  store: Store,
  host: Host,
  plan: WorkstreamPlan,
): Effect.Effect<Repository, PlanRejected> =>
  Effect.gen(function* () {
    const unreachable = (detail: string) =>
      new PlanRejected({
        reason: `the code host could not be reached to resolve ${plan.repository.owner}/${plan.repository.name} (${detail}); the plan was not materialized — retry`,
      });
    const observed = yield* host.repositories.resolve(plan.repository).pipe(
      Effect.mapError((error) => unreachable(error.detail)),
      Effect.timeoutOrElse({
        duration: RESOLVE_TIMEOUT,
        orElse: () =>
          Effect.fail(unreachable(`no response within ${Duration.format(RESOLVE_TIMEOUT)}`)),
      }),
    );
    if (Option.isNone(observed)) {
      return yield* Effect.fail(
        new PlanRejected({
          reason: `the code host does not know the repository ${plan.repository.owner}/${plan.repository.name}`,
        }),
      );
    }
    yield* store.repositories.putRepository(observed.value).pipe(Effect.orDie);
    return observed.value;
  });

/**
 * Materialize a {@link WorkstreamPlan} into a new top-level {@link Workstream}
 * and persist it (the publishing decorator fans out a `WorkstreamChanged` delta).
 * A blank spec, or a name that yields no slug, is a {@link PlanRejected}. The
 * epic/issue breakdown is produced later by a planning Job — a fresh workstream
 * has an empty `epics` list, so FK/child-list consistency holds trivially.
 *
 * The plan's repository key is resolved (or created) FIRST, through
 * {@link resolveRepository}: the workstream's `repositoryId` is a real FOREIGN KEY,
 * so the anchor has to exist before the workstream that references it can be written
 * at all. A plan the host does not recognise fails there, having written nothing.
 *
 * The id is derived from BOTH the plan name and its RESOLVED repository (a workstream is
 * repo-scoped, D14), so the same name for different repositories does not collide — and
 * it is derived from the repository's `id`, which is INJECTIVE, rather than from a slug
 * of its natural key, which is not: slugifying `${host}-${owner}-${name}` maps
 * `(github, a-b, c)` and `(github, a, b-c)` — two different repositories — onto one
 * string, and collapses case besides. The consequence was not corruption but a FALSE
 * rejection ("a workstream already exists…") for a plan naming a genuinely different
 * repository. The id is percent-encoded so the composed id stays url-safe despite the
 * separators inside a `RepositoryId`; nothing parses it back out.
 *
 * Deriving it from the RESOLVED record means the repository is observed BEFORE the
 * duplicate check, so a plan rejected as a duplicate has refreshed that repository's
 * observation. That is not a write the rejection was supposed to avoid: the D6 hazard
 * is fabricating a row for a repository nobody observed, and this is the opposite — a
 * real observation of a repository that necessarily already had a row, since the
 * workstream the duplicate check found holds a FOREIGN KEY to it.
 *
 * Because `putWorkstream` is an UPSERT, a create whose id already exists would
 * silently clobber the existing workstream — resetting its `name`/`repositoryId` and
 * its `epics` list while the epics' FK rows persist (a parentage desync). The contract
 * materializes a NEW workstream, so a colliding create is rejected, not upserted.
 */
const materialize = (
  store: Store,
  host: Host,
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
    const repository = yield* resolveRepository(store, host, plan);
    // The repository half of the id is the RESOLVED record's `id` — the one encoding of
    // a repository that is injective — percent-encoded so the composed id has no `:` or
    // `/` in it. Slugifying the natural key instead is what made `(github, a-b, c)` and
    // `(github, a, b-c)` collide.
    const idString = `ws-${slug}-${encodeURIComponent(repository.id)}`;
    // `slug` is non-empty (checked above) and the encoded id is non-empty, so the
    // branded decode cannot fail.
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
      repositoryId: repository.id,
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
 * maps to the distinct terminal `cancelled` (CE5.1) — a cancelled
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
 * True while a durable {@link Job} row is still MID-DISPATCH — i.e. genuinely queued
 * or running, so its live {@link SessionHandle} either is registered now or is about
 * to be within the register-after-dispatch window. The complement is a TERMINAL job
 * (settled: `succeeded`/`failed`/`cancelled`), whose registry entry has already been
 * torn down and will never reappear. Expressed as a POSITIVE allow-list of the
 * non-terminal statuses so any future status defaults to fail-fast (never
 * re-introducing a spurious multi-second stall on a settled/unknown state).
 *
 * The gate keys on the JOB, not the durable `Session` row, ON PURPOSE (CE4.1 FIX A):
 * `startup-reconcile` settles a stale `running` Job it cannot resume by writing ONLY
 * the Job row (its `putJob` → terminal), and NEVER settles the Session row. So after a
 * crash + restart a settled-not-resumed job leaves its Session row NON-TERMINAL
 * (`starting`/`active`) while its Job is terminal. Gating on the Session row would
 * mistake that crash-orphaned state for mid-dispatch and stall the full resolve bound;
 * the Job — the dispatch unit reconcile maintains — is the authoritative signal.
 */
const isMidDispatchJob = (status: Job["status"]): boolean =>
  status === "queued" || status === "running";

/**
 * True while a durable {@link Session} row is still LIVE — i.e. genuinely
 * `starting`/`active`/`idle`, so its handle is registered or about to be. The
 * complement is a TERMINAL session (`interrupted`/`completed`/`failed`), whose registry
 * entry is gone for good. A POSITIVE allow-list of the non-terminal statuses, so any
 * future status defaults to fail-fast.
 *
 * This is the belt to {@link isMidDispatchJob}'s braces (CE4.1-R4): `startup-reconcile`
 * can settle a stale `running` Job to `queued` under a paused Workstream — `queued` IS
 * mid-dispatch, so a Job-only gate would bounded-WAIT on that orphan and stall the full
 * resolve bound. Reconcile now also settles the Session row to a terminal status, so
 * gating on BOTH (mid-dispatch Job AND live Session) fails that `queued`-orphan fast
 * while still bridging a genuine mid-dispatch (running Job + `starting` Session).
 */
const isLiveSessionRow = (status: Session["status"]): boolean =>
  status === "starting" || status === "active" || status === "idle";

/**
 * Resolve the live {@link SessionHandle} for a `sessionId`, choosing wait-vs-fail-fast
 * on DURABLE state (the shared gate behind all four session-channel procedures). It
 * resolves session → job → job-status and gates on the JOB:
 *
 * - read the durable `Session` row from the {@link StateStore} to recover the `jobId`
 *   it belongs to (1 Job = 1 session); ABSENT → nothing is or will be registered, fail
 *   fast through `SessionRegistry.get`. A row that is itself TERMINAL (not
 *   {@link isLiveSessionRow live}) also fails fast — it covers the reconcile
 *   `queued`-orphan (CE4.1-R4) whose Job stays mid-dispatch but whose Session was settled;
 * - read that {@link Job} row; a job that is present AND still {@link isMidDispatchJob
 *   mid-dispatch} (`queued`/`running`) — with a LIVE Session row — is bridged through
 *   `SessionRegistry.resolve`, which bounded-WAITS out the register-after-dispatch window
 *   so a client reacting to the `running` delta needs no retry;
 * - a job that is TERMINAL (settled) OR ABSENT is resolved through
 *   `SessionRegistry.get`, which FAILS FAST with {@link SessionNotFound} — no
 *   multi-second stall on a registration that will never land. This covers the Inspector
 *   opening channels for SETTLED jobs (BE4.1), the crash-orphaned case (a terminal Job
 *   whose Session row an older reconcile left NON-TERMINAL), and — via the Session-row
 *   check above — the `queued`-orphan the Job gate alone would miss.
 *
 * FIX B (CE4.1, revised #76): a transient `StateStoreError` from either durable read is
 * a DEFECT (`Effect.die`), never folded into `SessionNotFound`. Conflating a store
 * hiccup with a genuine "no live handle / not mid-dispatch" miss is the #76 bug: the
 * `sessionEvents` serving path catches `SessionNotFound` to `succeedNone`, so a transient
 * store error on a genuinely LIVE session would collapse `live` to `None`, replay only the
 * durable prefix, and silently COMPLETE — dropping the live tail. Surfacing the store
 * error as a defect instead fails LOUDLY rather than silently truncating a live stream.
 * The resolved typed error channel stays exactly `SessionNotFound` (infra failures cross
 * as defects), and only a genuine registry miss yields `SessionNotFound`.
 */
const resolveLive = (
  store: Store,
  registry: Registry,
  sessionId: SessionId,
): Effect.Effect<SessionHandle, SessionNotFound> =>
  Effect.gen(function* () {
    const session = yield* store.jobs.getSession(sessionId);
    // No Session row → never existed / already torn down; fail fast.
    if (Option.isNone(session)) return yield* registry.get(sessionId);
    // Session row TERMINAL → registry entry gone for good (settled, or a reconcile
    // `queued`-orphan whose Job stays mid-dispatch, CE4.1-R4); fail fast, no stall.
    if (!isLiveSessionRow(session.value.status)) return yield* registry.get(sessionId);
    const job = yield* store.jobs.getJob(session.value.jobId);
    // Job absent, or terminal → registry entry gone for good; fail fast.
    // Job mid-dispatch (queued/running) AND Session live → bridge the register-after-
    // dispatch window (bounded wait).
    if (Option.isNone(job) || !isMidDispatchJob(job.value.status)) {
      return yield* registry.get(sessionId);
    }
    return yield* registry.resolve(sessionId);
  }).pipe(
    // FIX B (revised #76): a transient store read failure is a DEFECT, never folded into
    // SessionNotFound — the conflation let the sessionEvents path mis-classify a LIVE
    // session as settled and silently drop its live tail. The typed channel stays exactly
    // SessionNotFound; only a genuine registry miss produces it.
    Effect.catchTag("StateStoreError", (error) => Effect.die(error)),
  );

/**
 * Serve the contract's `sessionEvents` RPC as a UNIFIED replay-then-tail — the
 * session-channel mirror of {@link resyncEvents}, serving BOTH the settled-transcript replay
 * AND the live driving modality over ONE channel. Each streamed item is an
 * {@link OffsetSessionEvent} whose offset is PRESENT for a durable transcript-grade event
 * (replay and live tail share ONE coordinate space — the session fold journals and the
 * decorator publishes the same offset it appended — so a client resumes from any offset-bearing
 * item's offset) and ABSENT for an ephemeral live delta.
 *
 * The flow:
 *
 * - EAGERLY subscribe to the live {@link SessionEvents} feed BEFORE reading the durable log,
 *   so a durable entry committed between the read and the live hand-over is not lost
 *   (subscribe-before-replay). The boundary overlap it can produce (a durable event delivered
 *   by BOTH the replay and the live subscription) is eliminated on the live path by filtering
 *   the live tail to offsets STRICTLY ABOVE the replay high-water — because the id-keyed
 *   consumer reconciliation cannot dedup an id-less `Notice`.
 * - Gate liveness through the SHARED {@link resolveLive} durable-state gate (used ONLY to
 *   pick live-tail-vs-complete — the durable replay is independent of it): a mid-dispatch
 *   session resolves its handle (bounded wait), a settled/absent session fails fast.
 * - Existence: a session is viewable if it is LIVE, has a durable transcript, OR has a SETTLED
 *   (terminal) Session row — the last lets a settled session that emitted ZERO durable events
 *   replay an EMPTY transcript and complete. A NON-terminal row with no handle and no transcript
 *   is a mid-dispatch session whose handle never registered → `SessionNotFound` (retry), and a
 *   session with no row at all never existed → `SessionNotFound`.
 * - Replay the session's durable transcript from a SINGLE in-memory snapshot (entries strictly
 *   after `sinceOffset`; absent → the ORIGIN), strictly ordered by offset. Replay is
 *   DURABLE-ONLY: only offset-bearing events were persisted, so a reconnect never re-delivers
 *   (or needs to re-derive) an ephemeral delta. The same snapshot fixes the live-tail overlap
 *   high-water, so replay and filter agree exactly (no split-read race).
 * - If the session is LIVE, CONTINUE with the live tail — new durable entries (offset-stamped,
 *   above the replay high-water) AND ephemeral deltas (offset-less) fanned out on the feed as
 *   the fold runs, filtered to this session, interleaved in emission order. If the session is
 *   SETTLED, the fold has ended and the snapshot holds the WHOLE transcript — replay it and the
 *   stream COMPLETES (never a spurious `SessionNotFound` for a settled session, the gap this
 *   closes).
 *
 * The stream's error channel is exactly `SessionNotFound | ResyncRequired`, two INDEPENDENT
 * questions: does this session exist, and is the client's cursor from THIS store generation.
 * A resume is refused on GENERATION before the existence verdict and on EXTENT only after it,
 * so an unknown session id under the current generation answers `SessionNotFound` rather than
 * escalating to a whole-store resync.
 * The durable read is `orDie`'d and the liveness gate turns its transient store failures into
 * DEFECTS (#76) — never a `SessionNotFound` that `succeedNone` would silently collapse into a
 * settled replay, dropping a live session's tail — so no store hiccup leaks past the frozen
 * contract.
 */
const resyncSessionEvents = (
  store: Store,
  sessionFeed: SessionFeed,
  registry: Registry,
  sessionId: SessionId,
  resume?: ResumeContext,
): Stream.Stream<OffsetSessionEvent, SessionEventsError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Subscribe BEFORE the durable read (subscribe-before-replay), so a durable entry
      // committed during the read still reaches the live tail on the LIVE path.
      const subscription = yield* sessionFeed.subscribe;
      // GENERATION GATE — the same guard the work-graph `events` feed applies, for the
      // same reason: `session_event_log` is `AUTOINCREMENT` too, and a schema-version
      // bump DROPS it, restarting per-session offsets at `1`. A cursor is therefore a
      // coordinate in ONE generation's space, and an extent check alone cannot say so
      // (once a new generation's transcript outgrows the stale mark, `sinceOffset <=
      // max` holds and the resume looks perfectly valid). So the cursor and its
      // generation arrive as one inseparable `ResumeContext`, and a PRESENT one whose
      // generation is not the current one is refused at EVERY offset — with the extent
      // as a cheap secondary. An ABSENT `resume` is the origin request: it names no
      // coordinate and is valid in any generation. That absence is the only exemption;
      // there is no numeric one, so a dead generation paired with `sinceOffset: 0`
      // is refused exactly like any other stale resume.
      //
      // ONLY the generation half runs before the existence verdict — a dropped store's
      // session is a resync, not a "never existed", and that verdict does not depend on
      // the session existing. The extent half is deferred BELOW the existence check,
      // because unlike the global `event_log` a per-session extent of `0` has a benign
      // IN-generation meaning: "no such session". Refusing there would answer one
      // unknown session id with `ResyncRequired` — "discard ALL retained state and
      // re-hydrate" — when `SessionNotFound` is the honest and far narrower verdict.
      // The extent read is the store's indexed `sessionLog.maxOffset`, so a request
      // about to be refused on generation never decodes a transcript first.
      if (resume !== undefined && resume.generation !== store.generation) {
        const maxOffset = yield* store.sessionLog.maxOffset(sessionId).pipe(Effect.orDie);
        return Stream.fail<SessionEventsError>(
          new ResyncRequired({
            sinceOffset: resume.sinceOffset,
            maxOffset,
            generation: store.generation,
          }),
        );
      }
      const live = yield* resolveLive(store, registry, sessionId).pipe(
        Effect.asSome,
        Effect.catchTag("SessionNotFound", () => Effect.succeedNone),
      );
      const session = yield* store.jobs.getSession(sessionId).pipe(Effect.orDie);
      // ONE durable snapshot serves BOTH the replay AND the live-tail overlap boundary (no
      // double read). Replay = entries strictly after the client's cursor;
      // `maxReplayedOffset` is the EXACT high-water the live tail must exclude, taken from the
      // SAME snapshot the replay is built from so the two agree (no split-read race).
      const fromOffset = resume?.sinceOffset ?? 0;
      const durable = yield* store.sessionLog.read(sessionId).pipe(Effect.orDie);
      // Existence: a session is viewable if it is LIVE, has a durable transcript, OR has a
      // SETTLED (terminal) Session row — the last lets a settled session that emitted ZERO
      // durable events replay an EMPTY transcript and COMPLETE (not error). A NON-terminal row
      // with no live handle and no transcript is a mid-dispatch session whose handle never
      // registered → `SessionNotFound` (a transient the client retries), NOT a settled empty.
      const settledRow = Option.isSome(session) && !isLiveSessionRow(session.value.status);
      if (Option.isNone(live) && durable.length === 0 && !settledRow) {
        return Stream.fail<SessionEventsError>(new SessionNotFound({ id: sessionId }));
      }
      // EXTENT half of the resume guard, deferred to here so it speaks only about a
      // session that EXISTS: a cursor past this transcript's end under the CURRENT
      // generation is a client holding coordinates the store cannot honour. Taken from
      // the SAME snapshot the replay is built from (no second read, no split-read race).
      const durableExtent = durable.at(-1)?.offset ?? 0;
      if (resume !== undefined && resume.sinceOffset > durableExtent) {
        return Stream.fail<SessionEventsError>(
          new ResyncRequired({
            sinceOffset: resume.sinceOffset,
            maxOffset: durableExtent,
            generation: store.generation,
          }),
        );
      }
      const replayEntries = durable.filter((entry) => entry.offset > fromOffset);
      const maxReplayedOffset = replayEntries.at(-1)?.offset ?? fromOffset;
      const replay = Stream.fromIterable(
        replayEntries.map(
          (entry): OffsetSessionEvent => ({ offset: entry.offset, event: entry.event }),
        ),
      );
      // Settled: the durable log is the whole transcript — replay (possibly empty) and complete.
      if (Option.isNone(live)) return replay;
      // Live: replay, then tail new durable entries + ephemeral deltas for THIS session (one
      // coordinate space). Bound the live tail by the handle's terminal `result`, so the stream
      // COMPLETES when the session settles (mirroring the old `handle.events`) rather than
      // hanging open on the durable feed forever. The result's transport failure becomes a
      // defect (`orDie`), keeping the error channel exactly `SessionNotFound`.
      const handle = live.value;
      const liveTail = Stream.fromSubscription(subscription).pipe(
        Stream.filter((item) => item.sessionId === sessionId),
        // Drop durable items ALREADY covered by the replay snapshot (offset ≤ the replay
        // high-water), so a durable event committed in the subscribe→read window is not
        // delivered TWICE — the consumer's id-keyed reconciliation cannot dedup an id-less
        // `Notice`. Ephemeral deltas (no offset) are live-only and never replayed, so they
        // always pass.
        Stream.filter((item) => item.offset === undefined || item.offset > maxReplayedOffset),
        // Forward BOTH modalities interleaved in emission order: a durable entry keeps its
        // offset (advances the resume cursor), an ephemeral delta stays offset-less (the key
        // is omitted, not set to `undefined` — `exactOptionalPropertyTypes`).
        Stream.map(
          (item): OffsetSessionEvent =>
            item.offset === undefined
              ? { event: item.event }
              : { offset: item.offset, event: item.event },
        ),
        Stream.interruptWhen(handle.result.pipe(Effect.orDie)),
      );
      return Stream.concat(replay, liveTail);
    }),
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
    const host = yield* CodeHost;
    const feed = yield* WorkGraphEvents;
    const sessionFeed = yield* SessionEvents;
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
      // durable offset (CE2.0), and replay + live offsets share one
      // coordinate space, so the client can feed a streamed item's offset back as
      // `resume.sinceOffset`. The resume context is OPTIONAL: a request with NO
      // `resume` (a present but empty `{}` payload) replays from origin, present
      // resumes strictly after that offset, over the same `resyncFrom` primitive. The
      // strict `> sinceOffset` ordering is scoped to that resume; within one stream the
      // subscribe-before-replay boundary can overlap (harmless under upsert).
      // A PRESENT `resume` always carries the generation its cursor was minted under —
      // they are one value, not two optional keys — and it is refused unless that
      // generation is the CURRENT one, at EVERY offset, so a cursor minted before a
      // drop-and-recreate can never be resumed incrementally against the new log
      // (`ResyncRequired`).
      events: ({ resume }) => resyncEvents(store, feed, resume),
      createWorkstreamFromPlan: ({ plan }) => materialize(store, host, plan),
      control: ({ workstreamId, action }) =>
        controlWorkstream(store, runner, scope, workstreamId, action),
      retryIssue: ({ issueId }) => retry(store, runner, scope, issueId),
      // Session channel — AE4.2. `sessionSend`/`interrupt`/`answerUiRequest`
      // resolve the SAME live session through `resolveLive` (the durable-state gate:
      // mid-dispatch → bounded wait, settled/absent → fail fast) and bridge its neutral
      // `SessionHandle` surface; a miss is the contract's `SessionNotFound` — a settled
      // session is read-only. `sessionEvents` gains DURABLE replay: it replays the session's
      // durable transcript from the client's `sinceOffset` cursor (`SessionLogStore.tail`,
      // journaled by the store decorator as the fold runs), then — if the session is LIVE —
      // tails new durable entries off the `SessionEvents` feed; a SETTLED session's replay
      // COMPLETES (viewable transcript, no `SessionNotFound`), an absent one is
      // `SessionNotFound`. Each item is an `OffsetSessionEvent` carrying its durable offset
      // (INV-REACTIVE — no poll loop).
      // Its `resume` is the SAME `ResumeContext` as `events`', guarded by the same
      // unconditional generation check — the per-session log is dropped by a schema bump
      // too — so the error channel is `SessionNotFound | ResyncRequired`.
      sessionEvents: ({ sessionId, resume }) =>
        resyncSessionEvents(store, sessionFeed, sessions, sessionId, resume),
      sessionSend: ({ sessionId, input }) => driveInput(store, sessions, sessionId, input),
      interrupt: ({ sessionId }) => abortTurn(store, sessions, sessionId),
      answerUiRequest: ({ sessionId, response }) => answerUi(store, sessions, sessionId, response),
    };
  }),
);
