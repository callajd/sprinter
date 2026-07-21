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
 * - `retryIssue` — re-dispatch an issue's Job, reusing its SAME execution id.
 *
 * The execution-channel procedures (`executionEvents` / `executionSend` / `interrupt` /
 * `answerUiRequest`) bridge a LIVE `@sprinter/runner` {@link ExecutionHandle}, resolved
 * through the {@link ExecutionRegistry} PORT (AE4.2) via the shared {@link resolveLive}
 * helper. A registry entry only lives for its execution's run scope, so the helper gates
 * on DURABLE state (`StateStore`) to pick wait-vs-fail-fast — keying on the durable
 * JOB status (execution → job → job-status), the dispatch unit `startup-reconcile`
 * maintains (CE4.1 FIX A):
 *
 * - an execution whose durable `Job` is still NON-TERMINAL (`queued`/`running`) is
 *   genuinely MID-DISPATCH: the `running` delta is fanned out BEFORE
 *   `ExecutionRunner.run` registers the handle, so a client reacting to it can arrive
 *   before registration. The helper routes it to `ExecutionRegistry.resolve`, which
 *   bounded-waits for the handle to land rather than returning a spurious
 *   `ExecutionNotFound` (so the app needs no retry);
 * - an execution whose durable Job is TERMINAL (`succeeded`/`failed`/`cancelled`; the
 *   Inspector opens channels for SETTLED jobs by design, BE4.1) OR whose Job/Execution
 *   row does not exist is routed to `ExecutionRegistry.get`, which FAILS FAST with
 *   `ExecutionNotFound` — no multi-second stall waiting for a registration that will
 *   never come. Keying on the Job (not the Execution row) closes the crash-orphaned case:
 *   `startup-reconcile` settles a stale Job it cannot resume by writing ONLY the Job
 *   row, leaving its Execution row NON-TERMINAL — an Execution-row gate would stall on it.
 *
 * They carry ONLY owned neutral types (`ExecutionEvent` / `ExecutionInput` /
 * `UiResponse`) — no Pi wire type reaches this surface (INV-BOUNDARY). `executionSend` /
 * `interrupt` / `answerUiRequest` stay LIVE-only (a settled execution is read-only). Only
 * `executionEvents` gains DURABLE replay: it replays the execution's durable
 * transcript from the client's `sinceOffset` cursor then live-tails new durable entries if
 * the execution is running, or COMPLETES if it has settled — the execution-channel mirror of
 * the `events` offset-resync, so a SETTLED execution's transcript is viewable in the Inspector
 * rather than an `ExecutionNotFound`. It streams off the durable log + `ExecutionEvents` feed,
 * never a poll (INV-REACTIVE). The handle's infrastructure failures (`PiTransportError` /
 * `PiRpcError`) are NOT contract errors, so they are turned into defects and the
 * contract's error channel carries only genuine contract outcomes: `ExecutionNotFound`
 * for `executionSend`/`interrupt`/`answerUiRequest`, and `ExecutionNotFound |
 * ResyncRequired` for `executionEvents` — whose `sinceOffset` is a per-execution durable
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
  ExecutionNotFound,
  IssueNotFound,
  type OffsetExecutionEvent,
  PlanRejected,
  type ResumeContext,
  ResyncRequired,
  type Snapshot,
  SprinterRpc,
  WorkstreamNotFound,
  type WorkstreamPlan,
} from "@sprinter/contract";
import {
  type Epic,
  type Execution,
  type ExecutionId,
  type ExecutionInput,
  isExecutionLive,
  type Issue,
  type IssueId,
  Job,
  type Repository,
  type UiResponse,
  type WorkStatus,
  type Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { CodeHost, type CodeHostFailure } from "@sprinter/repository";
import type { ExecutionHandle } from "@sprinter/runner";
import { StateStore } from "@sprinter/state";
import { resyncEvents } from "./event-journal.ts";
import { ExecutionEvents } from "./execution-events.ts";
import { ExecutionRegistry } from "./execution-registry.ts";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Runner = Context.Service.Shape<typeof JobRunner>;
type Host = Context.Service.Shape<typeof CodeHost>;
type Registry = Context.Service.Shape<typeof ExecutionRegistry>;
type ExecutionFeed = Context.Service.Shape<typeof ExecutionEvents>;

/**
 * The `executionEvents` error channel: two INDEPENDENT refusals — the execution is not
 * one this daemon knows ({@link ExecutionNotFound}), or the client's resume cursor is
 * not from this store generation ({@link ResyncRequired}). Named so the stream's
 * failures widen to the whole channel rather than being inferred one branch at a time.
 */
type ExecutionEventsError = ExecutionNotFound | ResyncRequired;

// ── snapshot ────────────────────────────────────────────────────────────────

/**
 * Build the contract {@link Snapshot} by traversing the persisted work graph:
 * every workstream, its epics (by FK), their issues (by FK), each issue's jobs,
 * and each job's execution. Reuses the store's FK-scoped reads so the result is
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
    const executions: Array<Execution> = [];
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
            // The WHOLE tree, not just the root. `putExecution` journals an
            // `ExecutionChanged` delta for EVERY execution, so a snapshot carrying only
            // `getExecutionForJob`'s root would ship strictly less than the delta stream
            // that follows it — and a client that reconnected would hold a different read
            // model than one that never dropped, which is precisely what the
            // snapshot-then-deltas contract exists to rule out.
            executions.push(...(yield* store.jobs.listExecutionsForJob(job.id)));
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
      executions,
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

/**
 * Derive a stable, url-safe workstream-id slug from the plan name.
 *
 * KNOWN, TRACKED: this is LOSSY on the plan NAME — every run of non-alphanumerics
 * collapses to one `-` and case is folded — so `"Fix A/B"` and `"fix a b"` yield the
 * same slug and the second plan is rejected as a duplicate of the first, for the same
 * repository. It predates DE1.2 (the entity work only changed the REPOSITORY half of
 * the id, which is now injective) and is tracked as issue #95; it is deliberately not
 * fixed here.
 */
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
 * A host that could not be ASKED is a different outcome from a host that answered "no
 * such repository", and it stays that way — but it is a REJECTION, not a defect. Both
 * are `PlanRejected`, and the `reason` is what keeps them apart. Reporting "that
 * repository does not exist" for an unreachable host would be a lie the user would act
 * on; killing the request fiber over a transient upstream 500 would be worse — a plan
 * that was not materialised because GitHub hiccupped is an ordinary, expected outcome
 * of talking to a network service, and `PlanRejected` already carries the vocabulary to
 * say so.
 *
 * ## Only the RETRYABLE failure invites a retry
 *
 * "The host could not be asked" is itself three outcomes, and they take different
 * reasons, because the reason is the only thing the user can act on. `CodeHostError`
 * carries the closed `CodeHostFailure` that says which (INV-SUM), so this is a branch on
 * DATA, not on prose:
 *
 * - `unreachable` — a transport failure, a timeout, a 5xx. Transient and upstream, so
 *   the reason says RETRY.
 * - `denied` — a 401 (the daemon's token is absent, wrong or expired) or a 403 (the
 *   token lacks access, or the rate limit is spent). Retrying with the same token
 *   reproduces it exactly, so the reason says the host REFUSED and points at the
 *   credential.
 * - `unusable` — the host answered something Sprinter could not read (a body that failed
 *   `GhRepo`, a value that failed an owned schema). Also deterministic, and it points at
 *   Sprinter or at a host contract change rather than at anything the user did.
 *
 * Telling a user with an expired token to "retry" is a false instruction that produces
 * an unbounded retry loop and no progress, which is exactly what the earlier single
 * message did.
 *
 * ## `putRepository` fails as a REJECTION for the one HOST-caused collision
 *
 * A `StateStoreError` is normally a DEFECT here, and the rest of this file keeps that
 * convention: it means Sprinter's own store is broken, which is not an outcome the user
 * can act on or a plan can be corrected for.
 *
 * This one write is the documented exception, because at THIS call site the failure has
 * a second, entirely non-broken cause. The store holds `(host, owner, name)` UNIQUE, and
 * a record that was observed once and NEVER REFRESHED (the KNOWN GAP on
 * `@sprinter/domain`'s `Repository`) goes on occupying a natural key the host has since
 * FREED. So when repository B is renamed `callajd/x` → `callajd/y` upstream and
 * repository A is then renamed INTO `callajd/x`, storing A collides with our stale row
 * for B: the ids differ, so the id-keyed upsert does not fire, and the write fails —
 * permanently, on every retry, for a repository that is entirely valid on the host. The
 * trigger is ordinary GitHub behaviour, the store is intact, and nothing the daemon can
 * do at this instant fixes it. `packages/state/src/store.test.ts` pins the store half;
 * `./rpc-handlers.test.ts` pins it up through this RPC.
 *
 * Dying on it would deliver a client an unmodelled defect for a condition the contract
 * has a word for — the Swift client models `PlanRejected`, not a `Cause([Die(...)])` —
 * so it is REJECTED, with a reason that names the natural key so the user can see which
 * repository is in the way. Every OTHER `StateStoreError` from this write is genuinely a
 * broken store; a rejection is the strictly safer misclassification of the two (a
 * rejection that should have been a defect writes nothing and says so, while a defect
 * that should have been a rejection kills a live request).
 *
 * The reason is therefore phrased for the WHOLE set of errors that can reach it, not for
 * the collision alone. `StateStoreError` carries no discriminator, so this branch CANNOT
 * tell a natural-key collision from a locked database, a full disk or a decode failure —
 * a reason asserting "another repository is already recorded there" would be flatly false
 * for the others. It states what was ATTEMPTED and what the LIKELY cause is, and leaves
 * the store's own availability on the table. Issue **#97** is the real fix: carry the
 * distinction as DATA on `StateStoreError`, exactly the way `CodeHostFailure` already
 * does on the code-host port, after which this branch can name the cause it has instead
 * of hedging across all of them.
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
    const named = `${plan.repository.owner}/${plan.repository.name}`;
    /** The rejection for a host that could not be asked, phrased per {@link CodeHostFailure}. */
    const notAsked = (kind: CodeHostFailure, detail: string) =>
      new PlanRejected({
        reason:
          kind === "unreachable"
            ? `the code host could not be reached to resolve ${named} (${detail}); the plan was not materialized — retry`
            : kind === "denied"
              ? `the code host refused Sprinter's request to resolve ${named} (${detail}); the plan was not materialized — check the daemon's code-host credentials, or wait for the rate limit to reset; retrying now will not help`
              : `the code host answered with something Sprinter could not read while resolving ${named} (${detail}); the plan was not materialized, and retrying will not change the answer`,
      });
    const observed = yield* host.repositories.resolve(plan.repository).pipe(
      Effect.mapError((error) => notAsked(error.kind, error.detail)),
      Effect.timeoutOrElse({
        duration: RESOLVE_TIMEOUT,
        // A timeout IS the unreachable case: the host never answered.
        orElse: () =>
          Effect.fail(
            notAsked("unreachable", `no response within ${Duration.format(RESOLVE_TIMEOUT)}`),
          ),
      }),
    );
    if (Option.isNone(observed)) {
      // The host answered "not found", and that answer is AMBIGUOUS by design on GitHub:
      // it returns 404 — deliberately NOT 403 — for a repository the token cannot see, so
      // it does not confirm the existence of a private repository to an unauthorized
      // caller. Org SSO not authorised for the token, a missing `repo` scope, and a
      // fine-grained PAT that does not select this repository all arrive here as 404, and
      // those are the COMMONEST credential failures on this path. Saying "does not exist"
      // would name a diagnosis this outcome cannot support and send the user off to check
      // their spelling while the real fix is the daemon's token.
      return yield* Effect.fail(
        new PlanRejected({
          reason: `the code host has no repository ${named} that Sprinter can see — either it does not exist, or the daemon's code-host token cannot access it (a code host answers 404, not 403, for a private repository a token may not see); check the name, then check the token's access. The plan was not materialized`,
        }),
      );
    }
    // Every `StateStoreError` from this write reaches here, not just the natural-key
    // collision — see this function's docstring. So the reason says what we ATTEMPTED and
    // offers the collision as the LIKELY cause rather than asserting it: a locked
    // database, a full disk or a decode failure would otherwise be reported to the user
    // as "another repository is already recorded there", which is simply false. #97 is the
    // real fix — carrying the distinction as DATA on `StateStoreError`, the way
    // `CodeHostFailure` already does on the code-host port — after which this branch can
    // name the cause it actually has.
    yield* store.repositories.putRepository(observed.value).pipe(
      Effect.mapError(
        () =>
          new PlanRejected({
            reason: `Sprinter could not record its observation of ${observed.value.owner}/${observed.value.name}; most likely another repository still holds that name in Sprinter's store after a rename on the host, but the store may also be unavailable. The plan was not materialized`,
          }),
      ),
    );
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
 * separators inside a `RepositoryId`; nothing parses it back out. The PLAN-NAME half of
 * the slug is still lossy — see {@link slugify} and issue #95.
 *
 * Deriving it from the RESOLVED record means the repository is observed BEFORE the
 * duplicate check, and the ORDER is forced: the id cannot be computed without
 * `repository.id`, so moving the duplicate check earlier is not available. A plan
 * rejected as a duplicate has therefore refreshed that repository's observation. That is
 * not a write the rejection was supposed to avoid: the D6 hazard is fabricating a row
 * for a repository nobody observed, and this is the opposite — a real observation of a
 * repository that necessarily already had a row, since the workstream the duplicate
 * check found holds a FOREIGN KEY to it.
 *
 * It does mean a client retry-looping on the duplicate rejection re-puts that record on
 * every attempt. The row write is harmless (it is an id-keyed replace of a record with
 * itself), and the DELTA it would otherwise emit — an append to the durable, untrimmed
 * event log plus a broadcast to every client — is suppressed by the journaling
 * decorator, which journals a repository put only when the observation actually changed
 * (`./event-journal.ts`).
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
 * to the handler-layer scope. A running execution can take minutes, so the command
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
 * node but do NOT interrupt an in-flight execution (that rides on the execution
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
 * the issue's most recent persisted Job — which carries its `executionId`, so the
 * `JobRunner` re-attaches to the SAME execution (the runner reuses the id; the store no
 * longer constrains it to one) — or mints a
 * fresh implement Job when the issue has never been dispatched.
 *
 * "Most recent" is `listJobsForIssue`'s last row (ordered by id); under the current
 * one-job-per-issue `job-<issueId>` scheme that IS the issue's only Job. A retry of
 * a Job still IN FLIGHT (queued/running) is a no-op — re-dispatching it would fork a
 * second `dispatch` racing the same execution rows; retry acts only on a terminal or
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

// ── execution channel (bridge a live ExecutionHandle) ────────────────────────────────────────

/**
 * True while a durable {@link Job} row is still MID-DISPATCH — i.e. genuinely queued
 * or running, so its live {@link ExecutionHandle} either is registered now or is about
 * to be within the register-after-dispatch window. The complement is a TERMINAL job
 * (settled: `succeeded`/`failed`/`cancelled`), whose registry entry has already been
 * torn down and will never reappear. Expressed as a POSITIVE allow-list of the
 * non-terminal statuses so any future status defaults to fail-fast (never
 * re-introducing a spurious multi-second stall on a settled/unknown state).
 *
 * The gate keys on the JOB, not the durable `Execution` row, ON PURPOSE (CE4.1 FIX A):
 * `startup-reconcile` settles a stale `running` Job it cannot resume by writing ONLY
 * the Job row (its `putJob` → terminal), and NEVER settles the Execution row. So after a
 * crash + restart a settled-not-resumed job leaves its Execution row NON-TERMINAL
 * (`starting`/`active`) while its Job is terminal. Gating on the Execution row would
 * mistake that crash-orphaned state for mid-dispatch and stall the full resolve bound;
 * the Job — the dispatch unit reconcile maintains — is the authoritative signal.
 */
const isMidDispatchJob = (status: Job["status"]): boolean =>
  status === "queued" || status === "running";

// A durable `Execution` row is LIVE exactly while its TRANSCRIPT is open
// (`isExecutionLive`, `@sprinter/domain`), so its handle is registered or about to be;
// a SEALED transcript is a run that has ended and whose registry entry is gone for
// good. DE2.2 replaced the `ExecutionStatus` allow-list this used to consult: liveness
// is now ONE value rather than an enum a settle path and a gate had to keep agreeing
// about, so there is no longer a status this predicate could fail to classify.
//
// It remains the belt to `isMidDispatchJob`'s braces (CE4.1-R4): `startup-reconcile`
// can settle a stale `running` Job to `queued` under a paused Workstream — `queued` IS
// mid-dispatch, so a Job-only gate would bounded-WAIT on that orphan and stall the full
// resolve bound. Reconcile SEALS the execution's transcript alongside, so gating on
// BOTH (mid-dispatch Job AND live Execution) fails that `queued`-orphan fast while
// still bridging a genuine mid-dispatch (running Job + live Execution).

/**
 * Resolve the live {@link ExecutionHandle} for a `executionId`, choosing wait-vs-fail-fast
 * on DURABLE state (the shared gate behind all four execution-channel procedures). It
 * resolves execution → job → job-status and gates on the JOB:
 *
 * - read the durable `Execution` row from the {@link StateStore} to recover the `jobId`
 *   it belongs to (1 Job = 1 execution); ABSENT → nothing is or will be registered, fail
 *   fast through `ExecutionRegistry.get`. A row that is itself TERMINAL (not
 *   {@link isExecutionLive live}) also fails fast — it covers the reconcile
 *   `queued`-orphan (CE4.1-R4) whose Job stays mid-dispatch but whose Execution was settled;
 * - read that {@link Job} row; a job that is present AND still {@link isMidDispatchJob
 *   mid-dispatch} (`queued`/`running`) — with a LIVE Execution row — is bridged through
 *   `ExecutionRegistry.resolve`, which bounded-WAITS out the register-after-dispatch window
 *   so a client reacting to the `running` delta needs no retry;
 * - a job that is TERMINAL (settled) OR ABSENT is resolved through
 *   `ExecutionRegistry.get`, which FAILS FAST with {@link ExecutionNotFound} — no
 *   multi-second stall on a registration that will never land. This covers the Inspector
 *   opening channels for SETTLED jobs (BE4.1), the crash-orphaned case (a terminal Job
 *   whose Execution row an older reconcile left NON-TERMINAL), and — via the Execution-row
 *   check above — the `queued`-orphan the Job gate alone would miss.
 *
 * FIX B (CE4.1, revised #76): a transient `StateStoreError` from either durable read is
 * a DEFECT (`Effect.die`), never folded into `ExecutionNotFound`. Conflating a store
 * hiccup with a genuine "no live handle / not mid-dispatch" miss is the #76 bug: the
 * `executionEvents` serving path catches `ExecutionNotFound` to `succeedNone`, so a transient
 * store error on a genuinely LIVE execution would collapse `live` to `None`, replay only the
 * durable prefix, and silently COMPLETE — dropping the live tail. Surfacing the store
 * error as a defect instead fails LOUDLY rather than silently truncating a live stream.
 * The resolved typed error channel stays exactly `ExecutionNotFound` (infra failures cross
 * as defects), and only a genuine registry miss yields `ExecutionNotFound`.
 */
const resolveLive = (
  store: Store,
  registry: Registry,
  executionId: ExecutionId,
): Effect.Effect<ExecutionHandle, ExecutionNotFound> =>
  Effect.gen(function* () {
    const execution = yield* store.jobs.getExecution(executionId);
    // No Execution row → never existed / already torn down; fail fast.
    if (Option.isNone(execution)) return yield* registry.get(executionId);
    // Execution row TERMINAL → registry entry gone for good (settled, or a reconcile
    // `queued`-orphan whose Job stays mid-dispatch, CE4.1-R4); fail fast, no stall.
    if (!isExecutionLive(execution.value)) return yield* registry.get(executionId);
    const job = yield* store.jobs.getJob(execution.value.jobId);
    // Job absent, or terminal → registry entry gone for good; fail fast.
    // Job mid-dispatch (queued/running) AND Execution live → bridge the register-after-
    // dispatch window (bounded wait).
    if (Option.isNone(job) || !isMidDispatchJob(job.value.status)) {
      return yield* registry.get(executionId);
    }
    return yield* registry.resolve(executionId);
  }).pipe(
    // FIX B (revised #76): a transient store read failure is a DEFECT, never folded into
    // ExecutionNotFound — the conflation let the executionEvents path mis-classify a LIVE
    // execution as settled and silently drop its live tail. The typed channel stays exactly
    // ExecutionNotFound; only a genuine registry miss produces it.
    Effect.catchTag("StateStoreError", (error) => Effect.die(error)),
  );

/**
 * Serve the contract's `executionEvents` RPC as a UNIFIED replay-then-tail — the
 * execution-channel mirror of {@link resyncEvents}, serving BOTH the settled-transcript replay
 * AND the live driving modality over ONE channel. Each streamed item is an
 * {@link OffsetExecutionEvent} whose offset is PRESENT for a durable transcript-grade event
 * (replay and live tail share ONE coordinate space — the execution fold journals and the
 * decorator publishes the same offset it appended — so a client resumes from any offset-bearing
 * item's offset) and ABSENT for an ephemeral live delta.
 *
 * The flow:
 *
 * - EAGERLY subscribe to the live {@link ExecutionEvents} feed BEFORE reading the durable log,
 *   so a durable entry committed between the read and the live hand-over is not lost
 *   (subscribe-before-replay). The boundary overlap it can produce (a durable event delivered
 *   by BOTH the replay and the live subscription) is eliminated on the live path by filtering
 *   the live tail to offsets STRICTLY ABOVE the replay high-water — because the id-keyed
 *   consumer reconciliation cannot dedup an id-less `Notice`.
 * - Gate liveness through the SHARED {@link resolveLive} durable-state gate (used ONLY to
 *   pick live-tail-vs-complete — the durable replay is independent of it): a mid-dispatch
 *   execution resolves its handle (bounded wait), a settled/absent execution fails fast.
 * - Existence: an execution is viewable if it is LIVE, has a durable transcript, OR has a SETTLED
 *   (terminal) Execution row — the last lets a settled execution that emitted ZERO durable events
 *   replay an EMPTY transcript and complete. A NON-terminal row with no handle and no transcript
 *   is a mid-dispatch execution whose handle never registered → `ExecutionNotFound` (retry), and a
 *   execution with no row at all never existed → `ExecutionNotFound`.
 * - Replay the execution's durable transcript from a SINGLE in-memory snapshot (entries strictly
 *   after `sinceOffset`; absent → the ORIGIN), strictly ordered by offset. Replay is
 *   DURABLE-ONLY: only offset-bearing events were persisted, so a reconnect never re-delivers
 *   (or needs to re-derive) an ephemeral delta. The same snapshot fixes the live-tail overlap
 *   high-water, so replay and filter agree exactly (no split-read race).
 * - If the execution is LIVE, CONTINUE with the live tail — new durable entries (offset-stamped,
 *   above the replay high-water) AND ephemeral deltas (offset-less) fanned out on the feed as
 *   the fold runs, filtered to this execution, interleaved in emission order. If the execution is
 *   SETTLED, the fold has ended and the snapshot holds the WHOLE transcript — replay it and the
 *   stream COMPLETES (never a spurious `ExecutionNotFound` for a settled execution, the gap this
 *   closes).
 *
 * The stream's error channel is exactly `ExecutionNotFound | ResyncRequired`, two INDEPENDENT
 * questions: does this execution exist, and is the client's cursor from THIS store generation.
 * A resume is refused on GENERATION before the existence verdict and on EXTENT only after it,
 * so an unknown execution id under the current generation answers `ExecutionNotFound` rather than
 * escalating to a whole-store resync.
 * The durable read is `orDie`'d and the liveness gate turns its transient store failures into
 * DEFECTS (#76) — never an `ExecutionNotFound` that `succeedNone` would silently collapse into a
 * settled replay, dropping a live execution's tail — so no store hiccup leaks past the frozen
 * contract.
 */
const resyncExecutionEvents = (
  store: Store,
  executionFeed: ExecutionFeed,
  registry: Registry,
  executionId: ExecutionId,
  resume?: ResumeContext,
): Stream.Stream<OffsetExecutionEvent, ExecutionEventsError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Subscribe BEFORE the durable read (subscribe-before-replay), so a durable entry
      // committed during the read still reaches the live tail on the LIVE path.
      const subscription = yield* executionFeed.subscribe;
      // GENERATION GATE — the same guard the work-graph `events` feed applies, for the
      // same reason: `execution_event_log` is `AUTOINCREMENT` too, and a schema-version
      // bump DROPS it, restarting per-execution offsets at `1`. A cursor is therefore a
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
      // execution is a resync, not a "never existed", and that verdict does not depend on
      // the execution existing. The extent half is deferred BELOW the existence check,
      // because unlike the global `event_log` a per-execution extent of `0` has a benign
      // IN-generation meaning: "no such execution". Refusing there would answer one
      // unknown execution id with `ResyncRequired` — "discard ALL retained state and
      // re-hydrate" — when `ExecutionNotFound` is the honest and far narrower verdict.
      // The extent read is the store's indexed `executionLog.maxOffset`, so a request
      // about to be refused on generation never decodes a transcript first.
      if (resume !== undefined && resume.generation !== store.generation) {
        const maxOffset = yield* store.executionLog.maxOffset(executionId).pipe(Effect.orDie);
        return Stream.fail<ExecutionEventsError>(
          new ResyncRequired({
            sinceOffset: resume.sinceOffset,
            maxOffset,
            generation: store.generation,
          }),
        );
      }
      const live = yield* resolveLive(store, registry, executionId).pipe(
        Effect.asSome,
        Effect.catchTag("ExecutionNotFound", () => Effect.succeedNone),
      );
      const execution = yield* store.jobs.getExecution(executionId).pipe(Effect.orDie);
      // ONE durable snapshot serves BOTH the replay AND the live-tail overlap boundary (no
      // double read). Replay = entries strictly after the client's cursor;
      // `maxReplayedOffset` is the EXACT high-water the live tail must exclude, taken from the
      // SAME snapshot the replay is built from so the two agree (no split-read race).
      const fromOffset = resume?.sinceOffset ?? 0;
      const durable = yield* store.executionLog.read(executionId).pipe(Effect.orDie);
      // Existence: an execution is viewable if it is LIVE, has a durable transcript, OR has a
      // SETTLED (terminal) Execution row — the last lets a settled execution that emitted ZERO
      // durable events replay an EMPTY transcript and COMPLETE (not error). A NON-terminal row
      // with no live handle and no transcript is a mid-dispatch execution whose handle never
      // registered → `ExecutionNotFound` (a transient the client retries), NOT a settled empty.
      const settledRow = Option.isSome(execution) && !isExecutionLive(execution.value);
      if (Option.isNone(live) && durable.length === 0 && !settledRow) {
        return Stream.fail<ExecutionEventsError>(new ExecutionNotFound({ id: executionId }));
      }
      // EXTENT half of the resume guard, deferred to here so it speaks only about a
      // execution that EXISTS: a cursor past this transcript's end under the CURRENT
      // generation is a client holding coordinates the store cannot honour. Taken from
      // the SAME snapshot the replay is built from (no second read, no split-read race).
      const durableExtent = durable.at(-1)?.offset ?? 0;
      if (resume !== undefined && resume.sinceOffset > durableExtent) {
        return Stream.fail<ExecutionEventsError>(
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
          (entry): OffsetExecutionEvent => ({ offset: entry.offset, event: entry.event }),
        ),
      );
      // Settled: the durable log is the whole transcript — replay (possibly empty) and complete.
      if (Option.isNone(live)) return replay;
      // Live: replay, then tail new durable entries + ephemeral deltas for THIS execution (one
      // coordinate space). Bound the live tail by the handle's terminal `result`, so the stream
      // COMPLETES when the execution settles (mirroring the old `handle.events`) rather than
      // hanging open on the durable feed forever. The result's transport failure becomes a
      // defect (`orDie`), keeping the error channel exactly `ExecutionNotFound`.
      const handle = live.value;
      const liveTail = Stream.fromSubscription(subscription).pipe(
        Stream.filter((item) => item.executionId === executionId),
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
          (item): OffsetExecutionEvent =>
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
 * Drive an {@link ExecutionInput} into the live execution for the `executionSend` RPC:
 * resolve the {@link ExecutionHandle} through {@link resolveLive} ({@link ExecutionNotFound}
 * on a miss) and call `send`. The handle's `PiRpcError`/`PiTransportError` are
 * infrastructure failures, not contract errors, so they become defects — the error
 * channel is exactly `ExecutionNotFound`.
 */
const driveInput = (
  store: Store,
  registry: Registry,
  executionId: ExecutionId,
  input: ExecutionInput,
): Effect.Effect<void, ExecutionNotFound> =>
  resolveLive(store, registry, executionId).pipe(
    Effect.flatMap((handle) => handle.send(input).pipe(Effect.orDie)),
  );

/**
 * Abort the live execution's in-flight turn for the `interrupt` RPC: resolve the
 * {@link ExecutionHandle} through {@link resolveLive} ({@link ExecutionNotFound} on a miss)
 * and call `interrupt`; the handle's transport failures become defects, not contract
 * errors.
 */
const abortTurn = (
  store: Store,
  registry: Registry,
  executionId: ExecutionId,
): Effect.Effect<void, ExecutionNotFound> =>
  resolveLive(store, registry, executionId).pipe(
    Effect.flatMap((handle) => handle.interrupt.pipe(Effect.orDie)),
  );

/**
 * Answer an outstanding UI request for the `answerUiRequest` RPC, completing the
 * `extension_ui_request` round-trip: resolve the {@link ExecutionHandle} through
 * {@link resolveLive} ({@link ExecutionNotFound} on a miss) and hand the neutral
 * {@link UiResponse} to the live execution via `answerUi` (which is total — it cannot fail).
 */
const answerUi = (
  store: Store,
  registry: Registry,
  executionId: ExecutionId,
  response: UiResponse,
): Effect.Effect<void, ExecutionNotFound> =>
  resolveLive(store, registry, executionId).pipe(
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
    const executionFeed = yield* ExecutionEvents;
    const executions = yield* ExecutionRegistry;
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
      // Execution channel — AE4.2. `executionSend`/`interrupt`/`answerUiRequest`
      // resolve the SAME live execution through `resolveLive` (the durable-state gate:
      // mid-dispatch → bounded wait, settled/absent → fail fast) and bridge its neutral
      // `ExecutionHandle` surface; a miss is the contract's `ExecutionNotFound` — a settled
      // execution is read-only. `executionEvents` gains DURABLE replay: it replays the execution's
      // durable transcript from the client's `sinceOffset` cursor (`ExecutionLogStore.tail`,
      // journaled by the store decorator as the fold runs), then — if the execution is LIVE —
      // tails new durable entries off the `ExecutionEvents` feed; a SETTLED execution's replay
      // COMPLETES (viewable transcript, no `ExecutionNotFound`), an absent one is
      // `ExecutionNotFound`. Each item is an `OffsetExecutionEvent` carrying its durable offset
      // (INV-REACTIVE — no poll loop).
      // Its `resume` is the SAME `ResumeContext` as `events`', guarded by the same
      // unconditional generation check — the per-execution log is dropped by a schema bump
      // too — so the error channel is `ExecutionNotFound | ResyncRequired`.
      executionEvents: ({ executionId, resume }) =>
        resyncExecutionEvents(store, executionFeed, executions, executionId, resume),
      executionSend: ({ executionId, input }) => driveInput(store, executions, executionId, input),
      interrupt: ({ executionId }) => abortTurn(store, executions, executionId),
      answerUiRequest: ({ executionId, response }) =>
        answerUi(store, executions, executionId, response),
    };
  }),
);
