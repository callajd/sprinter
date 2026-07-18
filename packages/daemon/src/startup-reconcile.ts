/**
 * `StartupReconcile` — the daemon's restart-safety service (Track A, task AE5.1).
 * It is what makes the daemon survive a restart: durable state comes back from the
 * {@link StateStore}, status is reconciled against the {@link Repository} code host,
 * and a Job that was in flight is resumed through the {@link JobRunner} — without
 * loss and without a double-run.
 *
 * It depends ONLY on the three PORTS — `StateStore` / `Repository` / `JobRunner`
 * (INV-PORT) — never on a concrete backing, HTTP client, or `pi` process. A
 * consumer chooses the backings by providing adapter `Layer`s; the logic here is
 * exercised offline against `layerMemory` + a real tmpfile SQLite `layer` + fakes
 * ({@link ./startup-reconcile.test.ts}).
 *
 * {@link StartupReconcile.run} performs the startup sequence, in order:
 *
 * 1. **Reconcile + roll-up** — for every persisted Workstream, reconcile against the
 *    host and roll Issue/PR landings up into Epic/Workstream `WorkStatus`, reusing
 *    `reconcileWorkstream` one-directionally (D13). Per-issue host failures are
 *    isolated inside the reconciler (AE3.2 / #27 F4), so one 404/403/429 never
 *    aborts the whole roll-up.
 * 2. **Resume running Jobs** — walk the (post-roll-up) graph and re-dispatch each
 *    `running` Job through the `JobRunner`, which re-attaches to the Job's PERSISTED
 *    session id (1 Job = 1 session, `UNIQUE(session.jobId)`) — never a new session.
 *    Each resume is a BACKGROUND fiber tied to the daemon scope (a session can take
 *    minutes; boot must not block, mirroring the live `dispatchInBackground` path), so
 *    N in-flight sessions resume concurrently and `run` returns promptly.
 *
 * The resume is guarded so it never double-runs, respects control state, and leaves no
 * durable `running` limbo — a `running` Job that is NOT resumed is settled to a
 * terminal/pending status instead of being left `running` forever:
 *
 * - a Job that is NOT `running` (an already-terminal `succeeded`/`failed`/`cancelled`
 *   Job, or a not-yet-started `queued` one) is not a candidate at all;
 * - a Job whose Issue has already landed (reconciled to `done` + merged PR,
 *   {@link isIssueLanded}) is settled to `succeeded` — its work landed, not re-run;
 * - a Job of a Workstream persisted `cancelled` is settled to `cancelled`; one of a
 *   `blocked` (paused) Workstream is settled to `queued` so a later `control resume`
 *   re-dispatches it — a cancelled/paused Workstream stays that way across a restart
 *   (AE4.1 / #30 N1; the distinct terminal `cancelled` status is CE5.1).
 *
 * A single resume failure is isolated (logged in its background fiber) so one bad Job
 * never disturbs the others; a {@link StateStoreError} reading/writing the durable
 * graph is NOT isolated — our own store failing at startup is a real failure the
 * caller must see.
 *
 * A resumed Job whose session was mid-turn is re-driven from the start (the runner
 * re-issues the prompt, reusing the session id) — idempotent full re-run is the AE5.1
 * behavior; true mid-session continuation rides the deferred LocalPi adapter below.
 *
 * **Scope note — deferred provisioning.** The concrete LocalPi `ExecutionRunner`
 * adapter that spawns a real `pi` process (the `JobRunner`'s runtime) and a runnable
 * daemon `main` entrypoint that wires these ports to their production adapters and
 * calls `run` on boot are **provisioning**, tracked as deferred in
 * `docs/decisions.md` / architecture §10 — NOT part of AE5.1. This module is the
 * persist/reconcile/re-dispatch LOGIC, wired to the ports and tested offline.
 */
import { Context, Effect, Layer } from "effect";
import { type Issue, isIssueLanded, type Job, type JobId, type WorkStatus } from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { reconcileWorkstream, Repository } from "@sprinter/repository";
import { StateStore, type StateStoreError } from "@sprinter/state";

/**
 * The outcome of a {@link StartupReconcile.run}: how many Workstreams were
 * reconciled, and — among the `running` Jobs found — which were re-dispatched
 * (`resumed`) and which were held back (`skipped`, for a landed Issue or a
 * terminal/paused Workstream). Terminal/`queued` Jobs are not candidates and appear
 * in neither list. The report is durable-state-derived and drives the daemon's
 * startup log.
 */
export interface StartupSummary {
  /** The number of persisted Workstreams reconciled against the host. */
  readonly reconciledWorkstreams: number;
  /**
   * The ids of the `running` Jobs **re-dispatched** onto their persisted sessions.
   * Each is handed to the `JobRunner` as a BACKGROUND fiber (a session can take
   * minutes — boot must not block on it, mirroring the live dispatch path), so this
   * counts what was re-dispatched, NOT what has completed; a background resume's
   * outcome is logged asynchronously, never awaited here.
   */
  readonly resumed: ReadonlyArray<JobId>;
  /**
   * The ids of the `running` Jobs NOT resumed — each reconciled to a terminal/pending
   * status so no durable `running` limbo survives: a landed Issue's Job → `succeeded`,
   * a `cancelled` Workstream's Job → `cancelled`, a `blocked` (paused) Workstream's Job
   * → `queued` (so a later `control resume` re-dispatches it).
   */
  readonly skipped: ReadonlyArray<JobId>;
}

/**
 * What restart does with a `running` Job given its (post-roll-up) Issue and its
 * Workstream's control state: resume it in the background, or settle its durable row
 * to a terminal/pending status so it never lingers as a stale `running` across
 * restarts. A landed Issue's work succeeded; a `cancelled` Workstream's Job is
 * cancelled; a `blocked` (paused) Workstream's Job is re-queued to resume on the next
 * `control`.
 */
type ResumeAction =
  | { readonly _tag: "resume" }
  | { readonly _tag: "settle"; readonly status: "succeeded" | "cancelled" | "queued" };

const decideRunning = (issue: Issue, workstreamStatus: WorkStatus): ResumeAction =>
  isIssueLanded(issue)
    ? { _tag: "settle", status: "succeeded" }
    : workstreamStatus === "cancelled"
      ? { _tag: "settle", status: "cancelled" }
      : workstreamStatus === "blocked"
        ? { _tag: "settle", status: "queued" }
        : { _tag: "resume" };

/**
 * The {@link StartupReconcile} service PORT (INV-NAMING, `sprinter/<area>/<Name>`):
 * run the whole restart-safety sequence once, yielding a {@link StartupSummary}. The
 * daemon boot depends on THIS service; {@link layer} wires it over the three ports.
 */
export class StartupReconcile extends Context.Service<
  StartupReconcile,
  {
    /**
     * Reconcile every persisted Workstream against the host, roll status up, then
     * resume the in-flight (`running`) Jobs that control state allows — returning a
     * {@link StartupSummary}. Fails only with {@link StateStoreError} (a durable-store
     * read/write failure); host and resume failures are isolated internally.
     */
    readonly run: Effect.Effect<StartupSummary, StateStoreError>;
  }
>()("sprinter/daemon/StartupReconcile") {}

/**
 * The {@link StartupReconcile} implementation over the `StateStore` / `Repository` /
 * `JobRunner` ports (`Layer.effect` + `Service.of`, per conventions). The three
 * ports are captured at construction and re-provided into the reused
 * `reconcileWorkstream` and each `dispatch`; a consumer supplies concrete adapters
 * for them (INV-PORT).
 */
export const layer: Layer.Layer<StartupReconcile, never, StateStore | Repository | JobRunner> =
  Layer.effect(
    StartupReconcile,
    Effect.gen(function* () {
      const store = yield* StateStore;
      const repo = yield* Repository;
      const jobRunner = yield* JobRunner;
      // The daemon's boot scope: background resume fibers are tied to it, so they
      // live for the daemon's lifetime and are interrupted when it stops.
      const scope = yield* Effect.scope;

      /** Re-provide the captured store/host ports into a reconcile effect. */
      const reconcile = (workstreamId: Parameters<typeof reconcileWorkstream>[0]) =>
        reconcileWorkstream(workstreamId).pipe(
          Effect.provideService(StateStore, store),
          Effect.provideService(Repository, repo),
        );

      /**
       * Re-dispatch one Job onto its persisted session as a BACKGROUND fiber tied to
       * the daemon scope. `JobRunner.dispatch` awaits the session's terminal outcome
       * (a run can take minutes), so — exactly as the live command path
       * (`dispatchInBackground`) does — boot must fork it rather than block: N
       * in-flight sessions resume concurrently and `run` returns promptly. The
       * dispatch owns a fresh per-Job scope (the session lifetime); any failure is
       * isolated (logged) so one bad resume never disturbs the others.
       */
      const resume = (job: Job): Effect.Effect<void> =>
        jobRunner.dispatch(job).pipe(
          Effect.scoped,
          Effect.catch((error) =>
            Effect.logWarning(`startup: resume failed for job ${job.id}`, error),
          ),
          Effect.forkIn(scope, { startImmediately: true }),
          Effect.asVoid,
        );

      const run = Effect.gen(function* () {
        // 1. Reconcile + roll up every Workstream (per-issue host errors isolated in
        //    the reconciler). We re-read the rolled-up graph next, but the isolated
        //    per-issue failures are only in the outcomes — surface them here as a
        //    startup warning so a partially-degraded roll-up is not silent (CE1.3).
        const workstreams = yield* store.workGraph.listWorkstreams;
        const outcomes = yield* Effect.forEach(workstreams, (ws) => reconcile(ws.id));
        const skippedIssues = outcomes.flatMap((outcome) => outcome.failures);
        if (skippedIssues.length > 0) {
          yield* Effect.logWarning(
            `startup: ${skippedIssues.length} issue(s) skipped after host errors during roll-up`,
            skippedIssues,
          );
        }

        // 2. Walk the post-roll-up graph and resume the in-flight Jobs control state
        //    allows. Re-read so a Workstream flipped to `done`/`blocked` is respected.
        const rolled = yield* store.workGraph.listWorkstreams;
        const resumed: Array<JobId> = [];
        const skipped: Array<JobId> = [];

        for (const ws of rolled) {
          const epics = yield* store.workGraph.listEpics(ws.id);
          for (const epic of epics) {
            const issues = yield* store.workGraph.listIssues(epic.id);
            for (const issue of issues) {
              const jobs = yield* store.jobs.listJobsForIssue(issue.id);
              for (const job of jobs) {
                if (job.status !== "running") continue;
                const action = decideRunning(issue, ws.status);
                if (action._tag === "resume") {
                  yield* resume(job);
                  resumed.push(job.id);
                } else {
                  // Settle the stale `running` row to a terminal/pending status so it
                  // never survives as durable limbo across restarts.
                  yield* store.jobs.putJob({ ...job, status: action.status });
                  skipped.push(job.id);
                }
              }
            }
          }
        }

        return { reconciledWorkstreams: rolled.length, resumed, skipped };
      });

      return StartupReconcile.of({ run });
    }),
  );
