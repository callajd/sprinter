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
 *
 * The resume is guarded so it never double-runs and always respects control state:
 *
 * - a Job that is NOT `running` (an already-terminal `succeeded`/`failed`/`cancelled`
 *   Job, or a not-yet-started `queued` one) is not re-dispatched;
 * - a Job whose Issue has already landed (reconciled to `done` + merged PR,
 *   {@link isIssueLanded}) is not re-run;
 * - no Job of a Workstream persisted as terminal/paused (`done`/`blocked`) is
 *   re-dispatched — a cancelled/paused Workstream stays that way across a restart
 *   (AE4.1 / #30 N1).
 *
 * A single resume failure is isolated (logged, skipped) so one bad Job never aborts
 * the startup; a {@link StateStoreError} reading the durable graph is NOT isolated —
 * our own store failing at startup is a real failure the caller must see.
 *
 * **Scope note — deferred provisioning.** The concrete LocalPi `ExecutionRunner`
 * adapter that spawns a real `pi` process (the `JobRunner`'s runtime) and a runnable
 * daemon `main` entrypoint that wires these ports to their production adapters and
 * calls `run` on boot are **provisioning**, tracked as deferred in
 * `docs/decisions.md` / architecture §10 — NOT part of AE5.1. This module is the
 * persist/reconcile/re-dispatch LOGIC, wired to the ports and tested offline.
 */
import { Context, Effect, Layer } from "effect";
import { type Issue, isIssueLanded, type Job, type JobId } from "@sprinter/domain";
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
  /** The ids of the `running` Jobs re-dispatched onto their persisted sessions. */
  readonly resumed: ReadonlyArray<JobId>;
  /** The ids of the `running` Jobs held back (landed Issue, or `done`/`blocked` Workstream). */
  readonly skipped: ReadonlyArray<JobId>;
}

/** Whether a Workstream's control state permits resuming its Jobs on restart. */
const isResumable = (status: string): boolean => status !== "done" && status !== "blocked";

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

      /** Re-provide the captured store/host ports into a reconcile effect. */
      const reconcile = (workstreamId: Parameters<typeof reconcileWorkstream>[0]) =>
        reconcileWorkstream(workstreamId).pipe(
          Effect.provideService(StateStore, store),
          Effect.provideService(Repository, repo),
        );

      /**
       * Re-dispatch one Job onto its persisted session. The dispatch owns a fresh
       * per-Job scope (the session lifetime), and any dispatch failure is isolated —
       * logged and swallowed — so one bad resume never aborts the startup.
       */
      const resume = (job: Job): Effect.Effect<void> =>
        jobRunner.dispatch(job).pipe(
          Effect.scoped,
          Effect.catch((error) =>
            Effect.logWarning(`startup: resume failed for job ${job.id}`, error),
          ),
          Effect.asVoid,
        );

      /** True when a `running` Job should be re-dispatched given its Issue + Workstream. */
      const shouldResume = (job: Job, issue: Issue, workstreamStatus: string): boolean =>
        job.status === "running" && isResumable(workstreamStatus) && !isIssueLanded(issue);

      const run = Effect.gen(function* () {
        // 1. Reconcile + roll up every Workstream (per-issue host errors isolated in
        //    the reconciler). Discard the results; we re-read the rolled-up graph next.
        const workstreams = yield* store.workGraph.listWorkstreams;
        yield* Effect.forEach(workstreams, (ws) => reconcile(ws.id), { discard: true });

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
                if (shouldResume(job, issue, ws.status)) {
                  yield* resume(job);
                  resumed.push(job.id);
                } else {
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
