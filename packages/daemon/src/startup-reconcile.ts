/**
 * `StartupReconcile` — the daemon's restart-safety service (Track A, task AE5.1).
 * It is what makes the daemon survive a restart: durable state comes back from the
 * {@link StateStore}, status is reconciled against the {@link CodeHost} code host,
 * and a Job that was in flight is resumed through the {@link JobRunner} — without
 * loss and without a double-run.
 *
 * It depends ONLY on the three PORTS — `StateStore` / `CodeHost` / `JobRunner`
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
 *    execution id — never a new execution. (That re-attachment is the RUNNER's doing, not
 *    a store constraint: DE2.2 dropped `UNIQUE(execution.jobId)`, because a job may own a
 *    TREE of executions.)
 *    Each resume is a BACKGROUND fiber tied to the daemon scope (an execution can take
 *    minutes; boot must not block, mirroring the live `dispatchInBackground` path), so
 *    N in-flight executions resume concurrently and `run` returns promptly.
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
 *   (AE4.1 / #30 N1; the distinct terminal `cancelled` status is CE5.1);
 * - a stray, non-landed `running` Job under a truly-`done` Workstream is NEVER resumed
 *   (CE5-F4): the Workstream finished without it, so it is settled to `cancelled` rather
 *   than resurrected — resume is NARROWED to the genuinely-in-flight (`active`/`pending`)
 *   Workstreams, so every terminal/paused Workstream status settles instead.
 *
 * Every settle path also SEALS the transcript of EVERY Execution the Job owns — BEFORE it
 * writes the Job row (CE4.1-R4 root fix): a settled Job must never leave a LIVE Execution behind,
 * or the execution-resolve gate ({@link ./rpc-handlers.ts} `resolveLive`) would mistake
 * that orphan for a mid-dispatch execution and stall the full resolve bound before
 * `ExecutionNotFound`. "Terminal" for an Execution is a `SealedTranscript` and nothing
 * else — DE2.2 deleted the `ExecutionStatus` enum, so there is no `completed`/`interrupted`
 * status to move a row to and no second field a settle path could leave disagreeing with
 * the transcript (INV-SUM). EVERY execution, not just the root: a job owns a TREE, and a
 * sibling left with a `LiveTranscript` stalls the gate exactly as the root would. A later
 * re-dispatch of a `queued` Job re-attaches the SAME execution id and re-opens its
 * transcript, which never invalidates a cached prefix (offsets only grow). This is
 * INV-RESTART behavior.
 *
 * A single resume failure is isolated (logged in its background fiber) so one bad Job
 * never disturbs the others; a {@link StateStoreError} reading/writing the durable
 * graph is NOT isolated — our own store failing at startup is a real failure the
 * caller must see.
 *
 * A resumed Job whose execution was mid-turn is re-driven from the start (the runner
 * re-issues the prompt, reusing the execution id) — idempotent full re-run is the AE5.1
 * behavior; true mid-execution continuation rides the deferred LocalPi adapter below.
 *
 * **Scope note — deferred provisioning.** The concrete LocalPi `ExecutionRunner`
 * adapter that spawns a real `pi` process (the `JobRunner`'s runtime) and a runnable
 * daemon `main` entrypoint that wires these ports to their production adapters and
 * calls `run` on boot are **provisioning**, tracked as deferred in
 * `docs/decisions.md` / architecture §10 — NOT part of AE5.1. This module is the
 * persist/reconcile/re-dispatch LOGIC, wired to the ports and tested offline.
 */
import { Context, Effect, Layer } from "effect";
import {
  type Issue,
  isExecutionLive,
  isIssueLanded,
  type Job,
  type JobId,
  type WorkStatus,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { reconcileWorkstream, CodeHost } from "@sprinter/repository";
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
   * The ids of the `running` Jobs **re-dispatched** onto their persisted executions.
   * Each is handed to the `JobRunner` as a BACKGROUND fiber (an execution can take
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

/** The Job statuses a settle path moves a stale `running` Job to (never `running`). */
type SettleStatus = "succeeded" | "cancelled" | "queued";

/**
 * What restart does with a `running` Job given its (post-roll-up) Issue and its
 * Workstream's control state: resume it in the background, or settle its durable row
 * to a terminal/pending status so it never lingers as a stale `running` across
 * restarts. A landed Issue's work succeeded; a `cancelled` (or truly-`done`, CE5-F4)
 * Workstream's Job is cancelled; a `blocked` (paused) Workstream's Job is re-queued to
 * resume on the next `control`. Resume is reserved for a genuinely-in-flight
 * (`active`/`pending`) Workstream.
 */
type ResumeAction =
  | { readonly _tag: "resume" }
  | { readonly _tag: "settle"; readonly status: SettleStatus };

/**
 * Decide a `running` Job's fate. Resume is NARROWED (CE5-F4) to the genuinely-in-flight
 * Workstream statuses (`active`/`pending`); every terminal/paused status settles instead,
 * so a stray non-landed `running` Job under a truly-`done` Workstream is abandoned, never
 * resurrected. Exhaustive over {@link WorkStatus} — a new status is a compile error here,
 * never a silent fall-through back to `resume`.
 */
const decideRunning = (issue: Issue, workstreamStatus: WorkStatus): ResumeAction => {
  if (isIssueLanded(issue)) return { _tag: "settle", status: "succeeded" };
  switch (workstreamStatus) {
    case "active":
    case "pending":
      return { _tag: "resume" };
    case "blocked":
      return { _tag: "settle", status: "queued" };
    case "cancelled":
    case "done":
      return { _tag: "settle", status: "cancelled" };
  }
};

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
 * The {@link StartupReconcile} implementation over the `StateStore` / `CodeHost` /
 * `JobRunner` ports (`Layer.effect` + `Service.of`, per conventions). The three
 * ports are captured at construction and re-provided into the reused
 * `reconcileWorkstream` and each `dispatch`; a consumer supplies concrete adapters
 * for them (INV-PORT).
 */
export const layer: Layer.Layer<StartupReconcile, never, StateStore | CodeHost | JobRunner> =
  Layer.effect(
    StartupReconcile,
    Effect.gen(function* () {
      const store = yield* StateStore;
      const host = yield* CodeHost;
      const jobRunner = yield* JobRunner;
      // The daemon's boot scope: background resume fibers are tied to it, so they
      // live for the daemon's lifetime and are interrupted when it stops.
      const scope = yield* Effect.scope;

      /** Re-provide the captured store/host ports into a reconcile effect. */
      const reconcile = (workstreamId: Parameters<typeof reconcileWorkstream>[0]) =>
        reconcileWorkstream(workstreamId).pipe(
          Effect.provideService(StateStore, store),
          Effect.provideService(CodeHost, host),
        );

      /**
       * Re-dispatch one Job onto its persisted execution as a BACKGROUND fiber tied to
       * the daemon scope. `JobRunner.dispatch` awaits the execution's terminal outcome
       * (a run can take minutes), so — exactly as the live command path
       * (`dispatchInBackground`) does — boot must fork it rather than block: N
       * in-flight executions resume concurrently and `run` returns promptly. The
       * dispatch owns a fresh per-Job scope (the execution lifetime); any failure is
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

      /**
       * Settle a stale `running` Job to a terminal/pending status AND SEAL EVERY LIVE
       * EXECUTION it owns (CE4.1-R4 root fix) — writing ONLY the Job row would leave LIVE
       * Execution orphans that stall the execution-resolve gate. The pre-restart run is
       * over whatever the Job's fate is, and a sealed transcript is exactly that
       * statement: the durable entries it produced are complete at the extent the log
       * reached, and nothing will tail it.
       *
       * It iterates `listExecutionsForJob`, NOT `getExecutionForJob`. A job owns a TREE of
       * executions (DE2.2), and the root is only one node of it: sealing the root alone
       * left every SIBLING and CHILD holding a `LiveTranscript` forever, so
       * `isExecutionLive` stayed true on them and `resolveLive` (`./rpc-handlers.ts`)
       * bounded-WAITED on each whenever the job was still `queued`/`running` — the exact
       * stall this settle exists to prevent, merely moved off the root. This is the ONE
       * path whose correctness depends on seeing every execution, because it is the one
       * making a statement about all of them.
       *
       * Each execution is sealed at ITS OWN `executionLog.maxOffset`: the log is scoped
       * per execution, so a shared extent would understate one sibling and overstate
       * another. Already-sealed rows are left alone — re-sealing would rewrite a settled
       * extent for no gain, and a seal is idempotent only if it is not recomputed.
       *
       * A job with NO executions writes nothing, and that is now a real statement rather
       * than a gap: the composite `parent` key means a job's executions are closed under
       * `parent`, so an empty list means empty, not "the root was lost to a cross-job
       * edge" (see `putExecution`, `@sprinter/state`).
       *
       * The seal reuses each persisted execution id, so a later re-dispatch of a `queued`
       * Job re-attaches the SAME execution and re-opens its transcript — sound because
       * offsets only grow, so a cached prefix stays correct (see `Transcript`,
       * `@sprinter/domain`).
       *
       * The extent comes from the durable log's own indexed `maxOffset`; a transient read
       * failure seals at `0` rather than leaving the execution LIVE forever, which is the
       * failure this settle exists to prevent. That fallback is why the contract states
       * `lastOffset` as a LOWER BOUND rather than an exact extent (see `Transcript`,
       * `@sprinter/domain`, and the same fallback in `job-runner.ts`'s `sealTranscript`):
       * `0` here does NOT assert an empty transcript, it asserts only that `[0, 0]` is
       * settled — which is trivially true and never invalidates a cached prefix. There is
       * no local high-water mark to prefer on THIS path: the run whose appends it would
       * have counted belongs to a process that is already gone, which is why this settle
       * runs at all. That argument is specific to this path and does NOT carry over to
       * `job-runner`'s seal, whose fold IS the writer and therefore holds the offsets
       * `append` returned; it seals at `max(localHighWater, maxOffset ?? 0)` and is exact
       * in the common case. The LOWER-BOUND contract is written for the path here, where
       * nothing better exists.
       *
       * **TERMINAL WRITE ORDER — the seals FIRST, the Job LAST, and it is RECOVERY-ORDERED**
       * (round 5, B1). Both orders are legal to the engine here — the Job row already
       * exists, so `putJob` is a pure UPDATE with no foreign key to satisfy — so the order
       * is FREE and is chosen by asking what an interruption BETWEEN the writes leaves
       * behind. This is the identical question, and the identical answer, as the terminal
       * pair in `job-runner.ts`'s `dispatch` (see the full statement there):
       *
       * - Job LAST (what this does): an interrupted seal leaves a `running` Job with LIVE
       *   executions — precisely the state THIS function exists to settle, so the NEXT boot
       *   repairs it. `run` re-reads the graph from scratch every time, so a resumed settle
       *   is just a settle.
       * - Job FIRST (what this used to do): the same interruption leaves a TERMINAL Job with
       *   a LIVE execution, and NOTHING repairs it — `run` skips every job whose
       *   `status !== "running"`, and this settle is the only other `SealedTranscript`
       *   writer in the tree and sits behind that gate. A permanent live orphan that
       *   survives every restart. Unrepresentable-22 in `docs/plan/domain-remodel.md`.
       *
       * The seal loop is deliberately ALL-OR-NOTHING across siblings, and that is only safe
       * BECAUSE the Job write is last (round 5, B2). A `StateStoreError` on execution *k*
       * aborts *k+1…n* and fails the boot — but the Job is still `running`, so the next boot
       * re-lists the tree and re-seals from the top; the already-sealed rows are skipped and
       * the remainder is reached. Isolating each execution with a log instead would let the
       * `putJob` run over a PARTIALLY sealed tree, which is the live orphan again, one node
       * down. A failure here is therefore raised, not swallowed: our own store failing at
       * boot is a real failure the caller must see, and the durable state it leaves is
       * repairable.
       */
      const settle = (job: Job, status: SettleStatus): Effect.Effect<void, StateStoreError> =>
        Effect.gen(function* () {
          const executions = yield* store.jobs.listExecutionsForJob(job.id);
          yield* Effect.forEach(executions, (execution) =>
            Effect.gen(function* () {
              if (!isExecutionLive(execution)) return;
              const lastOffset = yield* store.executionLog
                .maxOffset(execution.id)
                .pipe(Effect.orElseSucceed(() => 0));
              yield* store.jobs.putExecution({
                ...execution,
                transcript: { _tag: "SealedTranscript", lastOffset },
              });
            }),
          );
          // ORDER: FREE (no FK — the Job row exists, this is a pure UPDATE), chosen
          // RECOVERY-ORDERED. The Job goes terminal LAST so any interruption above leaves
          // the `running` Job this very function is the repair for.
          yield* store.jobs.putJob({ ...job, status });
        });

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
                  // Settle the stale `running` Job AND its Execution row to terminal, so no
                  // durable `running` limbo — and no orphaned NON-TERMINAL Execution — survives
                  // across restarts (CE4.1-R4).
                  yield* settle(job, action.status);
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
