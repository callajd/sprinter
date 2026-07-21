/**
 * `JobRunner` — the single-Issue Job runner (Track A, task AE3.1). It joins
 * execution (AE1) to durability (AE2): dispatch a {@link Job} to an execution via the
 * {@link ExecutionRunner} port, drive the job's prompt, consume the execution's
 * events, await the terminal outcome, map it to the owned {@link JobResult}
 * envelope (D6), and persist the durable Job/execution rows through the
 * {@link StateStore} port.
 *
 * It depends ONLY on the two ports ({@link ExecutionRunner} + {@link StateStore})
 * and the runner's neutral {@link ExecutionHandle} surface — never on a concrete
 * `pi` process or SQLite instance (INV-PORT). It is deterministic daemon control
 * flow (D2): the agent only does cognition; assigning execution ids, mapping
 * outcomes, and persisting rows is all owned here.
 *
 * The Issue→Job→execution mapping is durable and 1 Job = 1 execution: the execution id
 * is derived deterministically from the job (or reused from the job's existing
 * `executionId`), so a re-dispatch/restart re-attaches by upserting the SAME execution
 * id, never a new one (the store's `UNIQUE(execution.jobId)` enforces the invariant).
 */
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type Execution,
  type ExecutionEvent,
  ExecutionId,
  type ExecutionInput,
  type ExecutionStatus,
  type Issue,
  type Job,
  type JobResult,
} from "@sprinter/domain";
import type { PiRpcError, PiTransportError, ExecutionResult } from "@sprinter/runner";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { ExecutionRunner, type ExecutionRunnerError } from "./execution-runner.ts";

/** Everything that can fail a dispatch: the two ports' failures plus driving the prompt. */
type DispatchError = ExecutionRunnerError | StateStoreError | PiRpcError | PiTransportError;

/**
 * The deterministic execution id for a job: the job's existing `executionId` when it
 * already has one (a re-dispatch/restart re-attaches to the SAME execution), else a
 * stable id derived from the job id. Derivation is decoded through the owned
 * {@link ExecutionId} schema (never a cast); the input is non-empty by construction,
 * so a decode failure is a broken invariant (`orDie`).
 *
 * Exported so a consumer that must key a live execution by the SAME id `dispatch`
 * persists — notably the daemon's execution-registry wiring (CE4.1), which registers
 * the live `ExecutionHandle` under exactly this id so the execution channel resolves it
 * — derives it identically rather than re-deriving (and risking drift from) it.
 */
export const executionIdFor = (job: Job): Effect.Effect<ExecutionId> =>
  job.executionId !== undefined
    ? Effect.succeed(job.executionId)
    : Schema.decodeUnknownEffect(ExecutionId)(`execution-${job.id}`).pipe(Effect.orDie);

/**
 * The durable transcript reference for an execution — a stable, neutral locator
 * decoded through the owned schema (never a cast). AE3.1 records the reference;
 * the transcript feed itself is the {@link StateStore} event log.
 */
const transcriptRefFor = (executionId: ExecutionId): Effect.Effect<string> =>
  Schema.decodeUnknownEffect(Schema.NonEmptyString)(`transcript://${executionId}`).pipe(
    Effect.orDie,
  );

/**
 * The prompt driven into a fresh execution — built from the REAL Issue content the
 * job advances (its number + title), decoded from the durable {@link StateStore},
 * never the id-only placeholder. It is expressed in owned, neutral terms only (no
 * Pi concept): the concrete `pi` prompt encoding happens below the
 * {@link ExecutionRunner} port. When the Issue row is absent (a degraded path that
 * should not occur once planning has persisted the graph), it falls back to the
 * job's own fields so a dispatch is still driveable rather than blocked.
 */
const promptForJob = (job: Job, issue: Option.Option<Issue>): ExecutionInput => ({
  text: Option.match(issue, {
    onNone: () => `Work the ${job.kind} job for issue ${job.issueId}.`,
    onSome: (found) => `Work the ${job.kind} job for issue #${found.number}: ${found.title}`,
  }),
  mode: "prompt",
});

/**
 * True for a DURABLE, transcript-grade {@link ExecutionEvent} — the ones the app folds into
 * the transcript and that must persist to the execution's durable transcript log so a SETTLED
 * execution stays viewable: the `EntryAppended` durable records and the
 * reconcilable `Notice`s. The complement — ephemeral streaming deltas (message/tool partials,
 * turn lifecycle, `UiRequestRaised`, status/retry/compaction) — is NOT persisted, but is still
 * TEED to the reactive feed offset-less so a live driving execution receives its whole flow (see
 * the fold below); it carries no durable offset and is absorbed into the durable entries by the
 * consumer's projection. A POSITIVE allow-list, so any future variant defaults to ephemeral
 * (never accidentally bloating the durable transcript).
 */
const isDurableTranscriptEvent = (event: ExecutionEvent): boolean =>
  event._tag === "EntryAppended" || event._tag === "Notice";

/**
 * Map a settled execution's {@link ExecutionResult} onto the terminal {@link Execution}
 * status — TOTAL via discriminated matching (INV-NOCAST): a clean end completes
 * the execution, a transport teardown fails it.
 */
const toExecutionStatus = (result: ExecutionResult): ExecutionStatus =>
  result._tag === "Completed" ? "completed" : "failed";

/**
 * Map a settled execution's {@link ExecutionResult} onto the terminal {@link JobResult}
 * envelope (D6) — TOTAL via discriminated matching (INV-NOCAST). `Completed` →
 * `succeeded`; `Failed` → `failed` carrying the neutral error detail. The open
 * `payload` records what the run observed (the transcript ref + entry count).
 */
const toJobResult = (result: ExecutionResult, payload: unknown): JobResult => {
  switch (result._tag) {
    case "Completed":
      return { status: "succeeded", payload };
    case "Failed":
      return { status: "failed", error: result.error, payload };
  }
};

/**
 * Best-effort persist of a `failed` terminal Job/execution when starting or driving
 * the execution fails — so a `run`/`send` failure never leaves the durable rows stuck
 * in the `running`/`starting` limbo the initial persist wrote. The terminal write is
 * best-effort (its own {@link StateStoreError} is swallowed) so the ORIGINAL dispatch
 * error still propagates to the caller.
 */
const persistFailedTerminal = (
  store: Context.Service.Shape<typeof StateStore>,
  job: Job,
  executionId: ExecutionId,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* store.jobs.putExecution({ id: executionId, jobId: job.id, status: "failed" });
    yield* store.jobs.putJob({ ...job, status: "failed", executionId });
  }).pipe(Effect.orElseSucceed(() => undefined));

/**
 * The one bounded cognitive dispatch: persist the initial Job/execution rows, start
 * the execution, drive the prompt, consume events, await the terminal outcome, and
 * persist the terminal rows — returning the {@link JobResult}.
 *
 * Events are consumed by folding the live stream (appending durable transcript
 * entries) so the runner is a real consumer of the execution's reactive output; a
 * transport teardown of the stream is tolerated (the terminal authority is
 * {@link ExecutionHandle.result}, which reports the failure). If instead STARTING or
 * DRIVING the execution fails (`run`/`send`), the durable rows are moved to a `failed`
 * terminal before the error propagates — the job never stays durably `running`.
 */
const dispatch = (
  runner: Context.Service.Shape<typeof ExecutionRunner>,
  store: Context.Service.Shape<typeof StateStore>,
  job: Job,
): Effect.Effect<JobResult, DispatchError, Scope> =>
  Effect.gen(function* () {
    const executionId = yield* executionIdFor(job);

    // Persist the durable Issue→Job→execution mapping BEFORE running: the same
    // execution id on re-dispatch upserts in place (1 Job = 1 execution).
    const startExecution: Execution = { id: executionId, jobId: job.id, status: "starting" };
    yield* store.jobs.putExecution(startExecution);
    yield* store.jobs.putJob({ ...job, status: "running", executionId });

    // Start + drive the execution and await its terminal outcome. On failure of
    // starting/driving (not a mere stream teardown), record a `failed` terminal so
    // the durable state is never left in limbo, then re-raise (F2).
    const settled = yield* Effect.gen(function* () {
      const handle = yield* runner.run(job);
      // Drive the REAL Issue-content prompt (title/number), read from the durable
      // graph — never the id-only placeholder. The concrete `pi` encoding is below
      // the ExecutionRunner port; this stays in owned, neutral terms (INV-PORT).
      const issue = yield* store.workGraph.getIssue(job.issueId);
      yield* handle.send(promptForJob(job, issue));

      // Fold the live events into the durable transcript log — the fold APPENDS each
      // durable entry to the execution's log (the sole writer of this execution's
      // transcript). The `entries` count is not tallied here: it is read back from that
      // durable log at terminal (`executionLog.countEntries`), so it survives a stream
      // teardown AND reflects the merged transcript across every run (issue #77). The
      // stream is not the terminal authority (`handle.result` is), so a typed teardown
      // of the fold is tolerated.
      //
      // BOUND the fold by the terminal `result` (F1 terminal-result contract), so it
      // can NEVER outlive the execution. `handle.events` is a fresh, lazily-subscribed
      // view over a bounded/sliding PubSub: if the truncating event slid out of THIS
      // subscription's replay window before its first pull (a real-`pi` burst / a
      // still-live execution), the stream would never end and the fold would HANG —
      // even though `handle.result` has already settled (its watcher subscribes
      // eagerly, so the terminal resolves regardless of this subscription's window).
      // `Stream.interruptWhen(handle.result)` interrupts the fold the moment the
      // execution reaches its terminal — deterministic and window-independent, not
      // reliant on the sliding-window timing — so dispatch always proceeds to read the
      // already-settled result instead of blocking on an idle-but-alive events stream.
      yield* handle.events.pipe(
        Stream.interruptWhen(handle.result),
        Stream.runForEach((event) =>
          // TEE the WHOLE reactive flow to the `ExecutionEvents` feed (ONE channel
          // serving BOTH the live driving modality AND settled-transcript replay). Every event
          // the execution emits must reach a live `executionEvents` subscriber:
          //   - a DURABLE transcript entry is APPENDED to the execution's durable transcript log,
          //     which the store's journaling decorator persists, mints a per-execution offset for,
          //     AND fans out on the feed offset-STAMPED — so a live subscriber tails it and a
          //     SETTLED execution replays it later;
          //   - an EPHEMERAL live delta is fanned out OFFSET-LESS via `publishEphemeral` — it
          //     reaches live subscribers to drive the reactive flow but is never persisted and
          //     never advances the reconnect cursor.
          // 1 Job = 1 execution = 1 transcript, so this fold is the sole writer of this execution's
          // transcript; a re-dispatch APPENDS (never resets the offset sequence).
          Effect.gen(function* () {
            if (isDurableTranscriptEvent(event)) {
              // Tolerate a transient `append` failure PER EVENT — drop THIS one entry and keep
              // folding — rather than letting one hiccup tear down the whole writer and silently
              // truncate every SUBSEQUENT durable entry. The `entries` count is derived at
              // terminal from the durable log itself (`executionLog.countEntries`), so a dropped
              // append is reflected there rather than tracked by a separate in-memory counter.
              yield* store.executionLog
                .append(executionId, event)
                .pipe(Effect.catchTag("StateStoreError", () => Effect.void));
            } else {
              yield* store.executionLog.publishEphemeral(executionId, event);
            }
          }),
        ),
        // The stream is not the terminal authority (`handle.result` is), so a typed teardown of
        // the fold is tolerated: whatever was appended to the durable log before the teardown is
        // preserved and counted at terminal (transient per-append failures are already absorbed
        // above, so one hiccup no longer truncates the rest of the fold).
        Effect.orElseSucceed(() => undefined),
      );
      return yield* handle.result;
    }).pipe(Effect.tapError(() => persistFailedTerminal(store, job, executionId)));

    // The `entries` metric = the count of DURABLE `EntryAppended` records in the execution's
    // transcript, computed cheaply in the store (`countEntries` — a `COUNT(*)`, no full-log
    // read/decode). DECISION (issue #77): the MERGED single-transcript model is intended by
    // construction — 1 Job = 1 execution = 1 durable transcript, and a re-dispatch/retry APPENDS
    // to that SAME log (offsets monotonic, never reset). So the count reflects the concatenation
    // of EVERY run — matching the transcript the Inspector renders — rather than diverging to the
    // latest run (the per-dispatch `Ref` divergence). Retry runs are deliberately NOT segmented;
    // run-segmentation is left to a separate product decision.
    //
    // Best-effort AND non-stranding: a transient count failure falls back to `0` rather than
    // stopping the terminal `putExecution`/`putJob` below — the metric is an unconsumed byproduct,
    // so a rare count hiccup must never leave a genuinely-settled job durably `running`.
    const entries = yield* store.executionLog
      .countEntries(executionId)
      .pipe(Effect.orElseSucceed(() => 0));
    const transcriptRef = yield* transcriptRefFor(executionId);
    const jobResult = toJobResult(settled, { transcriptRef, entries });

    // Persist the terminal Job/execution rows.
    yield* store.jobs.putExecution({
      id: executionId,
      jobId: job.id,
      status: toExecutionStatus(settled),
    });
    yield* store.jobs.putJob({ ...job, status: jobResult.status, executionId, transcriptRef });

    return jobResult;
  });

/**
 * The Job runner PORT (INV-NAMING): dispatch one {@link Job} to completion,
 * yielding its terminal {@link JobResult}. `dispatch` is `Scope`-managed — the
 * caller provides the per-dispatch scope that owns the underlying execution's
 * lifetime.
 */
export class JobRunner extends Context.Service<
  JobRunner,
  {
    readonly dispatch: (job: Job) => Effect.Effect<JobResult, DispatchError, Scope>;
  }
>()("sprinter/job/JobRunner") {}

/**
 * The {@link JobRunner} implementation over the {@link ExecutionRunner} and
 * {@link StateStore} ports (`Layer.effect` + `Service.of`, per conventions). A
 * consumer chooses the concrete runtime and backing by providing adapter layers
 * for those ports (INV-PORT).
 */
export const layer: Layer.Layer<JobRunner, never, ExecutionRunner | StateStore> = Layer.effect(
  JobRunner,
  Effect.gen(function* () {
    const runner = yield* ExecutionRunner;
    const store = yield* StateStore;
    return JobRunner.of({ dispatch: (job) => dispatch(runner, store, job) });
  }),
);
