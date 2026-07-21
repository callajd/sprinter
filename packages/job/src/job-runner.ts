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
 * The Issue→Job→execution mapping is durable, and one dispatch drives ONE execution:
 * the execution id is derived deterministically from the job (or reused from the job's
 * existing `executionId`), so a re-dispatch/restart re-attaches by upserting the SAME
 * execution id, never a new one. (A job may now own a TREE of executions — DE2.2 —
 * so that is a property of THIS dispatch, no longer a `UNIQUE` index on the store.)
 *
 * It is also the `Agent` registry's FIRST PRODUCTION WRITER (DE2.2 / D2): every
 * dispatch registers the agent revision it runs and attributes the execution to it, so
 * `Snapshot.agents` in a real daemon reflects the agents that actually ran.
 */
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type Execution,
  type ExecutionEvent,
  ExecutionId,
  type ExecutionInput,
  type Issue,
  type Job,
  type JobResult,
  type Transcript,
} from "@sprinter/domain";
import type { PiRpcError, PiTransportError, ExecutionResult } from "@sprinter/runner";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { registerAgent } from "./agent-registration.ts";
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
 *
 * DE2.1 changed the derived VALUE, not just the names: the prefix was `session-`, and
 * {@link transcriptRefFor} therefore emits `transcript://execution-…` where it once
 * emitted `transcript://session-…`. That is safe only because `INV-FRESH` holds — the
 * `SCHEMA_VERSION` bump drops and recreates the store, so no `session-`-prefixed id
 * survives a restart to mismatch a freshly derived one. There is no migration and none
 * is needed; a future prefix change carries the same obligation.
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
 * SEAL an execution's transcript at the extent its durable log has reached — the write
 * that ends a run (a settled execution's transcript is `[0, lastOffset]`: complete,
 * immutable, cacheable).
 *
 * The extent is read from the durable log itself (`executionLog.maxOffset`, answered
 * from the backing's index — no transcript is materialised), so the seal describes what
 * was actually persisted rather than what this dispatch believes it appended. A
 * transient read failure falls back to `0` rather than stopping the seal: an execution
 * that stayed LIVE because the extent could not be read would look forever-running to
 * every liveness gate, which is strictly worse than an understated extent (`0` is the
 * empty-transcript sentinel, below every durable offset).
 */
const sealTranscript = (
  store: Context.Service.Shape<typeof StateStore>,
  executionId: ExecutionId,
): Effect.Effect<Transcript> =>
  store.executionLog.maxOffset(executionId).pipe(
    Effect.orElseSucceed(() => 0),
    Effect.map((lastOffset): Transcript => ({ _tag: "SealedTranscript", lastOffset })),
  );

/**
 * Best-effort persist of a `failed` terminal Job and a SEALED execution when starting
 * or driving the execution fails — so a `run`/`send` failure never leaves the durable
 * rows stuck in the `running`/live limbo the initial persist wrote. The terminal write
 * is best-effort (its own {@link StateStoreError} is swallowed) so the ORIGINAL dispatch
 * error still propagates to the caller.
 */
const persistFailedTerminal = (
  store: Context.Service.Shape<typeof StateStore>,
  job: Job,
  execution: Execution,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const transcript = yield* sealTranscript(store, execution.id);
    yield* store.jobs.putJob({ ...job, status: "failed", executionId: execution.id });
    yield* store.jobs.putExecution({ ...execution, transcript });
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

    // REGISTER the agent revision this run is attributed to, FIRST — the registry's
    // first production writer (DE2.2 / D2), and a precondition rather than bookkeeping:
    // `execution.agentId` is a FOREIGN KEY onto `agent`, so an execution cannot be
    // stored until its agent revision is. Idempotent by construction (the id is derived
    // from the content), so a re-dispatch appends nothing and fans out no delta.
    const agentId = yield* registerAgent(store, runner.agent);

    // Persist the durable Issue→Job→execution mapping BEFORE running. ORDER IS LOAD-
    // BEARING: `execution."jobId"` is a FOREIGN KEY, so the Job row must exist before
    // the execution that names it (D1 — the link is `jobId` only until DE2.4 re-points
    // it at `sessionId`). The same execution id on re-dispatch upserts in place.
    //
    // The transcript starts LIVE: an open range with no last entry, which is also the
    // whole of this execution's liveness (there is no status column to agree with —
    // `Transcript`, `@sprinter/domain`). A re-dispatch re-opens the previous run's
    // SEALED transcript, which is sound because offsets only ever grow: whatever a
    // reader cached of `[0, lastOffset]` stays exactly correct (issue #77's merged
    // single-transcript decision).
    //
    // `mode` is `autonomous`: the agent holds the turn for a dispatched Job and yields
    // only when blocked. It is recorded on the EXECUTION and nowhere above it
    // (INV-MODE). `parent` is absent — a dispatched Job's execution is a ROOT; a
    // subagent's execution names it as parent, which is what makes the relation a tree.
    const startExecution: Execution = {
      id: executionId,
      jobId: job.id,
      agentId,
      mode: "autonomous",
      transcript: { _tag: "LiveTranscript" },
    };
    yield* store.jobs.putJob({ ...job, status: "running", executionId });
    yield* store.jobs.putExecution(startExecution);

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
          // 1 Execution = 1 Transcript, so this fold is the sole writer of this execution's
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
    }).pipe(Effect.tapError(() => persistFailedTerminal(store, job, startExecution)));

    // The `entries` metric = the count of DURABLE `EntryAppended` records in the execution's
    // transcript, computed cheaply in the store (`countEntries` — a `COUNT(*)`, no full-log
    // read/decode). DECISION (issue #77): the MERGED single-transcript model is intended by
    // construction — 1 Execution = 1 durable transcript, and a re-dispatch/retry APPENDS
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

    // Persist the terminal Job row and SEAL the execution's transcript. The seal is the
    // whole of "this run has ended": the transcript stops being tailable and becomes the
    // closed, cacheable range `[0, lastOffset]`, and every liveness gate reads that one
    // value. The run's OUTCOME (succeeded / failed) belongs to the Job it advanced —
    // recording it a second time on the execution would be a lifecycle its transcript
    // already determines (INV-LIFECYCLE).
    yield* store.jobs.putJob({ ...job, status: jobResult.status, executionId, transcriptRef });
    yield* store.jobs.putExecution({
      ...startExecution,
      transcript: yield* sealTranscript(store, executionId),
    });

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
