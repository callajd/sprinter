/**
 * `JobRunner` — the single-Issue Job runner (Track A, task AE3.1). It joins
 * execution (AE1) to durability (AE2): dispatch a {@link Job} to a session via the
 * {@link ExecutionRunner} port, drive the job's prompt, consume the session's
 * events, await the terminal outcome, map it to the owned {@link JobResult}
 * envelope (D6), and persist the durable Job/session rows through the
 * {@link StateStore} port.
 *
 * It depends ONLY on the two ports ({@link ExecutionRunner} + {@link StateStore})
 * and the runner's neutral {@link SessionHandle} surface — never on a concrete
 * `pi` process or SQLite instance (INV-PORT). It is deterministic daemon control
 * flow (D2): the agent only does cognition; assigning session ids, mapping
 * outcomes, and persisting rows is all owned here.
 *
 * The Issue→Job→session mapping is durable and 1 Job = 1 session: the session id
 * is derived deterministically from the job (or reused from the job's existing
 * `sessionId`), so a re-dispatch/restart re-attaches by upserting the SAME session
 * id, never a new one (the store's `UNIQUE(session.jobId)` enforces the invariant).
 */
import { Context, Effect, Layer, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type Job,
  type JobResult,
  type Session,
  SessionId,
  type SessionInput,
  type SessionStatus,
} from "@sprinter/domain";
import type { PiRpcError, PiTransportError, SessionResult } from "@sprinter/runner";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { ExecutionRunner, type ExecutionRunnerError } from "./execution-runner.ts";

/** Everything that can fail a dispatch: the two ports' failures plus driving the prompt. */
type DispatchError = ExecutionRunnerError | StateStoreError | PiRpcError | PiTransportError;

/**
 * The deterministic session id for a job: the job's existing `sessionId` when it
 * already has one (a re-dispatch/restart re-attaches to the SAME session), else a
 * stable id derived from the job id. Derivation is decoded through the owned
 * {@link SessionId} schema (never a cast); the input is non-empty by construction,
 * so a decode failure is a broken invariant (`orDie`).
 */
const sessionIdFor = (job: Job): Effect.Effect<SessionId> =>
  job.sessionId !== undefined
    ? Effect.succeed(job.sessionId)
    : Schema.decodeUnknownEffect(SessionId)(`session-${job.id}`).pipe(Effect.orDie);

/**
 * The durable transcript reference for a session — a stable, neutral locator
 * decoded through the owned schema (never a cast). AE3.1 records the reference;
 * the transcript feed itself is the {@link StateStore} event log.
 */
const transcriptRefFor = (sessionId: SessionId): Effect.Effect<string> =>
  Schema.decodeUnknownEffect(Schema.NonEmptyString)(`transcript://${sessionId}`).pipe(Effect.orDie);

/** The prompt driven into a fresh session — derived from the owned job (no Pi concept). */
const promptForJob = (job: Job): SessionInput => ({
  text: `Work the ${job.kind} job for issue ${job.issueId}.`,
  mode: "prompt",
});

/**
 * Map a settled session's {@link SessionResult} onto the terminal {@link Session}
 * status — TOTAL via discriminated matching (INV-NOCAST): a clean end completes
 * the session, a transport teardown fails it.
 */
const toSessionStatus = (result: SessionResult): SessionStatus =>
  result._tag === "Completed" ? "completed" : "failed";

/**
 * Map a settled session's {@link SessionResult} onto the terminal {@link JobResult}
 * envelope (D6) — TOTAL via discriminated matching (INV-NOCAST). `Completed` →
 * `succeeded`; `Failed` → `failed` carrying the neutral error detail. The open
 * `payload` records what the run observed (the transcript ref + entry count).
 */
const toJobResult = (result: SessionResult, payload: unknown): JobResult => {
  switch (result._tag) {
    case "Completed":
      return { status: "succeeded", payload };
    case "Failed":
      return { status: "failed", error: result.error, payload };
  }
};

/**
 * The one bounded cognitive dispatch: persist the initial Job/session rows, start
 * the session, drive the prompt, consume events, await the terminal outcome, and
 * persist the terminal rows — returning the {@link JobResult}.
 *
 * Events are consumed by folding the live stream (counting durable transcript
 * entries) so the runner is a real consumer of the session's reactive output; a
 * transport teardown of the stream is tolerated (the terminal authority is
 * {@link SessionHandle.result}, which reports the failure).
 */
const dispatch = (
  runner: Context.Service.Shape<typeof ExecutionRunner>,
  store: Context.Service.Shape<typeof StateStore>,
  job: Job,
): Effect.Effect<JobResult, DispatchError, Scope> =>
  Effect.gen(function* () {
    const sessionId = yield* sessionIdFor(job);

    // Persist the durable Issue→Job→session mapping BEFORE running: the same
    // session id on re-dispatch upserts in place (1 Job = 1 session).
    const startSession: Session = { id: sessionId, jobId: job.id, status: "starting" };
    yield* store.jobs.putSession(startSession);
    yield* store.jobs.putJob({ ...job, status: "running", sessionId });

    const handle = yield* runner.run(job);
    yield* handle.send(promptForJob(job));

    // Consume the session's events (fold durable entries); a transport teardown of
    // the stream is not the terminal authority — `handle.result` is.
    const entries = yield* handle.events.pipe(
      Stream.runFold(
        () => 0,
        (count, event) => (event._tag === "EntryAppended" ? count + 1 : count),
      ),
      Effect.orElseSucceed(() => 0),
    );

    const result = yield* handle.result;
    const transcriptRef = yield* transcriptRefFor(sessionId);
    const jobResult = toJobResult(result, { transcriptRef, entries });

    // Persist the terminal Job/session rows.
    yield* store.jobs.putSession({ id: sessionId, jobId: job.id, status: toSessionStatus(result) });
    yield* store.jobs.putJob({ ...job, status: jobResult.status, sessionId, transcriptRef });

    return jobResult;
  });

/**
 * The Job runner PORT (INV-NAMING): dispatch one {@link Job} to completion,
 * yielding its terminal {@link JobResult}. `dispatch` is `Scope`-managed — the
 * caller provides the per-dispatch scope that owns the underlying session's
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
