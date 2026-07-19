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
import { Context, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import {
  type Issue,
  type Job,
  type JobResult,
  type Session,
  type SessionEvent,
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
 *
 * Exported so a consumer that must key a live session by the SAME id `dispatch`
 * persists — notably the daemon's session-registry wiring (CE4.1), which registers
 * the live `SessionHandle` under exactly this id so the session channel resolves it
 * — derives it identically rather than re-deriving (and risking drift from) it.
 */
export const sessionIdFor = (job: Job): Effect.Effect<SessionId> =>
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

/**
 * The prompt driven into a fresh session — built from the REAL Issue content the
 * job advances (its number + title), decoded from the durable {@link StateStore},
 * never the id-only placeholder. It is expressed in owned, neutral terms only (no
 * Pi concept): the concrete `pi` prompt encoding happens below the
 * {@link ExecutionRunner} port. When the Issue row is absent (a degraded path that
 * should not occur once planning has persisted the graph), it falls back to the
 * job's own fields so a dispatch is still driveable rather than blocked.
 */
const promptForJob = (job: Job, issue: Option.Option<Issue>): SessionInput => ({
  text: Option.match(issue, {
    onNone: () => `Work the ${job.kind} job for issue ${job.issueId}.`,
    onSome: (found) => `Work the ${job.kind} job for issue #${found.number}: ${found.title}`,
  }),
  mode: "prompt",
});

/**
 * True for a DURABLE, transcript-grade {@link SessionEvent} — the ones the app folds into
 * the transcript and that must persist to the session's durable transcript log so a SETTLED
 * session stays viewable: the `EntryAppended` durable records and the
 * reconcilable `Notice`s. The complement — ephemeral streaming deltas (message/tool partials,
 * turn lifecycle, `UiRequestRaised`, status/retry/compaction) — is NOT persisted, but is still
 * TEED to the reactive feed offset-less so a live driving session receives its whole flow (see
 * the fold below); it carries no durable offset and is absorbed into the durable entries by the
 * consumer's projection. A POSITIVE allow-list, so any future variant defaults to ephemeral
 * (never accidentally bloating the durable transcript).
 */
const isDurableTranscriptEvent = (event: SessionEvent): boolean =>
  event._tag === "EntryAppended" || event._tag === "Notice";

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
 * Best-effort persist of a `failed` terminal Job/session when starting or driving
 * the session fails — so a `run`/`send` failure never leaves the durable rows stuck
 * in the `running`/`starting` limbo the initial persist wrote. The terminal write is
 * best-effort (its own {@link StateStoreError} is swallowed) so the ORIGINAL dispatch
 * error still propagates to the caller.
 */
const persistFailedTerminal = (
  store: Context.Service.Shape<typeof StateStore>,
  job: Job,
  sessionId: SessionId,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* store.jobs.putSession({ id: sessionId, jobId: job.id, status: "failed" });
    yield* store.jobs.putJob({ ...job, status: "failed", sessionId });
  }).pipe(Effect.orElseSucceed(() => undefined));

/**
 * The one bounded cognitive dispatch: persist the initial Job/session rows, start
 * the session, drive the prompt, consume events, await the terminal outcome, and
 * persist the terminal rows — returning the {@link JobResult}.
 *
 * Events are consumed by folding the live stream (counting durable transcript
 * entries) so the runner is a real consumer of the session's reactive output; a
 * transport teardown of the stream is tolerated (the terminal authority is
 * {@link SessionHandle.result}, which reports the failure). If instead STARTING or
 * DRIVING the session fails (`run`/`send`), the durable rows are moved to a `failed`
 * terminal before the error propagates — the job never stays durably `running`.
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

    // Start + drive the session and await its terminal outcome. On failure of
    // starting/driving (not a mere stream teardown), record a `failed` terminal so
    // the durable state is never left in limbo, then re-raise (F2).
    const settled = yield* Effect.gen(function* () {
      const handle = yield* runner.run(job);
      // Drive the REAL Issue-content prompt (title/number), read from the durable
      // graph — never the id-only placeholder. The concrete `pi` encoding is below
      // the ExecutionRunner port; this stays in owned, neutral terms (INV-PORT).
      const issue = yield* store.workGraph.getIssue(job.issueId);
      yield* handle.send(promptForJob(job, issue));

      // Count durable transcript entries in a `Ref` so the count SURVIVES a stream
      // teardown — `Stream.runForEach` drops its accumulator on failure/interrupt, a
      // `Ref` keeps it. The stream is not the terminal authority (`handle.result` is),
      // so a typed teardown of the fold is tolerated while preserving the count so far.
      //
      // BOUND the fold by the terminal `result` (F1 terminal-result contract), so it
      // can NEVER outlive the session. `handle.events` is a fresh, lazily-subscribed
      // view over a bounded/sliding PubSub: if the truncating event slid out of THIS
      // subscription's replay window before its first pull (a real-`pi` burst / a
      // still-live session), the stream would never end and the fold would HANG —
      // even though `handle.result` has already settled (its watcher subscribes
      // eagerly, so the terminal resolves regardless of this subscription's window).
      // `Stream.interruptWhen(handle.result)` interrupts the fold the moment the
      // session reaches its terminal — deterministic and window-independent, not
      // reliant on the sliding-window timing — so dispatch always proceeds to read the
      // already-settled result instead of blocking on an idle-but-alive events stream.
      const entriesRef = yield* Ref.make(0);
      yield* handle.events.pipe(
        Stream.interruptWhen(handle.result),
        Stream.runForEach((event) =>
          // TEE the WHOLE reactive flow to the `SessionEvents` feed (ONE channel
          // serving BOTH the live driving modality AND settled-transcript replay). Every event
          // the session emits must reach a live `sessionEvents` subscriber:
          //   - a DURABLE transcript entry is APPENDED to the session's durable transcript log,
          //     which the store's journaling decorator persists, mints a per-session offset for,
          //     AND fans out on the feed offset-STAMPED — so a live subscriber tails it and a
          //     SETTLED session replays it later;
          //   - an EPHEMERAL live delta is fanned out OFFSET-LESS via `publishEphemeral` — it
          //     reaches live subscribers to drive the reactive flow but is never persisted and
          //     never advances the reconnect cursor.
          // 1 Job = 1 session = 1 transcript, so this fold is the sole writer of this session's
          // transcript; a re-dispatch APPENDS (never resets the offset sequence).
          Effect.gen(function* () {
            if (isDurableTranscriptEvent(event)) {
              // Tolerate a transient `append` failure PER EVENT — drop THIS one entry and keep
              // folding — rather than letting one hiccup tear down the whole writer and silently
              // truncate every SUBSEQUENT durable entry. The count advances only on a real append.
              yield* store.sessionLog.append(sessionId, event).pipe(
                Effect.flatMap(() =>
                  event._tag === "EntryAppended"
                    ? Ref.update(entriesRef, (n) => n + 1)
                    : Effect.void,
                ),
                Effect.catchTag("StateStoreError", () => Effect.void),
              );
            } else {
              yield* store.sessionLog.publishEphemeral(sessionId, event);
            }
          }),
        ),
        // The stream is not the terminal authority (`handle.result` is), so a typed teardown of
        // the fold is tolerated while preserving the count so far (transient per-append failures
        // are already absorbed above, so one hiccup no longer truncates the rest of the fold).
        Effect.orElseSucceed(() => undefined),
      );
      const entries = yield* Ref.get(entriesRef);
      const result = yield* handle.result;
      return { result, entries };
    }).pipe(Effect.tapError(() => persistFailedTerminal(store, job, sessionId)));

    const transcriptRef = yield* transcriptRefFor(sessionId);
    const jobResult = toJobResult(settled.result, { transcriptRef, entries: settled.entries });

    // Persist the terminal Job/session rows.
    yield* store.jobs.putSession({
      id: sessionId,
      jobId: job.id,
      status: toSessionStatus(settled.result),
    });
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
