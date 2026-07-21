/**
 * `JobRunner` coverage (AE3.1) — the single-Issue Job runner exercised against
 * FAKES: a fake {@link ExecutionRunner} handing back a canned {@link ExecutionHandle}
 * and the in-memory {@link StateStore} adapter (`layerMemory`). Deterministic and
 * offline — no `pi` process, no filesystem (INV-PORT): the runner depends only on
 * the two ports and the runner's neutral handle surface.
 *
 * The suite proves the dispatch → terminal `JobResult` capture → persisted rows
 * flow for BOTH a succeeding and a failing execution, plus the 1 Job = 1 execution
 * re-attach invariant (a re-dispatch upserts the SAME execution id, never a new one).
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import {
  type ExecutionEvent,
  ExecutionId,
  type ExecutionInput,
  Issue,
  Job,
} from "@sprinter/domain";
import { PiTransportError, type ExecutionHandle, type ExecutionResult } from "@sprinter/runner";
import { layerMemory, StateStore, StateStoreError } from "@sprinter/state";
import { ExecutionRunner, ExecutionRunnerError, JobRunner, layer } from "./index.ts";

// ============================================================================
// Fixtures & fakes — decoded through the owned schemas (no casts)
// ============================================================================

const decode = <S extends Schema.Top>(schema: S, raw: S["Encoded"]) =>
  Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.orDie);

const makeJob = (over: Partial<(typeof Job)["Encoded"]> = {}) =>
  decode(Job, { id: "job-1", issueId: "issue-22", kind: "implement", status: "queued", ...over });

const entryEvent: ExecutionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "UserMessage", id: "u1", text: "hi" },
};
const turnStarted: ExecutionEvent = { _tag: "TurnStarted" };
// A durable, transcript-grade `Notice` (offset-bearing) and an ephemeral `MessageDelta`
// (offset-less) — used to prove the fold routes BOTH modalities, dropping nothing.
const noticeEvent: ExecutionEvent = { _tag: "Notice", id: "n1", level: "info", message: "go" };
const messageDelta: ExecutionEvent = { _tag: "MessageDelta", messageId: "u1", text: "h" };

/**
 * A {@link StateStore} decorator over `layerMemory` that RECORDS every `executionLog.append`
 * (durable) and `executionLog.publishEphemeral` (ephemeral) call while delegating to the base —
 * so a test can assert the JobRunner fold routes durable entries to `append` and ephemeral
 * deltas to `publishEphemeral`, teeing the whole flow.
 */
const recordingStore = (
  appended: Ref.Ref<ReadonlyArray<ExecutionEvent>>,
  published: Ref.Ref<ReadonlyArray<ExecutionEvent>>,
): Layer.Layer<StateStore, StateStoreError> =>
  Layer.effect(
    StateStore,
    Effect.gen(function* () {
      const base = yield* StateStore;
      return StateStore.of({
        ...base,
        executionLog: {
          ...base.executionLog,
          append: (executionId, event) =>
            Ref.update(appended, (xs) => [...xs, event]).pipe(
              Effect.andThen(base.executionLog.append(executionId, event)),
            ),
          publishEphemeral: (executionId, event) =>
            Ref.update(published, (xs) => [...xs, event]).pipe(
              Effect.andThen(base.executionLog.publishEphemeral(executionId, event)),
            ),
        },
      });
    }),
  ).pipe(Layer.provide(layerMemory));

/** A fake {@link ExecutionHandle}: a canned event stream and terminal result; the drive/answer verbs are inert. */
const fakeHandle = (
  events: Stream.Stream<ExecutionEvent, PiTransportError>,
  result: ExecutionResult,
): ExecutionHandle => ({
  pid: ChildProcessSpawner.ProcessId(4242),
  events,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed(result),
});

/** As {@link fakeHandle}, but records every {@link ExecutionInput} driven in via `send`. */
const recordingHandle = (
  events: Stream.Stream<ExecutionEvent, PiTransportError>,
  result: ExecutionResult,
  sent: Ref.Ref<ReadonlyArray<ExecutionInput>>,
): ExecutionHandle => ({
  ...fakeHandle(events, result),
  send: (input) => Ref.update(sent, (xs) => [...xs, input]),
});

/** A fake {@link ExecutionRunner} that hands back a fixed handle for every job. */
const fakeRunner = (handle: ExecutionHandle): Layer.Layer<ExecutionRunner> =>
  Layer.succeed(ExecutionRunner, ExecutionRunner.of({ run: () => Effect.succeed(handle) }));

/** Provide the runner-under-test plus its two faked ports; expose `StateStore` for assertions. */
const provide =
  (handle: ExecutionHandle) =>
  <A, E>(effect: Effect.Effect<A, E, JobRunner | StateStore | Scope>) =>
    provideRunner(fakeRunner(handle))(effect);

/** As {@link provide}, but with a caller-supplied {@link ExecutionRunner} layer (e.g. one that fails `run`). */
const provideRunner =
  (runnerLayer: Layer.Layer<ExecutionRunner>) =>
  <A, E>(effect: Effect.Effect<A, E, JobRunner | StateStore | Scope>) =>
    effect.pipe(
      Effect.scoped,
      Effect.provide(layer),
      Effect.provide(runnerLayer),
      Effect.provide(layerMemory),
    );

// ============================================================================
// Port surface
// ============================================================================

it("exposes the owned ExecutionRunnerError with a neutral shape", () => {
  const error = new ExecutionRunnerError({ operation: "run", detail: "spawn refused" });
  expect(error._tag).toBe("ExecutionRunnerError");
  expect(error.operation).toBe("run");
  expect(error.detail).toBe("spawn refused");
});

// ============================================================================
// Succeeding execution
// ============================================================================

it.effect("dispatches a job, captures a succeeded JobResult, and persists terminal rows", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    const result = yield* runner.dispatch(job);

    expect(result.status).toBe("succeeded");
    expect("error" in result).toBe(false);
    expect(result.payload).toStrictEqual({
      transcriptRef: "transcript://execution-job-1",
      entries: 1,
    });

    const store = yield* StateStore;
    const persistedJob = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(persistedJob.status).toBe("succeeded");
    expect(persistedJob.executionId).toBe("execution-job-1");
    expect(persistedJob.transcriptRef).toBe("transcript://execution-job-1");

    const forJob = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id));
    expect(forJob.id).toBe("execution-job-1");
    expect(forJob.status).toBe("completed");
    // Reading the same execution by its id round-trips identically.
    const byId = Option.getOrThrow(yield* store.jobs.getExecution(forJob.id));
    expect(byId).toStrictEqual(forJob);
  }).pipe(provide(fakeHandle(Stream.make(turnStarted, entryEvent), { _tag: "Completed" }))),
);

// ============================================================================
// Dual-modality tee — the fold routes durable vs ephemeral events
// ============================================================================

it.effect(
  "tees the whole fold — durable entries via append, ephemeral deltas via publishEphemeral",
  () =>
    Effect.gen(function* () {
      const appended = yield* Ref.make<ReadonlyArray<ExecutionEvent>>([]);
      const published = yield* Ref.make<ReadonlyArray<ExecutionEvent>>([]);
      // An execution emitting a MIX of ephemeral (TurnStarted, MessageDelta) and durable
      // (EntryAppended, Notice) events, interleaved.
      const handle = fakeHandle(Stream.make(turnStarted, entryEvent, messageDelta, noticeEvent), {
        _tag: "Completed",
      });

      yield* Effect.gen(function* () {
        const job = yield* makeJob();
        const runner = yield* JobRunner;
        yield* runner.dispatch(job);

        // Only the durable events reached the durable transcript log (ephemerals never persist).
        const store = yield* StateStore;
        const executionId = yield* Schema.decodeUnknownEffect(ExecutionId)("execution-job-1").pipe(
          Effect.orDie,
        );
        const persisted = yield* store.executionLog.read(executionId);
        expect(persisted.map((e) => e.event)).toEqual([entryEvent, noticeEvent]);
      }).pipe(
        Effect.scoped,
        Effect.provide(layer),
        Effect.provide(fakeRunner(handle)),
        Effect.provide(recordingStore(appended, published)),
      );

      // The fold DROPPED NOTHING: durable events took `append`, ephemeral deltas `publishEphemeral`.
      expect(yield* Ref.get(appended)).toEqual([entryEvent, noticeEvent]);
      expect(yield* Ref.get(published)).toEqual([turnStarted, messageDelta]);
    }),
);

// A TRANSIENT `executionLog.append` failure must drop ONLY that entry and keep folding — never
// tear down the whole writer and silently truncate every SUBSEQUENT durable entry (a data-loss
// path for a durability feature). A store whose append fails ONCE (on the middle entry) still
// persists the entries BEFORE and AFTER it.
it.effect(
  "a transient executionLog.append failure drops one entry but keeps folding (no truncation)",
  () =>
    Effect.gen(function* () {
      const first = entryEvent;
      const poison: ExecutionEvent = {
        _tag: "EntryAppended",
        entry: { _tag: "AssistantMessage", id: "poison", text: "x" },
      };
      const third: ExecutionEvent = {
        _tag: "EntryAppended",
        entry: { _tag: "AssistantMessage", id: "third", text: "z" },
      };
      // A StateStore whose `executionLog.append` fails transiently for the poison entry only.
      const failAppendOnPoison: Layer.Layer<StateStore, StateStoreError> = Layer.effect(
        StateStore,
        Effect.gen(function* () {
          const base = yield* StateStore;
          return StateStore.of({
            ...base,
            executionLog: {
              ...base.executionLog,
              append: (executionId, event) =>
                event === poison
                  ? Effect.fail(new StateStoreError({ operation: "append", detail: "transient" }))
                  : base.executionLog.append(executionId, event),
            },
          });
        }),
      ).pipe(Layer.provide(layerMemory));

      yield* Effect.gen(function* () {
        const job = yield* makeJob();
        const runner = yield* JobRunner;
        yield* runner.dispatch(job);

        const store = yield* StateStore;
        const executionId = yield* Schema.decodeUnknownEffect(ExecutionId)("execution-job-1").pipe(
          Effect.orDie,
        );
        const persisted = yield* store.executionLog.read(executionId);
        // The poison entry was dropped, but folding CONTINUED — `first` and `third` both persisted.
        expect(persisted.map((e) => e.event)).toEqual([first, third]);
      }).pipe(
        Effect.scoped,
        Effect.provide(layer),
        Effect.provide(
          fakeRunner(fakeHandle(Stream.make(first, poison, third), { _tag: "Completed" })),
        ),
        Effect.provide(failAppendOnPoison),
      );
    }),
);

// ============================================================================
// F1 terminal-result contract — the events fold is BOUNDED by handle.result
// ============================================================================

it.effect(
  "completes dispatch even when the events stream never naturally ends (handle.result bounds the fold)",
  () =>
    Effect.gen(function* () {
      const job = yield* makeJob();

      const runner = yield* JobRunner;
      // Dispatch MUST complete — not hang. If the fold were not bounded by
      // `handle.result`, `Stream.never` would block it forever (the fold reads
      // `handle.result` only AFTER the fold ends). The already-settled terminal
      // interrupts the fold so dispatch proceeds. Guarded by the test timeout: a
      // regression here fails as a timeout rather than hanging the suite.
      const result = yield* runner.dispatch(job);
      expect(result.status).toBe("succeeded");

      // The terminal rows are still persisted from the (bounded) dispatch.
      const store = yield* StateStore;
      expect(Option.getOrThrow(yield* store.jobs.getJob(job.id)).status).toBe("succeeded");
    }).pipe(
      // An events stream that emits, then NEVER ends — simulating a truncating event
      // that slid out of this subscription's window (or a still-live `pi`). Only
      // `handle.result` (Completed) can bound the fold.
      provide(
        fakeHandle(Stream.make(entryEvent).pipe(Stream.concat(Stream.never)), {
          _tag: "Completed",
        }),
      ),
    ),
);

// ============================================================================
// Real Issue-content prompt (CE1.1) — not the id-only placeholder
// ============================================================================

it.effect("drives a prompt built from the real Issue content (number + title), not the id", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();
    // Persist the Issue this job advances so the prompt is derived from real content.
    const store = yield* StateStore;
    const issue = yield* decode(Issue, {
      id: "issue-22",
      epicId: "epic-1",
      number: 22,
      title: "Postgres sink batching",
      status: "in_progress",
      dependsOn: [],
    });
    yield* store.workGraph.putIssue(issue);

    const sent = yield* Ref.make<ReadonlyArray<ExecutionInput>>([]);
    const handle = recordingHandle(Stream.make(turnStarted), { _tag: "Completed" }, sent);

    // Dispatch against the SAME (outer) `StateStore` — the JobRunner + fake runner
    // layers are provided here, but `StateStore` is inherited from the outer context
    // so the Issue persisted above is the one the prompt is derived from.
    yield* Effect.gen(function* () {
      const runner = yield* JobRunner;
      yield* runner.dispatch(job);
    }).pipe(Effect.provide(layer), Effect.provide(fakeRunner(handle)));

    const driven = yield* Ref.get(sent);
    expect(driven).toHaveLength(1);
    const prompt = driven[0];
    expect(prompt?.mode).toBe("prompt");
    // The prompt carries the real Issue number and title — not the opaque issueId.
    expect(prompt?.text).toContain("#22");
    expect(prompt?.text).toContain("Postgres sink batching");
    expect(prompt?.text).not.toContain("issue-22");
  }).pipe(Effect.scoped, Effect.provide(layerMemory)),
);

// ============================================================================
// Failing execution
// ============================================================================

it.effect("dispatches a job, captures a failed JobResult, and persists a failed execution", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    const result = yield* runner.dispatch(job);

    expect(result.status).toBe("failed");
    expect(result.error).toBe("pi transport closed");
    // The one entry emitted BEFORE the stream tore down is preserved — the count is
    // held in a Ref, so a teardown of the fold no longer discards it.
    expect(result.payload).toStrictEqual({
      transcriptRef: "transcript://execution-job-1",
      entries: 1,
    });

    const store = yield* StateStore;
    const persistedJob = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(persistedJob.status).toBe("failed");

    const persistedExecution = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id));
    expect(persistedExecution.status).toBe("failed");
    expect(persistedExecution.id).toBe("execution-job-1");
  }).pipe(
    provide(
      fakeHandle(
        Stream.make(entryEvent).pipe(
          Stream.concat(
            Stream.fail(new PiTransportError({ reason: "closed", detail: "pi transport closed" })),
          ),
        ),
        { _tag: "Failed", error: "pi transport closed" },
      ),
    ),
  ),
);

// ============================================================================
// Failure paths through dispatch move the durable rows OUT of limbo (no stuck running)
// ============================================================================

it.effect("fails and persists a failed terminal when the runner cannot start the execution", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();
    const runner = yield* JobRunner;

    const error = yield* runner.dispatch(job).pipe(Effect.flip);
    expect(error._tag).toBe("ExecutionRunnerError");

    // The initial persist wrote running/starting; a run failure must not leave that
    // limbo — the durable rows are moved to a failed terminal.
    const store = yield* StateStore;
    expect(Option.getOrThrow(yield* store.jobs.getJob(job.id)).status).toBe("failed");
    expect(Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id)).status).toBe("failed");
  }).pipe(
    provideRunner(
      Layer.succeed(
        ExecutionRunner,
        ExecutionRunner.of({
          run: () =>
            Effect.fail(new ExecutionRunnerError({ operation: "run", detail: "spawn refused" })),
        }),
      ),
    ),
  ),
);

it.effect("fails and persists a failed terminal when driving the execution fails", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();
    const runner = yield* JobRunner;

    const error = yield* runner.dispatch(job).pipe(Effect.flip);
    expect(error._tag).toBe("PiTransportError");

    const store = yield* StateStore;
    expect(Option.getOrThrow(yield* store.jobs.getJob(job.id)).status).toBe("failed");
    expect(Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id)).status).toBe("failed");
  }).pipe(
    provide({
      ...fakeHandle(Stream.empty, { _tag: "Completed" }),
      send: () =>
        Effect.fail(new PiTransportError({ reason: "closed", detail: "send after close" })),
    }),
  ),
);

// ============================================================================
// 1 Job = 1 execution — re-dispatch re-attaches the SAME execution id
// ============================================================================

it.effect("re-dispatch re-attaches the same execution id, never a second execution", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    yield* runner.dispatch(job);

    // A restart reloads the persisted job — which now carries its executionId — and
    // re-dispatches. `dispatch` must reuse that SAME id (upsert), not mint a new one.
    const store = yield* StateStore;
    const reloaded = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(reloaded.executionId).toBe("execution-job-1");

    const second = yield* runner.dispatch(reloaded);
    expect(second.status).toBe("succeeded");

    const forJob = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id));
    expect(forJob.id).toBe("execution-job-1");
  }).pipe(provide(fakeHandle(Stream.make(entryEvent), { _tag: "Completed" }))),
);

// ============================================================================
// Merged single-transcript on re-dispatch (issue #77) — a retry APPENDS to the
// SAME per-execution log (offsets monotonic, never reset), and `entries` counts the
// MERGED transcript across runs, not just the latest run.
// ============================================================================

it.effect(
  "re-dispatch APPENDS to one durable transcript (offsets monotonic) and entries counts the merged log",
  () =>
    Effect.gen(function* () {
      const job = yield* makeJob();
      const store = yield* StateStore;
      const executionId = yield* Schema.decodeUnknownEffect(ExecutionId)("execution-job-1").pipe(
        Effect.orDie,
      );

      // Two runs of the SAME job (same execution id), each emitting one distinct durable
      // entry, so the merged transcript is unambiguously the concatenation of both runs.
      const run1Entry: ExecutionEvent = {
        _tag: "EntryAppended",
        entry: { _tag: "UserMessage", id: "run-1", text: "first run" },
      };
      const run2Entry: ExecutionEvent = {
        _tag: "EntryAppended",
        entry: { _tag: "AssistantMessage", id: "run-2", text: "second run" },
      };

      // First dispatch — its own runner/handle, sharing the OUTER StateStore so the
      // durable transcript persists across both dispatches.
      const first = yield* Effect.gen(function* () {
        const runner = yield* JobRunner;
        return yield* runner.dispatch(job);
      }).pipe(
        Effect.scoped,
        Effect.provide(layer),
        Effect.provide(fakeRunner(fakeHandle(Stream.make(run1Entry), { _tag: "Completed" }))),
      );
      // After run 1 the count reflects run 1's single entry.
      expect(first.payload).toStrictEqual({
        transcriptRef: "transcript://execution-job-1",
        entries: 1,
      });

      // A restart reloads the persisted job (now carrying its executionId) and
      // re-dispatches against the SAME execution id.
      const reloaded = Option.getOrThrow(yield* store.jobs.getJob(job.id));
      expect(reloaded.executionId).toBe("execution-job-1");

      const second = yield* Effect.gen(function* () {
        const runner = yield* JobRunner;
        return yield* runner.dispatch(reloaded);
      }).pipe(
        Effect.scoped,
        Effect.provide(layer),
        Effect.provide(fakeRunner(fakeHandle(Stream.make(run2Entry), { _tag: "Completed" }))),
      );

      // (a) The durable transcript APPENDED: both runs' entries present, in order.
      const durable = yield* store.executionLog.read(executionId);
      expect(durable.map((e) => e.event)).toEqual([run1Entry, run2Entry]);
      // Offsets are monotonically increasing and DISTINCT — the re-dispatch never reset
      // the sequence (run 2's offset is strictly greater than run 1's).
      const offsets = durable.map((e) => e.offset);
      expect(offsets.length).toBe(2);
      expect(offsets[1]).toBeGreaterThan(offsets[0] ?? 0);

      // (b) `entries` counts the MERGED transcript (both runs), not just the latest run.
      expect(second.payload).toStrictEqual({
        transcriptRef: "transcript://execution-job-1",
        entries: 2,
      });
    }).pipe(Effect.scoped, Effect.provide(layerMemory)),
);
