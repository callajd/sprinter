/**
 * `JobRunner` coverage (AE3.1) — the single-Issue Job runner exercised against
 * FAKES: a fake {@link ExecutionRunner} handing back a canned {@link SessionHandle}
 * and the in-memory {@link StateStore} adapter (`layerMemory`). Deterministic and
 * offline — no `pi` process, no filesystem (INV-PORT): the runner depends only on
 * the two ports and the runner's neutral handle surface.
 *
 * The suite proves the dispatch → terminal `JobResult` capture → persisted rows
 * flow for BOTH a succeeding and a failing session, plus the 1 Job = 1 session
 * re-attach invariant (a re-dispatch upserts the SAME session id, never a new one).
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { Issue, Job, type SessionEvent, type SessionInput } from "@sprinter/domain";
import { PiTransportError, type SessionHandle, type SessionResult } from "@sprinter/runner";
import { layerMemory, StateStore } from "@sprinter/state";
import { ExecutionRunner, ExecutionRunnerError, JobRunner, layer } from "./index.ts";

// ============================================================================
// Fixtures & fakes — decoded through the owned schemas (no casts)
// ============================================================================

const decode = <S extends Schema.Top>(schema: S, raw: S["Encoded"]) =>
  Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.orDie);

const makeJob = (over: Partial<(typeof Job)["Encoded"]> = {}) =>
  decode(Job, { id: "job-1", issueId: "issue-22", kind: "implement", status: "queued", ...over });

const entryEvent: SessionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "UserMessage", id: "u1", text: "hi" },
};
const turnStarted: SessionEvent = { _tag: "TurnStarted" };

/** A fake {@link SessionHandle}: a canned event stream and terminal result; the drive/answer verbs are inert. */
const fakeHandle = (
  events: Stream.Stream<SessionEvent, PiTransportError>,
  result: SessionResult,
): SessionHandle => ({
  pid: ChildProcessSpawner.ProcessId(4242),
  events,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed(result),
});

/** As {@link fakeHandle}, but records every {@link SessionInput} driven in via `send`. */
const recordingHandle = (
  events: Stream.Stream<SessionEvent, PiTransportError>,
  result: SessionResult,
  sent: Ref.Ref<ReadonlyArray<SessionInput>>,
): SessionHandle => ({
  ...fakeHandle(events, result),
  send: (input) => Ref.update(sent, (xs) => [...xs, input]),
});

/** A fake {@link ExecutionRunner} that hands back a fixed handle for every job. */
const fakeRunner = (handle: SessionHandle): Layer.Layer<ExecutionRunner> =>
  Layer.succeed(ExecutionRunner, ExecutionRunner.of({ run: () => Effect.succeed(handle) }));

/** Provide the runner-under-test plus its two faked ports; expose `StateStore` for assertions. */
const provide =
  (handle: SessionHandle) =>
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
// Succeeding session
// ============================================================================

it.effect("dispatches a job, captures a succeeded JobResult, and persists terminal rows", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    const result = yield* runner.dispatch(job);

    expect(result.status).toBe("succeeded");
    expect("error" in result).toBe(false);
    expect(result.payload).toStrictEqual({
      transcriptRef: "transcript://session-job-1",
      entries: 1,
    });

    const store = yield* StateStore;
    const persistedJob = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(persistedJob.status).toBe("succeeded");
    expect(persistedJob.sessionId).toBe("session-job-1");
    expect(persistedJob.transcriptRef).toBe("transcript://session-job-1");

    const forJob = Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id));
    expect(forJob.id).toBe("session-job-1");
    expect(forJob.status).toBe("completed");
    // Reading the same session by its id round-trips identically.
    const byId = Option.getOrThrow(yield* store.jobs.getSession(forJob.id));
    expect(byId).toStrictEqual(forJob);
  }).pipe(provide(fakeHandle(Stream.make(turnStarted, entryEvent), { _tag: "Completed" }))),
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

    const sent = yield* Ref.make<ReadonlyArray<SessionInput>>([]);
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
// Failing session
// ============================================================================

it.effect("dispatches a job, captures a failed JobResult, and persists a failed session", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    const result = yield* runner.dispatch(job);

    expect(result.status).toBe("failed");
    expect(result.error).toBe("pi transport closed");
    // The one entry emitted BEFORE the stream tore down is preserved — the count is
    // held in a Ref, so a teardown of the fold no longer discards it.
    expect(result.payload).toStrictEqual({
      transcriptRef: "transcript://session-job-1",
      entries: 1,
    });

    const store = yield* StateStore;
    const persistedJob = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(persistedJob.status).toBe("failed");

    const persistedSession = Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id));
    expect(persistedSession.status).toBe("failed");
    expect(persistedSession.id).toBe("session-job-1");
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

it.effect("fails and persists a failed terminal when the runner cannot start the session", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();
    const runner = yield* JobRunner;

    const error = yield* runner.dispatch(job).pipe(Effect.flip);
    expect(error._tag).toBe("ExecutionRunnerError");

    // The initial persist wrote running/starting; a run failure must not leave that
    // limbo — the durable rows are moved to a failed terminal.
    const store = yield* StateStore;
    expect(Option.getOrThrow(yield* store.jobs.getJob(job.id)).status).toBe("failed");
    expect(Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id)).status).toBe("failed");
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

it.effect("fails and persists a failed terminal when driving the session fails", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();
    const runner = yield* JobRunner;

    const error = yield* runner.dispatch(job).pipe(Effect.flip);
    expect(error._tag).toBe("PiTransportError");

    const store = yield* StateStore;
    expect(Option.getOrThrow(yield* store.jobs.getJob(job.id)).status).toBe("failed");
    expect(Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id)).status).toBe("failed");
  }).pipe(
    provide({
      ...fakeHandle(Stream.empty, { _tag: "Completed" }),
      send: () =>
        Effect.fail(new PiTransportError({ reason: "closed", detail: "send after close" })),
    }),
  ),
);

// ============================================================================
// 1 Job = 1 session — re-dispatch re-attaches the SAME session id
// ============================================================================

it.effect("re-dispatch re-attaches the same session id, never a second session", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    yield* runner.dispatch(job);

    // A restart reloads the persisted job — which now carries its sessionId — and
    // re-dispatches. `dispatch` must reuse that SAME id (upsert), not mint a new one.
    const store = yield* StateStore;
    const reloaded = Option.getOrThrow(yield* store.jobs.getJob(job.id));
    expect(reloaded.sessionId).toBe("session-job-1");

    const second = yield* runner.dispatch(reloaded);
    expect(second.status).toBe("succeeded");

    const forJob = Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id));
    expect(forJob.id).toBe("session-job-1");
  }).pipe(provide(fakeHandle(Stream.make(entryEvent), { _tag: "Completed" }))),
);
