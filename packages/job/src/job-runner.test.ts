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
import { Effect, Layer, Option, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { Job, type SessionEvent } from "@sprinter/domain";
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

/** A fake {@link ExecutionRunner} that hands back a fixed handle for every job. */
const fakeRunner = (handle: SessionHandle): Layer.Layer<ExecutionRunner> =>
  Layer.succeed(ExecutionRunner, ExecutionRunner.of({ run: () => Effect.succeed(handle) }));

/** Provide the runner-under-test plus its two faked ports; expose `StateStore` for assertions. */
const provide =
  (handle: SessionHandle) =>
  <A, E>(effect: Effect.Effect<A, E, JobRunner | StateStore | Scope>) =>
    effect.pipe(
      Effect.scoped,
      Effect.provide(layer),
      Effect.provide(fakeRunner(handle)),
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
// Failing session
// ============================================================================

it.effect("dispatches a job, captures a failed JobResult, and persists a failed session", () =>
  Effect.gen(function* () {
    const job = yield* makeJob();

    const runner = yield* JobRunner;
    const result = yield* runner.dispatch(job);

    expect(result.status).toBe("failed");
    expect(result.error).toBe("pi transport closed");
    expect(result.payload).toStrictEqual({
      transcriptRef: "transcript://session-job-1",
      entries: 0,
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
