/**
 * `layerRegisterSessions` coverage (CE4.1) — the decorator that registers each
 * dispatched {@link SessionHandle} in the {@link SessionRegistry}, proven against a
 * FAKE inner {@link ExecutionRunner} (a canned handle, no `pi`). It asserts the wire
 * the acceptance loop rests on:
 *
 *   - a `run` registers its handle under `sessionIdFor(job)`, so the session channel
 *     can resolve the SAME session a command dispatched (the gap this closes);
 *   - registration is `Scope`-managed: once the run's scope closes, the entry is gone
 *     (a settled session is no longer resolvable — `SessionNotFound`);
 *   - the id keys on `sessionIdFor` (the job's own `sessionId` when present), matching
 *     what `JobRunner.dispatch` persists and the app reads back.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { Job, SessionId } from "@sprinter/domain";
import { ExecutionRunner, sessionIdFor } from "@sprinter/job";
import { type SessionHandle, SessionResult } from "@sprinter/runner";
import { layerSessionRegistry, SessionRegistry } from "./index.ts";
import { layerRegisterSessions } from "./session-runner.ts";

/** A canned neutral {@link SessionHandle} — never driven; identity is all the test needs. */
const cannedHandle: SessionHandle = {
  pid: ChildProcessSpawner.ProcessId(4321),
  events: Stream.empty,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" })),
};

/** A fake inner {@link ExecutionRunner} that hands back the {@link cannedHandle}. */
const fakeInner: Layer.Layer<ExecutionRunner> = Layer.succeed(
  ExecutionRunner,
  ExecutionRunner.of({ run: () => Effect.succeed(cannedHandle) }),
);

/** The decorated runner over the fake inner + a real registry. */
const decorated = layerRegisterSessions(fakeInner).pipe(Layer.provideMerge(layerSessionRegistry));

const makeJob = (raw: (typeof Job)["Encoded"]): Job => Schema.decodeUnknownSync(Job)(raw);

it.effect("registers the dispatched handle under sessionIdFor(job) for the run scope", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    const runner = yield* ExecutionRunner;
    const job = makeJob({ id: "job-1", issueId: "issue-1", kind: "implement", status: "queued" });
    const id = yield* sessionIdFor(job);

    // Inside the run scope: the handle is resolvable under the derived session id —
    // the wire that lets the session channel drive the just-dispatched session.
    yield* Effect.gen(function* () {
      const handle = yield* runner.run(job);
      const resolved = yield* registry.get(id);
      expect(resolved).toBe(handle);
      expect(resolved).toBe(cannedHandle);
    }).pipe(Effect.scoped);

    // After the run scope closes, the entry is gone — a settled session is no longer
    // resolvable (`SessionNotFound`), matching the registry's lifetime contract.
    const afterTeardown = yield* registry.get(id).pipe(Effect.flip);
    expect(afterTeardown._tag).toBe("SessionNotFound");
  }).pipe(Effect.provide(decorated)),
);

it.effect("keys on the job's existing sessionId when it carries one (re-dispatch)", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    const runner = yield* ExecutionRunner;
    const reusedId = Schema.decodeUnknownSync(SessionId)("session-reused");
    const job = makeJob({
      id: "job-1",
      issueId: "issue-1",
      kind: "implement",
      status: "running",
      sessionId: "session-reused",
    });

    yield* Effect.gen(function* () {
      yield* runner.run(job);
      // Resolvable under the REUSED id (not a fresh `session-<jobId>`), so a
      // re-dispatch re-attaches the channel to the same durable session id.
      const resolved = yield* registry.get(reusedId);
      expect(resolved).toBe(cannedHandle);
    }).pipe(Effect.scoped);
  }).pipe(Effect.provide(decorated)),
);
