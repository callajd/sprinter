/**
 * `layerRegisterExecutions` coverage (CE4.1) — the decorator that registers each
 * dispatched {@link ExecutionHandle} in the {@link ExecutionRegistry}, proven against a
 * FAKE inner {@link ExecutionRunner} (a canned handle, no `pi`). It asserts the wire
 * the acceptance loop rests on:
 *
 *   - a `run` registers its handle under `executionIdFor(job)`, so the execution channel
 *     can resolve the SAME execution a command dispatched (the gap this closes);
 *   - registration is `Scope`-managed: once the run's scope closes, the entry is gone
 *     (a settled execution is no longer resolvable — `ExecutionNotFound`);
 *   - the id keys on `executionIdFor` (the job's own `executionId` when present), matching
 *     what `JobRunner.dispatch` persists and the app reads back.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { Job, ExecutionId } from "@sprinter/domain";
import { ExecutionRunner, executionIdFor } from "@sprinter/job";
import { type ExecutionHandle, ExecutionResult } from "@sprinter/runner";
import { layerExecutionRegistry, ExecutionRegistry } from "./index.ts";
import { layerRegisterExecutions } from "./execution-runner.ts";

/** A canned neutral {@link ExecutionHandle} — never driven; identity is all the test needs. */
const cannedHandle: ExecutionHandle = {
  pid: ChildProcessSpawner.ProcessId(4321),
  events: Stream.empty,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed(Schema.decodeUnknownSync(ExecutionResult)({ _tag: "Completed" })),
};

/** A fake inner {@link ExecutionRunner} that hands back the {@link cannedHandle}. */
const fakeInner: Layer.Layer<ExecutionRunner> = Layer.succeed(
  ExecutionRunner,
  ExecutionRunner.of({ run: () => Effect.succeed(cannedHandle) }),
);

/** The decorated runner over the fake inner + a real registry. */
const decorated = layerRegisterExecutions(fakeInner).pipe(
  Layer.provideMerge(layerExecutionRegistry),
);

const makeJob = (raw: (typeof Job)["Encoded"]): Job => Schema.decodeUnknownSync(Job)(raw);

it.effect("registers the dispatched handle under executionIdFor(job) for the run scope", () =>
  Effect.gen(function* () {
    const registry = yield* ExecutionRegistry;
    const runner = yield* ExecutionRunner;
    const job = makeJob({ id: "job-1", issueId: "issue-1", kind: "implement", status: "queued" });
    const id = yield* executionIdFor(job);

    // Inside the run scope: the handle is resolvable under the derived execution id —
    // the wire that lets the execution channel drive the just-dispatched execution.
    yield* Effect.gen(function* () {
      const handle = yield* runner.run(job);
      const resolved = yield* registry.get(id);
      expect(resolved).toBe(handle);
      expect(resolved).toBe(cannedHandle);
    }).pipe(Effect.scoped);

    // After the run scope closes, the entry is gone — a settled execution is no longer
    // resolvable (`ExecutionNotFound`), matching the registry's lifetime contract.
    const afterTeardown = yield* registry.get(id).pipe(Effect.flip);
    expect(afterTeardown._tag).toBe("ExecutionNotFound");
  }).pipe(Effect.provide(decorated)),
);

it.effect("keys on the job's existing executionId when it carries one (re-dispatch)", () =>
  Effect.gen(function* () {
    const registry = yield* ExecutionRegistry;
    const runner = yield* ExecutionRunner;
    const reusedId = Schema.decodeUnknownSync(ExecutionId)("execution-reused");
    const job = makeJob({
      id: "job-1",
      issueId: "issue-1",
      kind: "implement",
      status: "running",
      executionId: "execution-reused",
    });

    yield* Effect.gen(function* () {
      yield* runner.run(job);
      // Resolvable under the REUSED id (not a fresh `execution-<jobId>`), so a
      // re-dispatch re-attaches the channel to the same durable execution id.
      const resolved = yield* registry.get(reusedId);
      expect(resolved).toBe(cannedHandle);
    }).pipe(Effect.scoped);
  }).pipe(Effect.provide(decorated)),
);
