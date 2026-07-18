/**
 * `layerLocalPi` coverage (CE1.1) — the concrete `LocalPi` `ExecutionRunner`
 * adapter exercised against a FAKE `ChildProcessSpawner` (the same offline pattern
 * as the runner's own tests), never a real `pi` binary. The tests prove the two
 * hard requirements the adapter owns:
 *
 *   - the **terminal-result contract**: a real `pi --mode rpc` stays alive after a
 *     turn, so the adapter must make the dispatch one-shot — the handle's `events`
 *     END at the first `SessionIdle` (Pi's `agent_settled`) and its `result`
 *     resolves `Completed`, so `JobRunner.dispatch` never hangs;
 *   - a transport teardown resolves `result` as `Failed` (with a neutral detail);
 *   - a spawn failure is translated into the owned {@link ExecutionRunnerError} at
 *     the boundary — no `PlatformError` crosses the port (INV-PORT).
 */
import { it } from "@effect/vitest";
import {
  Cause,
  Effect,
  Fiber,
  Layer,
  PlatformError,
  Queue,
  Ref,
  Schema,
  Sink,
  Stream,
} from "effect";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { Job } from "@sprinter/domain";
import { SessionResult } from "@sprinter/runner";
import { ExecutionRunner, layerInheritCwd, layerLocalPi } from "./index.ts";

/**
 * The adapter under test, with the no-op {@link layerInheritCwd} spawn router
 * provided (CE1.1-F2): these tests exercise the terminal-result contract and the
 * boundary, not per-Job worktree routing (covered in `./spawn-router.test.ts`), so
 * a Job inherits the parent cwd. Only the `ChildProcessSpawner` stays a per-test
 * substitution (the fake `pi`).
 */
const runnerLayer = layerLocalPi.pipe(Layer.provide(layerInheritCwd));

// ============================================================================
// Fixtures & fakes
// ============================================================================

const decode = <S extends Schema.Top>(schema: S, raw: S["Encoded"]) =>
  Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.orDie);

const makeJob = () =>
  decode(Job, { id: "job-1", issueId: "issue-22", kind: "implement", status: "queued" });

/** A fake `ChildProcessSpawner` plus the handles the tests drive the fake `pi` through. */
const makeFakePi = Effect.gen(function* () {
  const stdoutRaw = yield* Queue.make<unknown, Cause.Done>();
  const stdinBytes = yield* Queue.make<Uint8Array, Cause.Done>();
  const killed = yield* Ref.make(false);

  const spawner = ChildProcessSpawner.make(() =>
    Effect.acquireRelease(
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(4321),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          stdin: Sink.forEach<Uint8Array, boolean, never, never>((chunk) =>
            Queue.offer(stdinBytes, chunk),
          ),
          stdout: Stream.fromQueue(stdoutRaw).pipe(
            Stream.pipeThroughChannel(Ndjson.encode()),
            Stream.orDie,
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        }),
      ),
      () => Ref.set(killed, true),
    ),
  );

  return {
    layer: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    stdoutRaw,
    stdinBytes,
    killed,
  } as const;
});

// ============================================================================
// Terminal-result contract — one-shot settle
// ============================================================================

it.effect("makes the dispatch one-shot: events end at agent_settled and result is Completed", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runner = yield* ExecutionRunner;
        const job = yield* makeJob();
        const handle = yield* runner.run(job);
        expect(handle.pid).toBe(ChildProcessSpawner.ProcessId(4321));

        // A raw pi would keep emitting after the turn; the adapter truncates at the
        // first SessionIdle so this collect TERMINATES rather than hanging.
        const collecting = yield* Effect.forkChild(Stream.runCollect(handle.events));
        yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
        yield* Queue.offer(fake.stdoutRaw, { type: "agent_settled" });
        // A trailing event after settle must NOT be delivered — the stream already ended.
        yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });

        const events = yield* Fiber.join(collecting);
        expect(events).toEqual([{ _tag: "TurnStarted" }, { _tag: "SessionIdle" }]);

        const result = yield* handle.result;
        expect(result).toEqual(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" }));
      }),
    ).pipe(Effect.provide(runnerLayer), Effect.provide(fake.layer));
  }),
);

it.effect("settles Completed when pi output closes cleanly before any settle", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runner = yield* ExecutionRunner;
        const job = yield* makeJob();
        const handle = yield* runner.run(job);

        const collecting = yield* Effect.forkChild(Stream.runCollect(handle.events));
        yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
        yield* Queue.end(fake.stdoutRaw);

        const events = yield* Fiber.join(collecting);
        expect(events).toEqual([{ _tag: "TurnStarted" }]);

        const result = yield* handle.result;
        expect(result).toEqual(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" }));
      }),
    ).pipe(Effect.provide(runnerLayer), Effect.provide(fake.layer));
  }),
);

it.effect(
  "gates the settle-watcher: a pre-prompt agent_settled does NOT truncate or complete (CE1.1-F1)",
  () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const runner = yield* ExecutionRunner;
          const job = yield* makeJob();
          const handle = yield* runner.run(job);

          const collecting = yield* Effect.forkChild(Stream.runCollect(handle.events));

          // The stdout queue is FIFO, so the pump processes these in order:
          //  1. a real `pi` idles-on-startup, emitting `agent_settled` BEFORE any
          //     prompt — the gate must DROP it (never truncate/complete here);
          //  2. the prompt drives a turn (`turn_start`), the agent works, and it
          //     settles — NOW the one-shot truncates, after real work.
          // A broken gate would truncate at (1), ending the collect as `[SessionIdle]`
          // (or `[]`) with the later events ignored — which this assertion catches.
          yield* Queue.offer(fake.stdoutRaw, { type: "agent_settled" });
          yield* Queue.offer(fake.stdoutRaw, { type: "turn_start" });
          yield* Queue.offer(fake.stdoutRaw, { type: "agent_settled" });

          const events = yield* Fiber.join(collecting);
          // The pre-prompt settle was dropped; the terminal settle follows a real turn.
          expect(events).toEqual([{ _tag: "TurnStarted" }, { _tag: "SessionIdle" }]);

          const result = yield* handle.result;
          expect(result).toEqual(Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" }));
        }),
      ).pipe(Effect.provide(runnerLayer), Effect.provide(fake.layer));
    }),
);

it.effect("settles Failed with a neutral detail when the transport tears down", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runner = yield* ExecutionRunner;
        const job = yield* makeJob();
        const handle = yield* runner.run(job);

        const collecting = yield* Effect.forkChild(Effect.exit(Stream.runCollect(handle.events)));
        // An undecodable stdout line fails the transport (a stream error).
        yield* Queue.offer(fake.stdoutRaw, { type: "totally_unknown_event" });

        const exit = yield* Fiber.join(collecting);
        expect(exit._tag).toBe("Failure");

        const result = yield* handle.result;
        expect(result._tag).toBe("Failed");
        if (result._tag === "Failed") expect(result.error.length).toBeGreaterThan(0);
      }),
    ).pipe(Effect.provide(runnerLayer), Effect.provide(fake.layer));
  }),
);

// ============================================================================
// Boundary — a spawn failure becomes the owned ExecutionRunnerError (INV-PORT)
// ============================================================================

it.effect("translates a spawn failure into the owned ExecutionRunnerError", () =>
  Effect.gen(function* () {
    const failingSpawner = ChildProcessSpawner.make(() =>
      Effect.fail(
        new PlatformError.PlatformError(
          new PlatformError.BadArgument({
            module: "ChildProcessSpawner",
            method: "spawn",
            description: "spawn refused",
          }),
        ),
      ),
    );
    const failingLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, failingSpawner);

    const error = yield* Effect.scoped(
      Effect.gen(function* () {
        const runner = yield* ExecutionRunner;
        const job = yield* makeJob();
        return yield* runner.run(job).pipe(Effect.flip);
      }),
    ).pipe(Effect.provide(runnerLayer), Effect.provide(failingLayer));

    expect(error._tag).toBe("ExecutionRunnerError");
    expect(error.operation).toBe("run");
    expect(error.detail).toContain("spawn refused");
  }),
);
