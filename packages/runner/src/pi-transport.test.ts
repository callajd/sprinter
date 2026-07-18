/**
 * `PiTransport` substrate coverage (AE1.1).
 *
 * The `pi` binary is not spawned here — a fake `ChildProcessSpawner` stands in
 * (the same pattern the Effect repo uses to test `ChildProcess`), so the tests
 * are deterministic and run offline. The fake:
 *   - exposes `stdinBytes`, a queue of the RAW bytes the transport writes to the
 *     child's stdin — decoding them back through the owned `PiClientMessage`
 *     schema proves the stdin half of the NDJSON bridge; and
 *   - exposes `stdoutRaw`, a queue of raw server objects it re-encodes as NDJSON
 *     onto the child's stdout — proving the stdout half plus id-correlation and
 *     event routing.
 * `killed` records the spawn's scoped release, proving the process is torn down
 * on scope close (no orphaned `pi`).
 *
 * A live `pi --mode rpc` integration is intentionally out of scope for AE1.1:
 * the binary is not provisioned in this repo (Pi binary provisioning is a
 * tracked deferral in `docs/decisions.md`), and the FE2.2 live drift test was
 * removed for the same reason (#14). The transport builds only to the
 * `ChildProcessSpawner` port, so the real Bun spawner drops in unchanged.
 */
import { it } from "@effect/vitest";
import {
  Cause,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Ref,
  Schema,
  Sink,
  Stream,
} from "effect";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { PiClientMessage } from "@sprinter/domain/pi/wire";
import { make } from "./pi-transport.ts";

/** A realistic `get_state` snapshot, exactly as `pi` shapes it on the wire. */
const sessionStateData = {
  thinkingLevel: "off",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "all",
  followUpMode: "all",
  sessionId: "sid",
  autoCompactionEnabled: true,
  messageCount: 0,
  pendingMessageCount: 0,
};

/** A fake `ChildProcessSpawner` plus the handles the tests drive it through. */
const makeFakePi = Effect.gen(function* () {
  const stdoutRaw = yield* Queue.make<unknown, Cause.Done>();
  const stdinBytes = yield* Queue.make<Uint8Array, Cause.Done>();
  const killed = yield* Ref.make(false);
  // Captures the resolved `Command` the transport asked to spawn, so a test can
  // assert the command/args/options (e.g. that `env` extends rather than replaces).
  const spawned = yield* Ref.make<Option.Option<ChildProcess.Command>>(Option.none());

  const spawner = ChildProcessSpawner.make((command) =>
    Ref.set(spawned, Option.some(command)).pipe(
      Effect.andThen(
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
      ),
    ),
  );

  return {
    layer: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    stdoutRaw,
    stdinBytes,
    killed,
    spawned,
  } as const;
});

/**
 * Take exactly `count` `PiClientMessage`s from the captured stdin bytes,
 * decoding them through the owned schema. A single captured chunk may frame
 * several NDJSON lines (the outbound stream batches rapid sends), so this splits
 * on newlines and pulls further chunks until `count` messages are decoded.
 */
const takeClientMessages = (stdinBytes: Queue.Dequeue<Uint8Array, Cause.Done>, count: number) =>
  Effect.gen(function* () {
    const messages: Array<PiClientMessage> = [];
    while (messages.length < count) {
      const bytes = yield* Queue.take(stdinBytes);
      const lines = new TextDecoder()
        .decode(bytes)
        .split("\n")
        .filter((line) => line.length > 0);
      for (const line of lines) {
        const parsed = yield* Effect.try((): unknown => JSON.parse(line));
        messages.push(yield* Schema.decodeUnknownEffect(PiClientMessage)(parsed));
      }
    }
    return messages;
  });

/** Index into an array, throwing (failing the test) if the element is absent. */
const at = <A>(xs: ReadonlyArray<A>, index: number): A => {
  const value = xs[index];
  if (value === undefined) throw new Error(`expected element at index ${index}`);
  return value;
};

it.effect("spawns pi --mode rpc scope-managed and kills it on scope close", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make({
          command: "pi",
          args: ["--mode", "rpc"],
          cwd: "/tmp",
          env: { FOO: "bar" },
        });
        expect(transport.pid).toBe(ChildProcessSpawner.ProcessId(4321));
        expect(yield* Ref.get(fake.killed)).toBe(false);

        // The spawned command carries the configured executable/args/cwd, and
        // `env` EXTENDS the inherited environment (`extendEnv: true`) rather than
        // replacing it — otherwise PATH/HOME/credentials would be dropped.
        const command = yield* Ref.get(fake.spawned);
        if (Option.isNone(command)) throw new Error("expected the transport to have spawned");
        if (!ChildProcess.isStandardCommand(command.value)) {
          throw new Error("expected a standard command");
        }
        expect(command.value.command).toBe("pi");
        expect(command.value.args).toEqual(["--mode", "rpc"]);
        expect(command.value.options.cwd).toBe("/tmp");
        expect(command.value.options.env).toEqual({ FOO: "bar" });
        expect(command.value.options.extendEnv).toBe(true);
      }),
    ).pipe(Effect.provide(fake.layer));
    // The scope closed above → the spawn's scoped release ran.
    expect(yield* Ref.get(fake.killed)).toBe(true);
  }),
);

it.effect("spawns with no env override when config omits env (inherits the environment)", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* make();
        const command = yield* Ref.get(fake.spawned);
        if (Option.isNone(command)) throw new Error("expected the transport to have spawned");
        if (!ChildProcess.isStandardCommand(command.value)) {
          throw new Error("expected a standard command");
        }
        // No `env` → the spawner leaves it undefined → child inherits the parent's
        // environment. `extendEnv` stays unset for that (correct) default path.
        expect(command.value.options.env).toBeUndefined();
        expect(command.value.options.extendEnv).toBeUndefined();
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("correlates a command to its response by id, decoding real NDJSON both ways", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const fiber = yield* Effect.forkChild(transport.request({ type: "get_state" }));

        // The command is NDJSON-encoded from PiClientMessage onto stdin.
        const command = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        expect(command.type).toBe("get_state");
        expect(command.id).toBe("rpc-1");

        // pi answers with the correlated response on stdout.
        yield* Queue.offer(fake.stdoutRaw, {
          id: command.id,
          type: "response",
          command: "get_state",
          success: true,
          data: sessionStateData,
        });

        const response = yield* Fiber.join(fiber);
        expect(response.type).toBe("response");
        if (response.command === "get_state" && response.success) {
          expect(response.data.sessionId).toBe("sid");
        } else {
          throw new Error("expected a successful get_state response");
        }
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("matches concurrent requests to their own responses by id", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const first = yield* Effect.forkChild(
          transport.request({ type: "prompt", message: "one" }),
        );
        const second = yield* Effect.forkChild(
          transport.request({ type: "prompt", message: "two" }),
        );

        const commands = yield* takeClientMessages(fake.stdinBytes, 2);
        const a = at(commands, 0);
        const b = at(commands, 1);
        expect([a.id, b.id]).toEqual(["rpc-1", "rpc-2"]);

        // Answer out of order — correlation is by id, not arrival.
        yield* Queue.offer(fake.stdoutRaw, {
          id: b.id,
          type: "response",
          command: "prompt",
          success: true,
        });
        yield* Queue.offer(fake.stdoutRaw, {
          id: a.id,
          type: "response",
          command: "prompt",
          success: true,
        });

        const firstRes = yield* Fiber.join(first);
        const secondRes = yield* Fiber.join(second);
        expect(firstRes.id).toBe("rpc-1");
        expect(secondRes.id).toBe("rpc-2");
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("fails the caller with PiRpcError on an error response", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const fiber = yield* Effect.forkChild(transport.request({ type: "prompt", message: "go" }));
        const command = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        yield* Queue.offer(fake.stdoutRaw, {
          id: command.id,
          type: "response",
          command: "prompt",
          success: false,
          error: "No API key found.",
        });

        const exit = yield* Effect.exit(Fiber.join(fiber));
        expect(Exit.isFailure(exit)).toBe(true);
        const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
        if (Option.isSome(error) && error.value._tag === "PiRpcError") {
          expect(error.value.command).toBe("prompt");
          expect(error.value.error).toBe("No API key found.");
        } else {
          throw new Error("expected a PiRpcError failure");
        }
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("routes non-response server messages to the events stream in order", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const collecting = yield* Effect.forkChild(
          Stream.runCollect(Stream.take(transport.events, 2)),
        );
        yield* Queue.offer(fake.stdoutRaw, { type: "agent_start" });
        yield* Queue.offer(fake.stdoutRaw, { type: "session_info_changed", name: "s" });
        const events = yield* Fiber.join(collecting);
        expect(events.map((event) => event.type)).toEqual(["agent_start", "session_info_changed"]);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("send writes a fire-and-forget client message to stdin", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        yield* transport.send({ type: "extension_ui_response", id: "u1", value: "a" });
        const message = at(yield* takeClientMessages(fake.stdinBytes, 1), 0);
        expect(message.type).toBe("extension_ui_response");
        expect(message.id).toBe("u1");
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("fails outstanding requests and ends events when pi output closes", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const reqFiber = yield* Effect.forkChild(transport.request({ type: "get_state" }));
        const evFiber = yield* Effect.forkChild(Stream.runCollect(transport.events));
        yield* Queue.take(fake.stdinBytes); // the request is registered + sent
        yield* Queue.end(fake.stdoutRaw); // pi's stdout closes

        const reqExit = yield* Effect.exit(Fiber.join(reqFiber));
        expect(Exit.isFailure(reqExit)).toBe(true);
        const reqError = Exit.isFailure(reqExit)
          ? Cause.findErrorOption(reqExit.cause)
          : Option.none();
        if (Option.isSome(reqError) && reqError.value._tag === "PiTransportError") {
          expect(reqError.value.reason).toBe("closed");
        } else {
          throw new Error("expected a PiTransportError failure");
        }
        // events completes cleanly (empty) when stdout ends without error.
        const events = yield* Fiber.join(evFiber);
        expect(events.length).toBe(0);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("fails the transport with a stream error on an undecodable stdout line", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        const evFiber = yield* Effect.forkChild(Stream.runCollect(transport.events));
        yield* Queue.offer(fake.stdoutRaw, { type: "totally_unknown_event" });

        const exit = yield* Fiber.await(evFiber);
        expect(Exit.isFailure(exit)).toBe(true);
        const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
        if (Option.isSome(error) && error.value._tag === "PiTransportError") {
          expect(error.value.reason).toBe("stream");
        } else {
          throw new Error("expected a PiTransportError failure");
        }
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("drops a response whose id matches no outstanding request", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        // A response for an id we never sent (nor an `id`-less one) must be dropped
        // silently — not crash the pump — and must not disturb the events stream.
        const collecting = yield* Effect.forkChild(
          Stream.runCollect(Stream.take(transport.events, 1)),
        );
        yield* Queue.offer(fake.stdoutRaw, {
          id: "rpc-999",
          type: "response",
          command: "get_state",
          success: true,
          data: sessionStateData,
        });
        yield* Queue.offer(fake.stdoutRaw, { type: "agent_start" });
        const events = yield* Fiber.join(collecting);
        expect(events.map((event) => event.type)).toEqual(["agent_start"]);
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);

it.effect("fails a request issued after the transport has closed, instead of hanging", () =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const transport = yield* make();
        // Drive the transport to closed: pi's stdout ends.
        const evFiber = yield* Effect.forkChild(Stream.runCollect(transport.events));
        yield* Queue.end(fake.stdoutRaw);
        yield* Fiber.join(evFiber); // events completed → close has propagated

        // A request now must fail fast with the terminal PiTransportError, not
        // register a deferred that nothing will ever complete.
        const exit = yield* Effect.exit(transport.request({ type: "get_state" }));
        expect(Exit.isFailure(exit)).toBe(true);
        const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
        if (Option.isSome(error) && error.value._tag === "PiTransportError") {
          expect(error.value.reason).toBe("closed");
        } else {
          throw new Error("expected a PiTransportError failure");
        }
      }),
    ).pipe(Effect.provide(fake.layer));
  }),
);
