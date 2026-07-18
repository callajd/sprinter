/**
 * `PiTransport` — the process + wire-transport substrate for the `LocalPiRunner`
 * (Track A, task AE1.1).
 *
 * This is the lowest layer of the Pi `ExecutionRunner` adapter: it spawns
 * `pi --mode rpc` as a `Scope`-managed subprocess (killed on scope close, so no
 * orphaned `pi`), frames its stdio as NDJSON against the OWNED Pi wire schema
 * (`PiServerMessage` in / `PiClientMessage` out), and correlates each sent
 * command to its awaiting caller by the `id` field. The subprocess is reached
 * only through the `ChildProcessSpawner` port (INV-PORT); no caller depends on a
 * concrete `pi` process.
 *
 * This module deliberately traffics in Pi wire types (`PiServerMessage`,
 * `PiRpcCommand`, …) — it is the substrate INSIDE the runner package. Per
 * INV-BOUNDARY, those Pi types do NOT reach the package's public surface
 * (`index.ts`); the neutral `SessionHandle` + `SessionEvent` translation that
 * everything above the runner consumes lands in AE1.2 and is built ON this. The
 * correlation/lifecycle shape mirrors Pi's `rpc-process.ts` as a reference and
 * imports nothing from it (D12).
 */
import { Cause, Deferred, Effect, HashMap, Option, Queue, Ref, Schema, Stream } from "effect";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type { PlatformError } from "effect/PlatformError";
import type { Scope } from "effect/Scope";
import {
  type PiClientMessage,
  PiClientMessage as PiClientMessageSchema,
  type PiRpcCommand,
  type PiRpcResponse,
  type PiServerMessage,
  PiServerMessage as PiServerMessageSchema,
} from "@sprinter/domain/pi/wire";

/**
 * A Pi server message that is NOT a correlated RPC response — a streaming
 * session event, an extension UI request, or an extension error. These flow to
 * {@link PiTransport.events}; responses are consumed by the id-correlation
 * machinery instead.
 */
export type PiServerEvent = Exclude<PiServerMessage, PiRpcResponse>;

/**
 * Raised when `pi` answers a correlated command with an error response
 * (`success: false`). It fails the awaiting {@link PiTransport.request} caller.
 */
export class PiRpcError extends Schema.TaggedErrorClass<PiRpcError>()("PiRpcError", {
  /** The command that failed, echoed back by `pi`. */
  command: Schema.String,
  /** The human-readable error `pi` reported. */
  error: Schema.String,
}) {}

/**
 * Raised for a transport-level failure: the subprocess output ended while
 * requests were still outstanding (`closed`), or the NDJSON/schema decode of a
 * stdout line failed (`stream`). It fails the {@link PiTransport.events} stream
 * and every outstanding {@link PiTransport.request}.
 */
export class PiTransportError extends Schema.TaggedErrorClass<PiTransportError>()(
  "PiTransportError",
  {
    reason: Schema.Literals(["closed", "stream"]),
    detail: Schema.String,
  },
) {}

/**
 * Configuration for the spawned `pi` process. Every field is optional; the
 * defaults spawn `pi --mode rpc` inheriting the daemon's environment.
 */
export interface PiProcessConfig {
  /** Executable to spawn. Defaults to `"pi"`. */
  readonly command?: string;
  /** Arguments passed to the executable. Defaults to `["--mode", "rpc"]`. */
  readonly args?: ReadonlyArray<string>;
  /** Working directory for the process. Defaults to the parent's cwd. */
  readonly cwd?: string;
  /** Extra environment variables, merged over the inherited environment. */
  readonly env?: Readonly<Record<string, string>>;
}

/**
 * A connected, `Scope`-managed `pi --mode rpc` process with its NDJSON wire
 * bridge. Obtained from {@link make}; single-consumer for {@link events}.
 */
export interface PiTransport {
  /** The operating-system process id of the spawned `pi`. */
  readonly pid: ChildProcessSpawner.ProcessId;
  /**
   * The stream of non-response server messages (session events, UI requests,
   * extension errors), in emission order. Fails with {@link PiTransportError}
   * if a stdout line fails to decode; ends when `pi`'s stdout closes.
   */
  readonly events: Stream.Stream<PiServerEvent, PiTransportError>;
  /**
   * Send a command and await the correlated response, matched back by `id` (the
   * transport assigns a fresh id, overriding any on the command). Fails with
   * {@link PiRpcError} on an error response, or {@link PiTransportError} if the
   * transport closes before the response arrives.
   */
  readonly request: (
    command: PiRpcCommand,
  ) => Effect.Effect<PiRpcResponse, PiRpcError | PiTransportError>;
  /**
   * Send a client message fire-and-forget, without awaiting a response — used
   * for extension UI responses and any command whose ack is not needed.
   */
  readonly send: (message: PiClientMessage) => Effect.Effect<void>;
}

/** A caller awaiting the response correlated to a sent command. */
type Pending = Deferred.Deferred<PiRpcResponse, PiRpcError | PiTransportError>;

/** Prefix for transport-assigned correlation ids. */
const ID_PREFIX = "rpc-";

/**
 * Spawn `pi --mode rpc` and wire its NDJSON stdio to the owned Pi schema. The
 * process is bound to the calling `Scope`: closing the scope kills it and
 * interrupts the stdio pumps. Requires a {@link ChildProcessSpawner} adapter
 * (e.g. `BunServices.layer`) — the OS process is never depended on directly.
 */
export const make = (
  config?: PiProcessConfig,
): Effect.Effect<PiTransport, PlatformError, ChildProcessSpawner.ChildProcessSpawner | Scope> =>
  Effect.gen(function* () {
    // Both queues are unbounded by deliberate choice. Bounding `eventsQueue`
    // would apply backpressure onto the single stdout pump — stalling response
    // correlation too (head-of-line blocking), since responses and events share
    // that pump. The safe cap belongs at the consumer AE1.2 (#19) layers on
    // `events`, which must consume promptly; revisit the policy there.
    const outbound = yield* Queue.make<PiClientMessage>();
    const eventsQueue = yield* Queue.make<PiServerEvent, PiTransportError | Cause.Done>();
    const pending = yield* Ref.make(HashMap.empty<string, Pending>());
    const counter = yield* Ref.make(0);
    // Latched once the stdout pump terminates: no further response can arrive, so
    // new requests must fail fast instead of awaiting a deferred nothing completes.
    const closed = yield* Ref.make<Option.Option<PiTransportError>>(Option.none());

    // ── Spawn, Scope-managed ────────────────────────────────────────────────
    const command = ChildProcess.make(
      config?.command ?? "pi",
      config?.args ?? ["--mode", "rpc"],
      buildOptions(config),
    );
    const handle = yield* command;

    // ── stdin: PiClientMessage → NDJSON bytes → child stdin ──────────────────
    const outboundBytes = Stream.fromQueue(outbound).pipe(
      Stream.pipeThroughChannel(Ndjson.encodeSchema(PiClientMessageSchema)()),
      // Encoding our own well-typed messages cannot fail at runtime; a failure
      // here is a programming defect, not recoverable input.
      Stream.orDie,
    );
    yield* Effect.forkScoped(Stream.run(outboundBytes, handle.stdin));

    // ── correlation + event routing ─────────────────────────────────────────
    const complete = (response: PiRpcResponse): Effect.Effect<void> =>
      Effect.gen(function* () {
        const id = response.id;
        if (id === undefined) return;
        const entry = HashMap.get(yield* Ref.get(pending), id);
        if (Option.isNone(entry)) return;
        yield* Ref.update(pending, HashMap.remove(id));
        yield* response.success
          ? Deferred.succeed(entry.value, response)
          : Deferred.fail(
              entry.value,
              new PiRpcError({ command: response.command, error: response.error }),
            );
      });

    const route = (message: PiServerMessage): Effect.Effect<void> =>
      message.type === "response"
        ? complete(message)
        : Effect.asVoid(Queue.offer(eventsQueue, message));

    const closeTransport = (error: Option.Option<PiTransportError>): Effect.Effect<void> =>
      Effect.gen(function* () {
        const failure = Option.getOrElse(
          error,
          () => new PiTransportError({ reason: "closed", detail: "pi process output ended" }),
        );
        // Latch closed BEFORE draining `pending`, so a `request` that registers
        // concurrently either is drained here or observes `closed` on its re-check
        // and fails itself — no deferred is left un-completed.
        yield* Ref.set(closed, Option.some(failure));
        const outstanding = yield* Ref.getAndSet(pending, HashMap.empty<string, Pending>());
        yield* Effect.forEach(
          HashMap.values(outstanding),
          (deferred) => Deferred.fail(deferred, failure),
          {
            discard: true,
          },
        );
        yield* Option.match(error, {
          onNone: () => Queue.end(eventsQueue),
          onSome: (transportError) => Queue.fail(eventsQueue, transportError),
        });
      });

    // ── stdout: child stdout → NDJSON → PiServerMessage → route ──────────────
    const inbound = handle.stdout.pipe(
      Stream.pipeThroughChannel(
        Ndjson.decodeSchema(PiServerMessageSchema)({ ignoreEmptyLines: true }),
      ),
    );
    const pump = Effect.matchCauseEffect(Stream.runForEach(inbound, route), {
      onFailure: (cause) =>
        closeTransport(
          Option.some(new PiTransportError({ reason: "stream", detail: Cause.pretty(cause) })),
        ),
      onSuccess: () => closeTransport(Option.none()),
    });
    yield* Effect.forkScoped(pump);

    const request = (
      command_: PiRpcCommand,
    ): Effect.Effect<PiRpcResponse, PiRpcError | PiTransportError> =>
      Effect.gen(function* () {
        const alreadyClosed = yield* Ref.get(closed);
        if (Option.isSome(alreadyClosed)) return yield* Effect.fail(alreadyClosed.value);
        const seq = yield* Ref.updateAndGet(counter, (n) => n + 1);
        const id = `${ID_PREFIX}${seq}`;
        const deferred = yield* Deferred.make<PiRpcResponse, PiRpcError | PiTransportError>();
        yield* Ref.update(pending, HashMap.set(id, deferred));
        // Re-check: if the transport closed between the guard above and registering,
        // `closeTransport` may have already drained `pending` — fail fast rather than
        // await a deferred that nothing will complete.
        const closedNow = yield* Ref.get(closed);
        if (Option.isSome(closedNow)) {
          yield* Ref.update(pending, HashMap.remove(id));
          return yield* Effect.fail(closedNow.value);
        }
        yield* Queue.offer(outbound, { ...command_, id });
        // Drop our pending entry on ANY exit — including interruption/timeout before
        // the response arrives — so an abandoned caller never leaks its id.
        return yield* Deferred.await(deferred).pipe(
          Effect.onExit(() => Ref.update(pending, HashMap.remove(id))),
        );
      });

    const send = (message: PiClientMessage): Effect.Effect<void> =>
      Effect.asVoid(Queue.offer(outbound, message));

    return {
      pid: handle.pid,
      events: Stream.fromQueue(eventsQueue),
      request,
      send,
    };
  });

/** Build the `pi` process options, threading only the fields the config sets. */
const buildOptions = (config: PiProcessConfig | undefined): ChildProcess.CommandOptions => ({
  // Forward pi's diagnostics to the daemon's own stderr; an unread "pipe" would
  // risk blocking the child once its stderr buffer fills.
  stderr: "inherit",
  ...(config?.cwd !== undefined ? { cwd: config.cwd } : {}),
  // `extendEnv: true` MERGES `env` over the inherited environment (per the field
  // docs). Without it the spawner treats `env` as the child's ENTIRE environment,
  // dropping PATH/HOME and any credentials `pi` needs to start — so a config `env`
  // must always extend, never replace.
  ...(config?.env !== undefined ? { env: config.env, extendEnv: true } : {}),
});
