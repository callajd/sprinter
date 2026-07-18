/**
 * `layerLocalPi` — the concrete `LocalPi` adapter for the {@link ExecutionRunner}
 * port (CVG task CE1.1). It is the ONE production `Layer` behind the port: it
 * starts a real `pi` session via `@sprinter/runner`'s {@link makeSession} and hands
 * back the neutral {@link SessionHandle} the {@link JobRunner} dispatches through.
 * It is a drop-in `Layer` substitution for the test fake — no consumer above the
 * `ExecutionRunner` tag changes, and none gains any Pi/localness knowledge
 * (INV-PORT / INV-EFFECT-DI). Its single external requirement is the
 * `ChildProcessSpawner` port (satisfied at the daemon edge, e.g. `BunServices`).
 *
 * **Terminal-result contract (the adapter's responsibility).** A raw
 * `pi --mode rpc` process stays alive and idle after a single turn — its `events`
 * stream never ends and its `result` never resolves, which would hang
 * `JobRunner.dispatch` (it folds `events` to completion, then awaits `result`).
 * This adapter makes the dispatch ONE-SHOT deterministically: it drives the
 * session to its first `SessionIdle` (Pi's `agent_settled`) and truncates the
 * handle there. The returned handle's `events` END at that settle (so the fold
 * terminates) and its `result` resolves — `Completed` on a clean settle/close,
 * `Failed` on a transport teardown. The underlying `pi` process is still
 * `Scope`-managed by the caller: it is torn down when the dispatch scope closes,
 * so nothing is orphaned.
 */
import { Cause, Deferred, Effect, Layer, Option, Ref, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import type { Job, SessionEvent } from "@sprinter/domain";
import {
  makeSession,
  type PiProcessConfig,
  type PiTransportError,
  type SessionHandle,
  type SessionResult,
} from "@sprinter/runner";
import { ExecutionRunner, ExecutionRunnerError } from "./execution-runner.ts";
import { PiSpawnRouter } from "./spawn-router.ts";

/** The `SessionIdle` (Pi `agent_settled`) event that ends a one-shot dispatch. */
const isSessionIdle = (event: SessionEvent): boolean => event._tag === "SessionIdle";

/** The `TurnStarted` (Pi `turn_start`) event: the agent has begun a prompted turn. */
const isTurnStarted = (event: SessionEvent): boolean => event._tag === "TurnStarted";

/**
 * The neutral terminal outcome of a transport-error teardown: the transport's own
 * neutral `detail` when present, else the pretty-printed cause. Mirrors the
 * runner's own `failedResult` so a truncated session reports failure identically.
 */
const failedResult = (cause: Cause.Cause<PiTransportError>): SessionResult => {
  const failure = Cause.findErrorOption(cause);
  return {
    _tag: "Failed",
    error: Option.isSome(failure) ? failure.value.detail : Cause.pretty(cause),
  };
};

/**
 * Wrap a live {@link SessionHandle} into a ONE-SHOT handle that honours the
 * terminal-result contract. The wrapped `events` stream ends at the first
 * `SessionIdle` (inclusive), so the {@link JobRunner}'s fold terminates instead of
 * hanging on an idle-but-alive `pi`. A scoped watcher drains that same truncated
 * stream to resolve the terminal `result`: `Completed` when it settles/ends
 * cleanly, `Failed` when the transport tears down. `send` / `interrupt` /
 * `answerUi` / `pid` delegate to the underlying handle unchanged.
 *
 * **Pre-prompt settle gating (CE1.1-F2 cold review / CE1.1-F1).** A real
 * `pi --mode rpc` idles-on-startup: it emits an `agent_settled` (→ `SessionIdle`)
 * BEFORE any prompt is sent. Truncating the one-shot on THAT settle would end the
 * dispatch with zero work and report `Completed` — the load-bearing failure of the
 * dispatch loop. So the settle-watcher is GATED: a `SessionIdle` only arms
 * truncation once a turn has begun (the {@link JobRunner}'s prompt drives Pi's
 * `turn_start` → `TurnStarted`). Every pre-turn (pre-prompt) settle is DROPPED — it
 * neither ends the stream nor resolves `result` — so a truncating settle always
 * follows real agent work.
 */
const oneShot = (handle: SessionHandle): Effect.Effect<SessionHandle, never, Scope> =>
  Effect.gen(function* () {
    const settled = yield* Deferred.make<SessionResult>();
    // The gated one-shot stream. Each subscription (the consumer below AND the
    // terminal watcher) gets its OWN `turnStarted` ref via `Stream.unwrap` — a
    // SHARED ref would race: the watcher, draining ahead, could flip the ref from a
    // later `turn_start` while the consumer is still on the pre-prompt settle, so the
    // consumer would wrongly keep it. Per-subscription state keeps the gate correct
    // regardless of interleaving. A `SessionIdle` truncates only AFTER `TurnStarted`
    // in THAT subscription; every pre-turn settle is dropped.
    const events = Stream.unwrap(
      Effect.gen(function* () {
        const turnStarted = yield* Ref.make(false);
        return handle.events.pipe(
          Stream.filterEffect((event) =>
            isTurnStarted(event)
              ? Ref.set(turnStarted, true).pipe(Effect.as(true))
              : isSessionIdle(event)
                ? Ref.get(turnStarted)
                : Effect.succeed(true),
          ),
          Stream.takeUntil(isSessionIdle),
        );
      }),
    );
    // A fresh subscription (the fan-out stream is multi-consumer) drives the
    // terminal outcome from the same truncated boundary the consumer observes.
    const watch = events.pipe(
      Stream.runDrain,
      Effect.matchCauseEffect({
        onFailure: (cause: Cause.Cause<PiTransportError>) =>
          Deferred.succeed(settled, failedResult(cause)),
        onSuccess: () => Deferred.succeed(settled, { _tag: "Completed" as const }),
      }),
    );
    yield* Effect.forkScoped(watch);
    return { ...handle, events, result: Deferred.await(settled) };
  });

/**
 * Start a real `pi` session for a {@link Job} in the Job's OWN spawn config and
 * expose it as a one-shot {@link SessionHandle}. The `config` (notably `cwd`) is
 * resolved per-Job by the {@link PiSpawnRouter} at the composition root, so `pi`
 * runs in the Job's worktree, not the daemon's cwd (CE1.1-F2). A `makeSession`
 * spawn failure (a `PlatformError`) is translated at the boundary into the owned
 * {@link ExecutionRunnerError}, so no consumer depends on a concrete runtime's
 * error shape (INV-PORT). The prompt is NOT driven here — the {@link JobRunner}
 * owns deriving and sending the Issue-content prompt; this adapter owns starting
 * the session and the terminal-result contract.
 */
const run = (
  job: Job,
  config: PiProcessConfig,
): Effect.Effect<
  SessionHandle,
  ExecutionRunnerError,
  ChildProcessSpawner.ChildProcessSpawner | Scope
> =>
  makeSession(config).pipe(
    Effect.mapError(
      (error) =>
        new ExecutionRunnerError({
          operation: "run",
          detail: `failed to start pi session for job ${job.id}: ${error.message}`,
        }),
    ),
    Effect.flatMap(oneShot),
  );

/**
 * The `LocalPi` adapter `Layer` for the {@link ExecutionRunner} port — a drop-in
 * substitution for the test fake (INV-EFFECT-DI). It captures the
 * `ChildProcessSpawner` and the {@link PiSpawnRouter} at construction and, on every
 * `run`, resolves the Job's spawn config through the router then re-provides the
 * spawner — so the service the port exposes requires only the caller's `Scope`
 * (matching the port signature). The daemon composition root provides a concrete
 * spawner and worktree router behind it.
 */
export const layerLocalPi: Layer.Layer<
  ExecutionRunner,
  never,
  ChildProcessSpawner.ChildProcessSpawner | PiSpawnRouter
> = Layer.effect(
  ExecutionRunner,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const router = yield* PiSpawnRouter;
    return ExecutionRunner.of({
      run: (job) =>
        router.configFor(job).pipe(
          Effect.flatMap((config) => run(job, config)),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
    });
  }),
);
