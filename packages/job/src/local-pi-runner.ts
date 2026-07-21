/**
 * `layerLocalPi` — the concrete `LocalPi` adapter for the {@link ExecutionRunner}
 * port (CVG task CE1.1). It is the ONE production `Layer` behind the port: it
 * starts a real `pi` execution via `@sprinter/runner`'s {@link makeExecution} and hands
 * back the neutral {@link ExecutionHandle} the {@link JobRunner} dispatches through.
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
 * execution to its first `ExecutionIdle` (Pi's `agent_settled`) and truncates the
 * handle there. The returned handle's `events` END at that settle (so the fold
 * terminates) and its `result` resolves — `Completed` on a clean settle/close,
 * `Failed` on a transport teardown. The underlying `pi` process is still
 * `Scope`-managed by the caller: it is torn down when the dispatch scope closes,
 * so nothing is orphaned.
 */
import { Cause, Deferred, Effect, Layer, Option, Pull, Ref, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import type { AgentContent, Job, ExecutionEvent } from "@sprinter/domain";
import {
  type ExecutionHandle,
  type ExecutionResult,
  makeExecution,
  type PiProcessConfig,
  type PiTransportError,
} from "@sprinter/runner";
import { ExecutionRunner, ExecutionRunnerError } from "./execution-runner.ts";
import { PiSpawnRouter } from "./spawn-router.ts";

/**
 * The agent revision this adapter runs — the `pi` CLI as Sprinter dispatches it
 * (DE2.2 / D2). Every execution it starts is attributed to THIS content, and the
 * registry revision's id is DERIVED from it (`agent-registration.ts`), so changing any
 * field here lands as a new revision rather than colliding with the old one.
 *
 * Three fields are recorded honestly rather than invented:
 *
 * - `model` — the model selection is `pi`'s OWN configuration. This adapter spawns
 *   `pi --mode rpc` and the wire it consumes reports the resolved model only through a
 *   `get_state` response the {@link ExecutionHandle} does not surface, so what Sprinter
 *   can truthfully say today is "whatever `pi` resolves for itself". When the runner
 *   learns the concrete model, this content changes — and because identity is
 *   content-derived, that is automatically a NEW registry revision, with every past
 *   execution still resolving to the one it actually ran under.
 * - `tools` — likewise `pi`'s own configuration, so Sprinter declares NO tool
 *   allow-list here rather than asserting one it does not enforce. An empty list is
 *   "not declared by Sprinter", and it is not a claim that the agent has no tools.
 * - `version` — a PLACEHOLDER, and a load-bearing one, so it is called out rather than
 *   left to look settled. It does not track the `pi` binary: this adapter spawns
 *   whatever `pi` the spawn router resolves and never asks it what it is, so upgrading
 *   the binary leaves this content byte-identical and re-derives the SAME registry id.
 *   The registry then records that "the same agent" ran both before and after the
 *   upgrade, which is the OPPOSITE of the guarantee `Execution.agentId` exists to give
 *   ("a historical execution always resolves to the agent that actually ran it" —
 *   `sqlite.ts`). The key and the content-derived id are both working correctly; what
 *   is wrong is the CONTENT they are computed over.
 *
 *   For it to track the binary, the adapter needs the resolved version at spawn time —
 *   i.e. the `pi` version has to reach this module as DATA (a `--version` probe through
 *   {@link PiSpawnRouter}, or the `get_state` response the {@link ExecutionHandle} does
 *   not surface today, which is the same missing capability the `model` caveat above
 *   names). At that point `LOCAL_PI_AGENT` stops being a constant and becomes a
 *   per-spawn value; nothing else changes, because identity is already derived from
 *   content, so every upgrade lands as a new revision automatically and every past
 *   execution keeps resolving to the revision it actually ran.
 */
export const LOCAL_PI_AGENT: AgentContent = {
  name: "pi",
  model: "pi-cli-default",
  version: "1.0.0",
  tools: [],
};

/** The `ExecutionIdle` (Pi `agent_settled`) event that ends a one-shot dispatch. */
const isExecutionIdle = (event: ExecutionEvent): boolean => event._tag === "ExecutionIdle";

/** The `TurnStarted` (Pi `turn_start`) event: the agent has begun a prompted turn. */
const isTurnStarted = (event: ExecutionEvent): boolean => event._tag === "TurnStarted";

/**
 * The neutral terminal outcome of a transport-error teardown: the transport's own
 * neutral `detail` when present, else the pretty-printed cause. Mirrors the
 * runner's own `failedResult` so a truncated execution reports failure identically.
 */
const failedResult = (cause: Cause.Cause<PiTransportError>): ExecutionResult => {
  const failure = Cause.findErrorOption(cause);
  return {
    _tag: "Failed",
    error: Option.isSome(failure) ? failure.value.detail : Cause.pretty(cause),
  };
};

/**
 * Wrap a live {@link ExecutionHandle} into a ONE-SHOT handle that honours the
 * terminal-result contract. The wrapped `events` stream ends at the first
 * `ExecutionIdle` (inclusive), so the {@link JobRunner}'s fold terminates instead of
 * hanging on an idle-but-alive `pi`. A scoped watcher drains that same truncated
 * stream to resolve the terminal `result`: `Completed` when it settles/ends
 * cleanly, `Failed` when the transport tears down. `send` / `interrupt` /
 * `answerUi` / `pid` delegate to the underlying handle unchanged.
 *
 * **Pre-prompt settle gating (CE1.1-F2 cold review / CE1.1-F1).** A real
 * `pi --mode rpc` idles-on-startup: it emits an `agent_settled` (→ `ExecutionIdle`)
 * BEFORE any prompt is sent. Truncating the one-shot on THAT settle would end the
 * dispatch with zero work and report `Completed` — the load-bearing failure of the
 * dispatch loop. So the settle-watcher is GATED: an `ExecutionIdle` only arms
 * truncation once a turn has begun (the {@link JobRunner}'s prompt drives Pi's
 * `turn_start` → `TurnStarted`). Every pre-turn (pre-prompt) settle is DROPPED — it
 * neither ends the stream nor resolves `result` — so a truncating settle always
 * follows real agent work.
 *
 * **Subscribe-before-emit (CE1.1-F1 cold review).** The gate is only sound if the
 * watcher is LISTENING before `TurnStarted` can flow: the execution's `events` is a
 * bounded SLIDING PubSub (`@sprinter/runner`), so under a real-`pi` burst a
 * `TurnStarted` could slide out of the replay window before a late subscriber
 * attaches — leaving the gate un-armed so the watcher never truncates and dispatch
 * HANGS. So the watcher's subscription is established EAGERLY here (via
 * {@link Stream.toPull}) before `run` yields the handle — hence before the
 * `JobRunner` sends the prompt that drives `TurnStarted` (`dispatch` calls `send`
 * only after `run` returns). The subscription is armed before any event can flow.
 */
const oneShot = (handle: ExecutionHandle): Effect.Effect<ExecutionHandle, never, Scope> =>
  Effect.gen(function* () {
    const settled = yield* Deferred.make<ExecutionResult>();
    // A fresh gated view of the execution's events. Each call gets its OWN
    // `turnStarted` ref via `Stream.unwrap` — a SHARED ref would race: the watcher,
    // draining ahead, could flip the ref from a later `turn_start` while the consumer
    // is still on the pre-prompt settle, so the consumer would wrongly keep it.
    // Per-subscription state keeps the gate correct regardless of interleaving. A
    // `ExecutionIdle` truncates only AFTER `TurnStarted` in THAT subscription; every
    // pre-turn settle is dropped.
    const gated = (): Stream.Stream<ExecutionEvent, PiTransportError> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const turnStarted = yield* Ref.make(false);
          return handle.events.pipe(
            Stream.filterEffect((event) =>
              isTurnStarted(event)
                ? Ref.set(turnStarted, true).pipe(Effect.as(true))
                : isExecutionIdle(event)
                  ? Ref.get(turnStarted)
                  : Effect.succeed(true),
            ),
            Stream.takeUntil(isExecutionIdle),
          );
        }),
      );
    // Arm the terminal watcher's subscription EAGERLY, before returning the handle:
    // `Stream.toPull` opens the channel (subscribing to the execution's PubSub) as it
    // runs, so the gate is listening before any event can flow (subscribe-before-emit).
    const pull = yield* Stream.toPull(gated());
    // Drain that pre-armed subscription to its terminal: a clean stream end (the gated
    // `ExecutionIdle`, or `pi` closing) resolves `Completed`; a transport teardown
    // resolves `Failed`. `Pull.catchDone` turns the end-of-stream halt into the clean
    // outcome, leaving only a real `PiTransportError` cause for the failure branch.
    const watch = Effect.forever(pull).pipe(
      Pull.catchDone(() => Deferred.succeed(settled, { _tag: "Completed" as const })),
      Effect.catchCause((cause: Cause.Cause<PiTransportError>) =>
        Deferred.succeed(settled, failedResult(cause)),
      ),
    );
    yield* Effect.forkScoped(watch);
    return { ...handle, events: gated(), result: Deferred.await(settled) };
  });

/**
 * Start a real `pi` execution for a {@link Job} in the Job's OWN spawn config and
 * expose it as a one-shot {@link ExecutionHandle}. The `config` (notably `cwd`) is
 * resolved per-Job by the {@link PiSpawnRouter} at the composition root, so `pi`
 * runs in the Job's worktree, not the daemon's cwd (CE1.1-F2). A `makeExecution`
 * spawn failure (a `PlatformError`) is translated at the boundary into the owned
 * {@link ExecutionRunnerError}, so no consumer depends on a concrete runtime's
 * error shape (INV-PORT). The prompt is NOT driven here — the {@link JobRunner}
 * owns deriving and sending the Issue-content prompt; this adapter owns starting
 * the execution and the terminal-result contract.
 */
const run = (
  job: Job,
  config: PiProcessConfig,
): Effect.Effect<
  ExecutionHandle,
  ExecutionRunnerError,
  ChildProcessSpawner.ChildProcessSpawner | Scope
> =>
  makeExecution(config).pipe(
    Effect.mapError(
      (error) =>
        new ExecutionRunnerError({
          operation: "run",
          detail: `failed to start pi execution for job ${job.id}: ${error.message}`,
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
      agent: LOCAL_PI_AGENT,
      run: (job) =>
        router.configFor(job).pipe(
          Effect.flatMap((config) => run(job, config)),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
    });
  }),
);
