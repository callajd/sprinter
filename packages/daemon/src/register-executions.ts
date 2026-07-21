/**
 * `layerRegisterExecutions` — the daemon-side wiring that makes a DISPATCHED execution
 * reachable over the contract's execution channel (CVG task CE4.1).
 *
 * The convergence gap it closes: the four execution-channel handlers
 * (`executionEvents` / `executionSend` / `interrupt` / `answerUiRequest`) resolve a live
 * {@link ExecutionHandle} through the {@link ExecutionRegistry} PORT, but NOTHING in the
 * daemon ever CALLED {@link ExecutionRegistry.register} — the `JobRunner` dispatches
 * through the {@link ExecutionRunner} port and keeps the handle to itself, so every
 * execution-channel call returned `ExecutionNotFound`. Each piece (the runner, the
 * registry, the handlers) was landed and unit-tested against a fake, but the wire
 * between them was missing — exactly the kind of seam an end-to-end acceptance task
 * surfaces.
 *
 * This is a pure {@link Layer} DECORATOR over the `ExecutionRunner` port (INV-EFFECT-DI):
 * it wraps an inner adapter's `run` so that, the moment an execution starts, the returned
 * handle is registered in the {@link ExecutionRegistry} under the SAME id the
 * {@link JobRunner} persists ({@link executionIdFor}). Registration is `Scope`-managed by
 * the runner's own `run` scope (the handle's lifetime), so the entry is removed when
 * the dispatch scope closes — a settled execution is no longer resolvable, matching the
 * registry's own lifetime contract. No consumer above the `ExecutionRunner` tag
 * changes; selecting "register vs. not" is a `Layer` substitution and nothing else.
 *
 * It keys the registry on {@link executionIdFor}(job) — NOT on any id the handle might
 * carry — because that is exactly the id `JobRunner.dispatch` persists into the
 * durable `Execution`/`Job` rows, hence the id the app reads from `snapshot`/`events`
 * and hands back on `executionEvents`/`executionSend`/`interrupt`. Deriving it identically
 * keeps the registry key and the durable key from drifting.
 */
import { Effect, Layer } from "effect";
import { ExecutionRunner, executionIdFor } from "@sprinter/job";
import { ExecutionRegistry } from "./execution-registry.ts";

/**
 * Decorate an inner {@link ExecutionRunner} adapter so every `run` also REGISTERS
 * the started {@link ExecutionHandle} in the {@link ExecutionRegistry} (under
 * {@link executionIdFor}(job)) for the lifetime of the run's scope. The inner adapter
 * is provided beneath, so this outputs the `ExecutionRunner` port and adds only the
 * {@link ExecutionRegistry} requirement (plus whatever the inner adapter needs).
 *
 * `Effect.tap` runs the registration on the handle within the caller's `run` scope
 * — the SAME scope that owns the underlying execution — so the registry entry shares
 * the execution's lifetime (`ExecutionRegistry.register` is `acquireRelease`-scoped) and
 * is torn down with it. The wrapped `run` is otherwise transparent: same handle,
 * same `ExecutionRunnerError`, same `Scope` requirement as the port.
 */
export const layerRegisterExecutions = <E, R>(
  inner: Layer.Layer<ExecutionRunner, E, R>,
): Layer.Layer<ExecutionRunner, E, ExecutionRegistry | R> =>
  Layer.effect(
    ExecutionRunner,
    Effect.gen(function* () {
      const registry = yield* ExecutionRegistry;
      const base = yield* ExecutionRunner;
      return ExecutionRunner.of({
        run: (job) =>
          base
            .run(job)
            .pipe(
              Effect.tap((handle) =>
                executionIdFor(job).pipe(Effect.flatMap((id) => registry.register(id, handle))),
              ),
            ),
      });
    }),
  ).pipe(Layer.provide(inner));
