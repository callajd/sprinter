/**
 * `layerRegisterSessions` — the daemon-side wiring that makes a DISPATCHED session
 * reachable over the contract's session channel (CVG task CE4.1).
 *
 * The convergence gap it closes: the four session-channel handlers
 * (`sessionEvents` / `sessionSend` / `interrupt` / `answerUiRequest`) resolve a live
 * {@link SessionHandle} through the {@link SessionRegistry} PORT, but NOTHING in the
 * daemon ever CALLED {@link SessionRegistry.register} — the `JobRunner` dispatches
 * through the {@link ExecutionRunner} port and keeps the handle to itself, so every
 * session-channel call returned `SessionNotFound`. Each piece (the runner, the
 * registry, the handlers) was landed and unit-tested against a fake, but the wire
 * between them was missing — exactly the kind of seam an end-to-end acceptance task
 * surfaces.
 *
 * This is a pure {@link Layer} DECORATOR over the `ExecutionRunner` port (INV-EFFECT-DI):
 * it wraps an inner adapter's `run` so that, the moment a session starts, the returned
 * handle is registered in the {@link SessionRegistry} under the SAME id the
 * {@link JobRunner} persists ({@link sessionIdFor}). Registration is `Scope`-managed by
 * the runner's own `run` scope (the handle's lifetime), so the entry is removed when
 * the dispatch scope closes — a settled session is no longer resolvable, matching the
 * registry's own lifetime contract. No consumer above the `ExecutionRunner` tag
 * changes; selecting "register vs. not" is a `Layer` substitution and nothing else.
 *
 * It keys the registry on {@link sessionIdFor}(job) — NOT on any id the handle might
 * carry — because that is exactly the id `JobRunner.dispatch` persists into the
 * durable `Session`/`Job` rows, hence the id the app reads from `snapshot`/`events`
 * and hands back on `sessionEvents`/`sessionSend`/`interrupt`. Deriving it identically
 * keeps the registry key and the durable key from drifting.
 */
import { Effect, Layer } from "effect";
import { ExecutionRunner, sessionIdFor } from "@sprinter/job";
import { SessionRegistry } from "./session-registry.ts";

/**
 * Decorate an inner {@link ExecutionRunner} adapter so every `run` also REGISTERS
 * the started {@link SessionHandle} in the {@link SessionRegistry} (under
 * {@link sessionIdFor}(job)) for the lifetime of the run's scope. The inner adapter
 * is provided beneath, so this outputs the `ExecutionRunner` port and adds only the
 * {@link SessionRegistry} requirement (plus whatever the inner adapter needs).
 *
 * `Effect.tap` runs the registration on the handle within the caller's `run` scope
 * — the SAME scope that owns the underlying session — so the registry entry shares
 * the session's lifetime (`SessionRegistry.register` is `acquireRelease`-scoped) and
 * is torn down with it. The wrapped `run` is otherwise transparent: same handle,
 * same `ExecutionRunnerError`, same `Scope` requirement as the port.
 */
export const layerRegisterSessions = <E, R>(
  inner: Layer.Layer<ExecutionRunner, E, R>,
): Layer.Layer<ExecutionRunner, E, SessionRegistry | R> =>
  Layer.effect(
    ExecutionRunner,
    Effect.gen(function* () {
      const registry = yield* SessionRegistry;
      const base = yield* ExecutionRunner;
      return ExecutionRunner.of({
        run: (job) =>
          base
            .run(job)
            .pipe(
              Effect.tap((handle) =>
                sessionIdFor(job).pipe(Effect.flatMap((id) => registry.register(id, handle))),
              ),
            ),
      });
    }),
  ).pipe(Layer.provide(inner));
