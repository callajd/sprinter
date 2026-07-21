/**
 * `ExecutionRegistry` — the daemon's map from a live `executionId` to its owned
 * {@link ExecutionHandle} (Track A, task AE4.2). It is the PORT the four
 * execution-channel RPC handlers (`executionEvents` / `executionSend` / `interrupt` /
 * `answerUiRequest`) resolve against, so all four address the SAME live execution
 * for a given id (INV-PORT). A miss is the contract's own {@link ExecutionNotFound}.
 *
 * The registry traffics ONLY in owned neutral types — the branded
 * `@sprinter/domain` `ExecutionId` key and the `@sprinter/runner` {@link ExecutionHandle}
 * value, whose entire surface (`events` / `send` / `interrupt` / `answerUi`) is
 * expressed in `ExecutionEvent` / `ExecutionInput` / `UiResponse`. No Pi wire type
 * crosses this boundary (INV-BOUNDARY): the handle is the runner's neutral façade,
 * imported as a type only, and nothing here reaches a concrete `pi` process.
 *
 * Registration is **Scope-managed**: {@link ExecutionRegistry.register} adds the
 * entry via `Effect.acquireRelease`, so the entry is removed automatically when
 * the registering scope closes — the same scope that owns the `ExecutionHandle`'s
 * `pi` process (the handle is itself `Scope`-managed). An execution and its registry
 * entry therefore share one lifetime; a torn-down execution cannot be resolved by a
 * later handler call.
 *
 * ## Ordering guarantee — the register-after-dispatch window ({@link resolve})
 *
 * `JobRunner.dispatch` persists the `running`/`starting` Job/execution rows — fanning
 * out the `JobChanged`/`ExecutionChanged` deltas the app reacts to — BEFORE
 * `ExecutionRunner.run(job)` returns the live {@link ExecutionHandle} that
 * {@link register}s it (the `pi` spawn + handshake happen inside `run`). So a
 * `running` delta does NOT yet imply the execution is registered: a client that reacts
 * to it by opening the execution channel can arrive during the not-yet-registered
 * window and would otherwise see a spurious {@link ExecutionNotFound}.
 *
 * {@link resolve} bridges that window so no client needs a retry: on a miss it AWAITS
 * the execution's registration (event-driven — completed the instant {@link register}
 * runs, so it is clock-independent and never busy-polls), bounded by the
 * layer's resolve timeout (default {@link EXECUTION_RESOLVE_TIMEOUT}). An execution that is
 * mid-`run` therefore resolves once its handle lands; a genuinely-absent execution still
 * fails with `ExecutionNotFound` after the bound. The immediate {@link get} is retained
 * for callers that want a strict present-or-absent read (no wait).
 *
 * ## Who decides wait-vs-fail-fast — the durable-state gate lives in the CALLER
 *
 * `resolve` is INTENTIONALLY unconditional: on a miss it always waits out the bound.
 * A registry entry only lives for its execution's run scope, so a SETTLED
 * (completed/failed/interrupted) execution — or one that never existed — is absent from
 * the map, and waiting the full bound on it would be a spurious multi-second stall
 * (the Inspector opens channels for SETTLED jobs by design, BE4.1). So the
 * execution-channel handlers gate the choice on DURABLE state (`StateStore`): an execution
 * whose durable `Execution` row is still NON-TERMINAL (`starting`/`active`/`idle` — genuinely
 * mid-dispatch) resolves through {@link resolve} (bridging the window); an execution whose
 * row is TERMINAL or ABSENT resolves through {@link get} (fail fast, no wait). The
 * registry stays a pure map + wait primitive; the durable read that distinguishes the
 * two cases belongs to the caller that already holds the `StateStore`.
 */
import { Context, Deferred, Duration, Effect, HashMap, Layer, Option, Ref } from "effect";
import type { Scope } from "effect/Scope";
import { ExecutionNotFound } from "@sprinter/contract";
import type { ExecutionId } from "@sprinter/domain";
import type { ExecutionHandle } from "@sprinter/runner";

/**
 * The DEFAULT hard upper bound {@link ExecutionRegistry.resolve} waits for a
 * not-yet-registered execution before giving up with {@link ExecutionNotFound}. It only
 * needs to cover the register-after-dispatch window (a `pi` spawn + RPC handshake), so
 * a few seconds is generous; a genuinely-mid-dispatch execution that never registers
 * fails after exactly this bound (never hangs).
 *
 * ## The assumption this value must hold — and the operational knob
 *
 * This bound MUST comfortably exceed the real `pi` spawn + RPC-handshake window: if a
 * cold-start `pi` takes longer to register its handle than the bound, `resolve` gives
 * up early and the execution channel returns a spurious {@link ExecutionNotFound} even
 * though the execution is genuinely mid-dispatch. It is NOT hardcoded at the resolve
 * site: it is threaded through {@link DaemonConfig.executionResolveTimeout} (defaulting
 * to this constant, overridable via `SPRINTER_EXECUTION_RESOLVE_TIMEOUT_MS`) and passed
 * to {@link layerWith}, so an operator whose `pi` cold-start exceeds it can raise the
 * bound WITHOUT a code change. Exported so a test can advance a `TestClock` precisely
 * past the default.
 */
export const EXECUTION_RESOLVE_TIMEOUT: Duration.Duration = Duration.seconds(5);

/**
 * The live-execution registry PORT (INV-NAMING, `sprinter/<area>/<Name>`). The
 * execution-channel handlers depend on THIS service, never on a concrete map or a
 * `pi` process; {@link layer} provides the backing (INV-PORT).
 */
export class ExecutionRegistry extends Context.Service<
  ExecutionRegistry,
  {
    /**
     * Resolve the live {@link ExecutionHandle} for a `executionId` IMMEDIATELY, or fail
     * with the contract's {@link ExecutionNotFound} when no execution is registered under
     * it right now (a strict present-or-absent read; never waits). Prefer
     * {@link resolve} on the execution channel, where a not-yet-registered execution may
     * still be mid-dispatch.
     */
    readonly get: (id: ExecutionId) => Effect.Effect<ExecutionHandle, ExecutionNotFound>;
    /**
     * Resolve the live {@link ExecutionHandle} for a `executionId`, tolerating the
     * register-after-dispatch window: on a miss it AWAITS the execution's
     * {@link register} (event-driven, so it completes the instant registration runs —
     * clock-independent, no busy-poll) up to the layer's resolve timeout, then fails
     * with the contract's {@link ExecutionNotFound} if still absent. The execution-channel
     * handlers route here ONLY for an execution their durable-state gate has confirmed is
     * genuinely mid-dispatch (durable row NON-TERMINAL), so a client reacting to a
     * `running` delta never needs to retry a spurious `ExecutionNotFound` — while a
     * SETTLED or never-existed execution is sent to {@link get} instead and fails fast
     * (see the module "durable-state gate" note).
     */
    readonly resolve: (id: ExecutionId) => Effect.Effect<ExecutionHandle, ExecutionNotFound>;
    /**
     * Register a live {@link ExecutionHandle} under its `executionId` for the lifetime
     * of the calling scope: the entry is added now and removed by a scope
     * finalizer, so it shares the handle's `Scope`-managed lifetime (an execution that
     * tears down cannot be resolved afterwards).
     */
    readonly register: (
      id: ExecutionId,
      handle: ExecutionHandle,
    ) => Effect.Effect<void, never, Scope>;
  }
>()("sprinter/daemon/ExecutionRegistry") {}

/**
 * The {@link ExecutionRegistry} implementation over a `Ref<HashMap>` of live handles
 * (`Layer.effect` + `Service.of`, per conventions), with a CONFIGURABLE resolve bound.
 * Registration is tied to the caller's scope via `Effect.acquireRelease`, so no handle
 * outlives its execution's scope in the map. `resolveTimeout` is the bound
 * {@link ExecutionRegistry.resolve} waits out for a not-yet-registered execution — the
 * composition root passes {@link DaemonConfig.executionResolveTimeout} so it is an
 * operational knob (see {@link EXECUTION_RESOLVE_TIMEOUT}); {@link layer} pins the default.
 */
export const layerWith = (resolveTimeout: Duration.Duration): Layer.Layer<ExecutionRegistry> =>
  Layer.effect(
    ExecutionRegistry,
    Effect.gen(function* () {
      const handles = yield* Ref.make(HashMap.empty<ExecutionId, ExecutionHandle>());
      // Pending {@link resolve} waiters per id: each awaits a Deferred that {@link
      // register} completes with the freshly-registered handle. An array (not a set)
      // holds the rare case of several handlers resolving one id concurrently.
      const waiters = yield* Ref.make(
        HashMap.empty<ExecutionId, ReadonlyArray<Deferred.Deferred<ExecutionHandle>>>(),
      );
      // A typed empty parked-waiter list, so `getOrElse` fallbacks stay `ReadonlyArray`
      // without a cast (INV-NOCAST).
      const noWaiters: ReadonlyArray<Deferred.Deferred<ExecutionHandle>> = [];

      // Complete + clear every waiter parked on `id` with the just-registered handle.
      const notifyWaiters = (id: ExecutionId, handle: ExecutionHandle): Effect.Effect<void> =>
        Ref.modify(waiters, (map) => [
          Option.getOrElse(HashMap.get(map, id), () => noWaiters),
          HashMap.remove(map, id),
        ]).pipe(
          Effect.flatMap((pending) =>
            Effect.forEach(pending, (deferred) => Deferred.succeed(deferred, handle), {
              discard: true,
            }),
          ),
        );

      // Drop one waiter (on its resolve settling — success, timeout, or interrupt), so
      // a settled resolver never leaves a dangling Deferred parked under its id.
      const dropWaiter = (
        id: ExecutionId,
        deferred: Deferred.Deferred<ExecutionHandle>,
      ): Effect.Effect<void> =>
        Ref.update(waiters, (map) =>
          HashMap.modifyAt(map, id, (pending) => {
            const next = Option.getOrElse(pending, () => noWaiters).filter((d) => d !== deferred);
            return next.length === 0 ? Option.none() : Option.some(next);
          }),
        );

      return ExecutionRegistry.of({
        get: (id) =>
          Ref.get(handles).pipe(
            Effect.flatMap((map) =>
              Option.match(HashMap.get(map, id), {
                onNone: () => Effect.fail(new ExecutionNotFound({ id })),
                onSome: (handle) => Effect.succeed(handle),
              }),
            ),
          ),
        resolve: (id) =>
          Effect.gen(function* () {
            // Fast path: already registered.
            const present = HashMap.get(yield* Ref.get(handles), id);
            if (Option.isSome(present)) return present.value;

            // Miss: park a waiter, then RE-CHECK to close the lost-wakeup race — a
            // `register` that ran between the read above and enlisting the waiter would
            // have found no waiter to notify, so its handle is now in `handles` and we
            // take it directly (dropping the just-parked waiter).
            const deferred = yield* Deferred.make<ExecutionHandle>();
            yield* Ref.update(waiters, (map) =>
              HashMap.modifyAt(map, id, (pending) =>
                Option.some([...Option.getOrElse(pending, () => noWaiters), deferred]),
              ),
            );
            const recheck = HashMap.get(yield* Ref.get(handles), id);
            if (Option.isSome(recheck)) {
              yield* dropWaiter(id, deferred);
              return recheck.value;
            }

            // Await registration, bounded — a genuinely-absent execution fails after the
            // bound rather than hanging; the waiter is always dropped on settle.
            return yield* Deferred.await(deferred).pipe(
              Effect.timeoutOrElse({
                duration: resolveTimeout,
                orElse: () => Effect.fail(new ExecutionNotFound({ id })),
              }),
              Effect.ensuring(dropWaiter(id, deferred)),
            );
          }),
        register: (id, handle) =>
          Effect.acquireRelease(
            // Publish the handle, then wake any waiter parked by `resolve` on this id.
            Ref.update(handles, HashMap.set(id, handle)).pipe(
              Effect.andThen(notifyWaiters(id, handle)),
            ),
            () =>
              // Remove ONLY if this handle is still the one mapped: under execution-id
              // reuse (a re-dispatch registers a fresh handle under
              // the same id), a blind `remove(id)` on this handle's scope-close would
              // evict the live SUCCESSOR. Guard on identity so a superseded entry's
              // teardown never removes its replacement.
              Ref.update(handles, (map) =>
                Option.match(HashMap.get(map, id), {
                  onNone: () => map,
                  onSome: (current) => (current === handle ? HashMap.remove(map, id) : map),
                }),
              ),
          ).pipe(Effect.asVoid),
      });
    }),
  );

/**
 * The {@link ExecutionRegistry} backed by the DEFAULT {@link EXECUTION_RESOLVE_TIMEOUT} —
 * the convenient value most tests provide directly. Production wires the registry via
 * {@link layerWith}({@link DaemonConfig.executionResolveTimeout}) so the bound is an
 * operational knob rather than a hardcoded constant.
 */
export const layer: Layer.Layer<ExecutionRegistry> = layerWith(EXECUTION_RESOLVE_TIMEOUT);
