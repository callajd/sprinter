/**
 * `SessionRegistry` — the daemon's map from a live `sessionId` to its owned
 * {@link SessionHandle} (Track A, task AE4.2). It is the PORT the four
 * session-channel RPC handlers (`sessionEvents` / `sessionSend` / `interrupt` /
 * `answerUiRequest`) resolve against, so all four address the SAME live session
 * for a given id (INV-PORT). A miss is the contract's own {@link SessionNotFound}.
 *
 * The registry traffics ONLY in owned neutral types — the branded
 * `@sprinter/domain` `SessionId` key and the `@sprinter/runner` {@link SessionHandle}
 * value, whose entire surface (`events` / `send` / `interrupt` / `answerUi`) is
 * expressed in `SessionEvent` / `SessionInput` / `UiResponse`. No Pi wire type
 * crosses this boundary (INV-BOUNDARY): the handle is the runner's neutral façade,
 * imported as a type only, and nothing here reaches a concrete `pi` process.
 *
 * Registration is **Scope-managed**: {@link SessionRegistry.register} adds the
 * entry via `Effect.acquireRelease`, so the entry is removed automatically when
 * the registering scope closes — the same scope that owns the `SessionHandle`'s
 * `pi` process (the handle is itself `Scope`-managed). A session and its registry
 * entry therefore share one lifetime; a torn-down session cannot be resolved by a
 * later handler call.
 *
 * ## Ordering guarantee — the register-after-dispatch window ({@link resolve})
 *
 * `JobRunner.dispatch` persists the `running`/`starting` Job/session rows — fanning
 * out the `JobChanged`/`SessionChanged` deltas the app reacts to — BEFORE
 * `ExecutionRunner.run(job)` returns the live {@link SessionHandle} that
 * {@link register}s it (the `pi` spawn + handshake happen inside `run`). So a
 * `running` delta does NOT yet imply the session is registered: a client that reacts
 * to it by opening the session channel can arrive during the not-yet-registered
 * window and would otherwise see a spurious {@link SessionNotFound}.
 *
 * {@link resolve} bridges that window so no client needs a retry: on a miss it AWAITS
 * the session's registration (event-driven — completed the instant {@link register}
 * runs, so it is clock-independent and never busy-polls), bounded by
 * {@link SESSION_RESOLVE_TIMEOUT}. A session that is mid-`run` therefore resolves once
 * its handle lands; a genuinely-absent session still fails with `SessionNotFound`
 * after the bound. The immediate {@link get} is retained for callers that want a
 * strict present-or-absent read (no wait).
 */
import { Context, Deferred, Duration, Effect, HashMap, Layer, Option, Ref } from "effect";
import type { Scope } from "effect/Scope";
import { SessionNotFound } from "@sprinter/contract";
import type { SessionId } from "@sprinter/domain";
import type { SessionHandle } from "@sprinter/runner";

/**
 * The hard upper bound {@link SessionRegistry.resolve} waits for a not-yet-registered
 * session before giving up with {@link SessionNotFound}. It only needs to cover the
 * register-after-dispatch window (a `pi` spawn + handshake), so a few seconds is
 * generous; a genuinely-absent session fails after exactly this bound (never hangs).
 * Exported so a test can advance a `TestClock` precisely past it.
 */
export const SESSION_RESOLVE_TIMEOUT: Duration.Duration = Duration.seconds(5);

/**
 * The live-session registry PORT (INV-NAMING, `sprinter/<area>/<Name>`). The
 * session-channel handlers depend on THIS service, never on a concrete map or a
 * `pi` process; {@link layer} provides the backing (INV-PORT).
 */
export class SessionRegistry extends Context.Service<
  SessionRegistry,
  {
    /**
     * Resolve the live {@link SessionHandle} for a `sessionId` IMMEDIATELY, or fail
     * with the contract's {@link SessionNotFound} when no session is registered under
     * it right now (a strict present-or-absent read; never waits). Prefer
     * {@link resolve} on the session channel, where a not-yet-registered session may
     * still be mid-dispatch.
     */
    readonly get: (id: SessionId) => Effect.Effect<SessionHandle, SessionNotFound>;
    /**
     * Resolve the live {@link SessionHandle} for a `sessionId`, tolerating the
     * register-after-dispatch window: on a miss it AWAITS the session's
     * {@link register} (event-driven, so it completes the instant registration runs —
     * clock-independent, no busy-poll) up to {@link SESSION_RESOLVE_TIMEOUT}, then
     * fails with the contract's {@link SessionNotFound} if still absent. This is what
     * the session-channel handlers use so a client reacting to a `running` delta never
     * needs to retry a spurious `SessionNotFound` (see the module ordering guarantee).
     */
    readonly resolve: (id: SessionId) => Effect.Effect<SessionHandle, SessionNotFound>;
    /**
     * Register a live {@link SessionHandle} under its `sessionId` for the lifetime
     * of the calling scope: the entry is added now and removed by a scope
     * finalizer, so it shares the handle's `Scope`-managed lifetime (a session that
     * tears down cannot be resolved afterwards).
     */
    readonly register: (id: SessionId, handle: SessionHandle) => Effect.Effect<void, never, Scope>;
  }
>()("sprinter/daemon/SessionRegistry") {}

/**
 * The {@link SessionRegistry} implementation over a `Ref<HashMap>` of live
 * handles (`Layer.effect` + `Service.of`, per conventions). Registration is tied
 * to the caller's scope via `Effect.acquireRelease`, so no handle outlives its
 * session's scope in the map.
 */
export const layer: Layer.Layer<SessionRegistry> = Layer.effect(
  SessionRegistry,
  Effect.gen(function* () {
    const handles = yield* Ref.make(HashMap.empty<SessionId, SessionHandle>());
    // Pending {@link resolve} waiters per id: each awaits a Deferred that {@link
    // register} completes with the freshly-registered handle. An array (not a set)
    // holds the rare case of several handlers resolving one id concurrently.
    const waiters = yield* Ref.make(
      HashMap.empty<SessionId, ReadonlyArray<Deferred.Deferred<SessionHandle>>>(),
    );
    // A typed empty parked-waiter list, so `getOrElse` fallbacks stay `ReadonlyArray`
    // without a cast (INV-NOCAST).
    const noWaiters: ReadonlyArray<Deferred.Deferred<SessionHandle>> = [];

    // Complete + clear every waiter parked on `id` with the just-registered handle.
    const notifyWaiters = (id: SessionId, handle: SessionHandle): Effect.Effect<void> =>
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
      id: SessionId,
      deferred: Deferred.Deferred<SessionHandle>,
    ): Effect.Effect<void> =>
      Ref.update(waiters, (map) =>
        HashMap.modifyAt(map, id, (pending) => {
          const next = Option.getOrElse(pending, () => noWaiters).filter((d) => d !== deferred);
          return next.length === 0 ? Option.none() : Option.some(next);
        }),
      );

    return SessionRegistry.of({
      get: (id) =>
        Ref.get(handles).pipe(
          Effect.flatMap((map) =>
            Option.match(HashMap.get(map, id), {
              onNone: () => Effect.fail(new SessionNotFound({ id })),
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
          const deferred = yield* Deferred.make<SessionHandle>();
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

          // Await registration, bounded — a genuinely-absent session fails after the
          // bound rather than hanging; the waiter is always dropped on settle.
          return yield* Deferred.await(deferred).pipe(
            Effect.timeoutOrElse({
              duration: SESSION_RESOLVE_TIMEOUT,
              orElse: () => Effect.fail(new SessionNotFound({ id })),
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
            // Remove ONLY if this handle is still the one mapped: under session-id
            // reuse (1 Job = 1 session — a re-dispatch registers a fresh handle under
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
