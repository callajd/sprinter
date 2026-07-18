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
 */
import { Context, Effect, HashMap, Layer, Option, Ref } from "effect";
import type { Scope } from "effect/Scope";
import { SessionNotFound } from "@sprinter/contract";
import type { SessionId } from "@sprinter/domain";
import type { SessionHandle } from "@sprinter/runner";

/**
 * The live-session registry PORT (INV-NAMING, `sprinter/<area>/<Name>`). The
 * session-channel handlers depend on THIS service, never on a concrete map or a
 * `pi` process; {@link layer} provides the backing (INV-PORT).
 */
export class SessionRegistry extends Context.Service<
  SessionRegistry,
  {
    /**
     * Resolve the live {@link SessionHandle} for a `sessionId`, or fail with the
     * contract's {@link SessionNotFound} when no session is registered under it.
     */
    readonly get: (id: SessionId) => Effect.Effect<SessionHandle, SessionNotFound>;
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
      register: (id, handle) =>
        Effect.acquireRelease(Ref.update(handles, HashMap.set(id, handle)), () =>
          Ref.update(handles, HashMap.remove(id)),
        ).pipe(Effect.asVoid),
    });
  }),
);
