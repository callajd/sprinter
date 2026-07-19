/**
 * `SessionRegistry` coverage (AE4.2) — the `sessionId → live SessionHandle` map
 * the session-channel handlers resolve against. Deterministic and offline: a
 * registered fake handle is resolved by `get`, a miss is the contract's
 * `SessionNotFound`, and Scope-managed registration removes the entry when the
 * registering scope closes (INV-PORT / INV-BOUNDARY — only owned neutral types
 * cross this surface).
 */
import { it } from "@effect/vitest";
import { Duration, Effect, Exit, Fiber, Schema, Scope, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { SessionNotFound } from "@sprinter/contract";
import { SessionId } from "@sprinter/domain";
import type { SessionHandle } from "@sprinter/runner";
import { layer, layerWith, SESSION_RESOLVE_TIMEOUT, SessionRegistry } from "./session-registry.ts";

const sessionId = Schema.decodeUnknownSync(SessionId)("ses-1");

/** A minimal fake {@link SessionHandle}: an empty event stream; inert verbs. */
const makeHandle = (): SessionHandle => ({
  pid: ChildProcessSpawner.ProcessId(4242),
  events: Stream.empty,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed({ _tag: "Completed" }),
});

const fakeHandle = makeHandle();

it.effect("get resolves a registered handle to the same instance", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    yield* registry.register(sessionId, fakeHandle);
    const resolved = yield* registry.get(sessionId);
    expect(resolved).toBe(fakeHandle);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("get fails with SessionNotFound for an unknown session", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    const error = yield* registry.get(sessionId).pipe(Effect.flip);
    expect(error).toBeInstanceOf(SessionNotFound);
    expect(error.id).toBe("ses-1");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("resolve returns a handle registered before the call, without waiting", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    yield* registry.register(sessionId, fakeHandle);
    // Already present: resolves immediately (no clock advance needed).
    const resolved = yield* registry.resolve(sessionId);
    expect(resolved).toBe(fakeHandle);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("resolve WAITS out the register-after-dispatch window, then resolves the handle", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    // Open the channel BEFORE anything is registered — the register-after-dispatch
    // window. `resolve` must not fail; it parks until registration lands. This is
    // event-driven (no TestClock advance), proving the wait wakes on `register`, not
    // on a clock tick — so a client reacting to a `running` delta needs no retry.
    const fiber = yield* Effect.forkChild(registry.resolve(sessionId), {
      startImmediately: true,
    });
    yield* registry.register(sessionId, fakeHandle);
    const resolved = yield* Fiber.join(fiber);
    expect(resolved).toBe(fakeHandle);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("resolve fails with SessionNotFound after the bound for a genuinely-absent session", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    // Nothing ever registers. `resolve` waits out the bound, then fails — it must not
    // hang. Advancing the TestClock past the hard bound fires the timeout exactly.
    const fiber = yield* Effect.forkChild(registry.resolve(sessionId).pipe(Effect.flip), {
      startImmediately: true,
    });
    yield* TestClock.adjust(SESSION_RESOLVE_TIMEOUT);
    const error = yield* Fiber.join(fiber);
    expect(error).toBeInstanceOf(SessionNotFound);
    expect(error.id).toBe("ses-1");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("layerWith honors a CONFIGURED resolve bound (FIX B — operational knob)", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    // A custom, shorter bound than the default: `resolve` must fire the timeout at
    // exactly the configured duration, proving the bound is threaded through the layer
    // (DaemonConfig.sessionResolveTimeout) and not the hardcoded default.
    const customBound = Duration.seconds(1);
    const fiber = yield* Effect.forkChild(registry.resolve(sessionId).pipe(Effect.flip), {
      startImmediately: true,
    });
    yield* TestClock.adjust(customBound);
    const error = yield* Fiber.join(fiber);
    expect(error).toBeInstanceOf(SessionNotFound);
  }).pipe(Effect.scoped, Effect.provide(layerWith(Duration.seconds(1)))),
);

it.effect("register is Scope-managed: the entry is removed when its scope closes", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    // Register inside a nested scope; on its close the finalizer removes the entry.
    yield* Effect.scoped(registry.register(sessionId, fakeHandle));
    const error = yield* registry.get(sessionId).pipe(Effect.flip);
    expect(error).toBeInstanceOf(SessionNotFound);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("id reuse: a superseded entry's teardown does not evict its live successor", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    const first = makeHandle();
    const second = makeHandle();

    // `first` registers in its OWN scope; `second` (same id) registers in the
    // ambient scope, overwriting `first` as the current mapping.
    const scopeA = yield* Scope.make();
    yield* registry.register(sessionId, first).pipe(Scope.provide(scopeA));
    yield* registry.register(sessionId, second);

    // Closing `first`'s scope must NOT evict `second` — the finalizer is
    // identity-guarded, so the live successor survives.
    yield* Scope.close(scopeA, Exit.void);
    const resolved = yield* registry.get(sessionId);
    expect(resolved).toBe(second);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);
