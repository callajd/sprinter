/**
 * `SessionRegistry` coverage (AE4.2) — the `sessionId → live SessionHandle` map
 * the session-channel handlers resolve against. Deterministic and offline: a
 * registered fake handle is resolved by `get`, a miss is the contract's
 * `SessionNotFound`, and Scope-managed registration removes the entry when the
 * registering scope closes (INV-PORT / INV-BOUNDARY — only owned neutral types
 * cross this surface).
 */
import { it } from "@effect/vitest";
import { Effect, Schema, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { SessionNotFound } from "@sprinter/contract";
import { SessionId } from "@sprinter/domain";
import type { SessionHandle } from "@sprinter/runner";
import { layer, SessionRegistry } from "./session-registry.ts";

const sessionId = Schema.decodeUnknownSync(SessionId)("ses-1");

/** A minimal fake {@link SessionHandle}: an empty event stream; inert verbs. */
const fakeHandle: SessionHandle = {
  pid: ChildProcessSpawner.ProcessId(4242),
  events: Stream.empty,
  send: () => Effect.void,
  interrupt: Effect.void,
  answerUi: () => Effect.void,
  result: Effect.succeed({ _tag: "Completed" }),
};

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

it.effect("register is Scope-managed: the entry is removed when its scope closes", () =>
  Effect.gen(function* () {
    const registry = yield* SessionRegistry;
    // Register inside a nested scope; on its close the finalizer removes the entry.
    yield* Effect.scoped(registry.register(sessionId, fakeHandle));
    const error = yield* registry.get(sessionId).pipe(Effect.flip);
    expect(error).toBeInstanceOf(SessionNotFound);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);
