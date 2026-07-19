/**
 * `WorkGraphEvents` coverage (AE4.1) — the real `PubSub` feed behind the
 * streaming `events` RPC (D17 / INV-REACTIVE). Deterministic and offline: a
 * published delta reaches a prior subscriber, and flows over the `changes`
 * stream the handler returns.
 */
import { it } from "@effect/vitest";
import { Effect, Fiber, Option, PubSub, Schema, Stream } from "effect";
import { expect } from "vitest";
import type { OffsetEvent } from "@sprinter/contract";
import { Workstream } from "@sprinter/domain";
import { layer, WorkGraphEvents } from "./work-graph-events.ts";

const workstream = Schema.decodeUnknownSync(Workstream)({
  id: "ws-1",
  name: "Foundation",
  repo: "callajd/sprinter",
  status: "pending",
  epics: [],
});
// The feed carries offset-stamped envelopes (contract v3 / CE2.0).
const delta: OffsetEvent = { offset: 7, event: { _tag: "WorkstreamChanged", workstream } };

it.effect("delivers a published delta to a prior subscriber", () =>
  Effect.gen(function* () {
    const feed = yield* WorkGraphEvents;
    // Subscribe BEFORE publishing — no race, unlike the lazy `changes` stream.
    const subscription = yield* feed.subscribe;
    yield* feed.publish(delta);
    const received = yield* PubSub.take(subscription);
    expect(received).toStrictEqual(delta);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("streams published deltas over the changes feed", () =>
  Effect.gen(function* () {
    const feed = yield* WorkGraphEvents;
    const collector = yield* feed.changes.pipe(Stream.take(1), Stream.runHead, Effect.forkChild);
    // Publish until the (lazily-subscribing) consumer has attached and taken one.
    yield* feed
      .publish(delta)
      .pipe(Effect.andThen(Effect.yieldNow), Effect.forever, Effect.forkChild);
    const head = yield* Fiber.join(collector);
    expect(Option.getOrThrow(head)).toStrictEqual(delta);
  }).pipe(Effect.provide(layer)),
);
