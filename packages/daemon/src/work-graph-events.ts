/**
 * `WorkGraphEvents` — the daemon's reactive work-graph feed (Track A, task AE4.1),
 * the real `PubSub` spine behind the frozen contract's streaming `events` RPC
 * (D17 / INV-REACTIVE).
 *
 * It is a thin, owned wrapper over an Effect `PubSub<OffsetEvent>`: producers
 * `publish` an offset-stamped delta whenever a work-graph node changes, and
 * consumers read the feed either as a `Stream` ({@link WorkGraphEvents.changes} —
 * what the `events` handler returns) or as a scoped {@link PubSub.Subscription}
 * ({@link WorkGraphEvents.subscribe} — the precise primitive for a consumer that
 * must be subscribed *before* a producer publishes). The feed is streaming-first:
 * there is no poll loop anywhere on this path.
 *
 * Only owned domain types cross this surface — each item is the contract's
 * {@link OffsetEvent} (a {@link WorkGraphEvent} plus its durable offset), composed
 * exclusively of `@sprinter/domain` nodes (INV-BOUNDARY / INV-PORT). The single
 * producer is the journaling `StateStore` decorator (`./event-journal.ts`) — the
 * one layer that mints the durable offset — so *every* persisted mutation, whether
 * from a command handler or the `JobRunner`, automatically fans out an offset-stamped
 * delta whose offset matches the durable log.
 */
import { Context, Effect, Layer, PubSub, Stream } from "effect";
import type { Scope } from "effect/Scope";
import type { OffsetEvent } from "@sprinter/contract";

/**
 * The reactive work-graph feed PORT (INV-NAMING, `sprinter/<area>/<Name>`). A
 * consumer depends on THIS service, never on a concrete `PubSub` instance;
 * {@link layer} provides the backing (INV-PORT).
 *
 * The feed carries {@link OffsetEvent}s — each delta paired with the DURABLE
 * `event_log` offset it was journaled at (CE2.0). The producer (the
 * journaling `StateStore` decorator, `./event-journal.ts`) is the one layer that
 * mints that offset, so it is also the one that publishes: the live-tail offset
 * shares ONE coordinate space with the durable replay (`EventLogStore.tail`), so a
 * client can resume from any streamed item's offset. The strict `> sinceOffset`,
 * no-re-delivery guarantee is scoped to that RECONNECT RESUME (the durable
 * `tail`); the live feed itself is not guaranteed strictly monotonic — publish
 * happens after the durable commit, so concurrent writers can interleave — and it
 * can overlap the durable replay at the boundary, all harmless under upsert
 * idempotency.
 */
export class WorkGraphEvents extends Context.Service<
  WorkGraphEvents,
  {
    /** Publish one offset-stamped {@link OffsetEvent} delta to every current subscriber. */
    readonly publish: (event: OffsetEvent) => Effect.Effect<void>;
    /**
     * The feed as a `Stream` — what the contract's `events` RPC handler returns.
     * A subscription is established when the stream is pulled; deltas published
     * before that point are not replayed (durable offset-resync covers the gap).
     */
    readonly changes: Stream.Stream<OffsetEvent>;
    /**
     * A scoped subscription to the feed. Unlike {@link changes}, resolving this
     * effect establishes the subscription eagerly, so a consumer can subscribe
     * and only then trigger the producer without racing the subscription.
     */
    readonly subscribe: Effect.Effect<PubSub.Subscription<OffsetEvent>, never, Scope>;
  }
>()("sprinter/daemon/WorkGraphEvents") {}

/**
 * The {@link WorkGraphEvents} implementation over an unbounded {@link PubSub}
 * (`Layer.effect` + `Service.of`, per conventions). Unbounded so a slow client
 * never applies backpressure to — or drops a delta from — the work graph.
 */
export const layer: Layer.Layer<WorkGraphEvents> = Layer.effect(
  WorkGraphEvents,
  Effect.gen(function* () {
    const pubsub = yield* PubSub.unbounded<OffsetEvent>();
    return WorkGraphEvents.of({
      publish: (event) => PubSub.publish(pubsub, event).pipe(Effect.asVoid),
      changes: Stream.fromPubSub(pubsub),
      subscribe: PubSub.subscribe(pubsub),
    });
  }),
);
