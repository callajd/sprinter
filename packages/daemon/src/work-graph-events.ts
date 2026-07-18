/**
 * `WorkGraphEvents` — the daemon's reactive work-graph feed (Track A, task AE4.1),
 * the real `PubSub` spine behind the frozen contract's streaming `events` RPC
 * (D17 / INV-REACTIVE).
 *
 * It is a thin, owned wrapper over an Effect `PubSub<WorkGraphEvent>`: producers
 * `publish` a delta whenever a work-graph node changes, and consumers read the
 * feed either as a `Stream` ({@link WorkGraphEvents.changes} — what the `events`
 * handler returns) or as a scoped {@link PubSub.Subscription}
 * ({@link WorkGraphEvents.subscribe} — the precise primitive for a consumer that
 * must be subscribed *before* a producer publishes). The feed is streaming-first:
 * there is no poll loop anywhere on this path.
 *
 * Only owned domain types cross this surface — the delta is the contract's
 * {@link WorkGraphEvent}, composed exclusively of `@sprinter/domain` nodes
 * (INV-BOUNDARY / INV-PORT). The single producer in AE4.1 is the publishing
 * `StateStore` decorator (`./store-publishing.ts`), so *every* persisted mutation
 * — whether from a command handler or the `JobRunner` — automatically fans out a
 * delta.
 */
import { Context, Effect, Layer, PubSub, Stream } from "effect";
import type { Scope } from "effect/Scope";
import type { WorkGraphEvent } from "@sprinter/contract";

/**
 * The reactive work-graph feed PORT (INV-NAMING, `sprinter/<area>/<Name>`). A
 * consumer depends on THIS service, never on a concrete `PubSub` instance;
 * {@link layer} provides the backing (INV-PORT).
 */
export class WorkGraphEvents extends Context.Service<
  WorkGraphEvents,
  {
    /** Publish a single {@link WorkGraphEvent} delta to every current subscriber. */
    readonly publish: (event: WorkGraphEvent) => Effect.Effect<void>;
    /**
     * The feed as a `Stream` — what the contract's `events` RPC handler returns.
     * A subscription is established when the stream is pulled; deltas published
     * before that point are not replayed (snapshot-on-connect covers the gap).
     */
    readonly changes: Stream.Stream<WorkGraphEvent>;
    /**
     * A scoped subscription to the feed. Unlike {@link changes}, resolving this
     * effect establishes the subscription eagerly, so a consumer can subscribe
     * and only then trigger the producer without racing the subscription.
     */
    readonly subscribe: Effect.Effect<PubSub.Subscription<WorkGraphEvent>, never, Scope>;
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
    const pubsub = yield* PubSub.unbounded<WorkGraphEvent>();
    return WorkGraphEvents.of({
      publish: (event) => PubSub.publish(pubsub, event).pipe(Effect.asVoid),
      changes: Stream.fromPubSub(pubsub),
      subscribe: PubSub.subscribe(pubsub),
    });
  }),
);
