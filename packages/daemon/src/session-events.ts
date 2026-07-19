/**
 * `SessionEvents` — the daemon's reactive DURABLE-TRANSCRIPT feed, the
 * session-channel analogue of {@link WorkGraphEvents}. It is the live half of the
 * `sessionEvents` durable replay-then-tail: as a session's fold journals each durable,
 * transcript-grade {@link SessionEvent} to the per-session transcript log, the journaling
 * `StateStore` decorator (`./event-journal.ts`) fans it out here stamped with the durable
 * offset it committed at, so a subscribed `sessionEvents` handler tails new entries the
 * moment they persist (INV-REACTIVE — no poll loop on this path).
 *
 * It is a thin, owned wrapper over an Effect `PubSub<SessionFeedItem>`: the feed is
 * GLOBAL across sessions (one PubSub for the daemon), each item carrying the `sessionId`
 * it belongs to so a per-session subscriber filters to its own session. Only owned domain
 * types cross this surface — the branded `SessionId`, the durable `NonNegativeInt` offset,
 * and the owned `SessionEvent` (INV-BOUNDARY / INV-PORT). The single producer is the
 * journaling decorator — the one layer that mints the durable per-session offset — so the
 * live-tail offset shares ONE coordinate space with the durable replay
 * ({@link SessionLogStore.tail}), the property the `sinceOffset` reconnect resume relies on.
 */
import { Context, Effect, Layer, PubSub } from "effect";
import type { Scope } from "effect/Scope";
import type { NonNegativeInt, SessionEvent, SessionId } from "@sprinter/domain";

/**
 * One item on the {@link SessionEvents} feed: a durable, transcript-grade
 * {@link SessionEvent} paired with the `sessionId` it belongs to and the durable per-session
 * `offset` it was journaled at. The `sessionId` scopes the global feed so a per-session
 * subscriber filters to its own session; `offset` is the same durable coordinate the
 * `sessionEvents` replay reads, so live and replay agree on one offset per entry.
 */
export interface SessionFeedItem {
  /** The session this durable transcript entry belongs to (the feed's per-session scope). */
  readonly sessionId: SessionId;
  /** The durable per-session offset the entry was journaled at. */
  readonly offset: NonNegativeInt;
  /** The durable, transcript-grade session event. */
  readonly event: SessionEvent;
}

/**
 * The reactive session-transcript feed PORT (INV-NAMING, `sprinter/<area>/<Name>`). A
 * consumer depends on THIS service, never on a concrete `PubSub`; {@link layer} provides
 * the backing (INV-PORT). The producer (the journaling `StateStore` decorator) is the one
 * layer that mints the durable per-session offset, so it is also the one that publishes:
 * the live-tail offset shares one coordinate space with the durable replay. The strict
 * `> sinceOffset`, no-re-delivery guarantee is scoped to the RECONNECT RESUME (the durable
 * `tail`); a single live stream can overlap the durable replay at the boundary, harmless
 * under the consumer's id-keyed transcript reconciliation.
 */
export class SessionEvents extends Context.Service<
  SessionEvents,
  {
    /** Publish one offset-stamped durable transcript entry to every current subscriber. */
    readonly publish: (item: SessionFeedItem) => Effect.Effect<void>;
    /**
     * A scoped subscription to the feed. Resolving this effect establishes the
     * subscription EAGERLY, so a `sessionEvents` handler can subscribe and only then read
     * the durable log without racing the producer (subscribe-before-replay).
     */
    readonly subscribe: Effect.Effect<PubSub.Subscription<SessionFeedItem>, never, Scope>;
  }
>()("sprinter/daemon/SessionEvents") {}

/**
 * The {@link SessionEvents} implementation over an unbounded {@link PubSub}
 * (`Layer.effect` + `Service.of`, per conventions). Unbounded so a slow `sessionEvents`
 * subscriber never applies backpressure to — or drops a durable entry from — a running
 * session's fold.
 */
export const layer: Layer.Layer<SessionEvents> = Layer.effect(
  SessionEvents,
  Effect.gen(function* () {
    const pubsub = yield* PubSub.unbounded<SessionFeedItem>();
    return SessionEvents.of({
      publish: (item) => PubSub.publish(pubsub, item).pipe(Effect.asVoid),
      subscribe: PubSub.subscribe(pubsub),
    });
  }),
);
