/**
 * `SessionEvents` — the daemon's reactive UNIFIED session feed, the
 * session-channel analogue of {@link WorkGraphEvents}. It is the live half of the
 * `sessionEvents` durable replay-then-tail AND the carrier of a live driving session's full
 * reactive flow. As a session's fold runs, the journaling `StateStore` decorator
 * (`./event-journal.ts`) fans out EVERY {@link SessionEvent} here:
 *
 * - DURABLE, transcript-grade events (`EntryAppended`/`Notice`) are journaled to the
 *   per-session transcript log first, then fanned out stamped WITH the durable offset they
 *   committed at, so a subscribed `sessionEvents` handler tails new entries the moment they
 *   persist and a reconnect can resume from that offset;
 * - EPHEMERAL live deltas (turn lifecycle, message/tool partials, `UiRequestRaised`, …) are
 *   fanned out offset-LESS — never persisted, never advancing the resume cursor — so a live
 *   subscriber still receives the fine-grained reactive flow.
 *
 * Either way there is no poll loop on this path (INV-REACTIVE).
 *
 * It is a thin, owned wrapper over an Effect `PubSub<SessionFeedItem>`: the feed is
 * GLOBAL across sessions (one PubSub for the daemon), each item carrying the `sessionId`
 * it belongs to so a per-session subscriber filters to its own session. Only owned domain
 * types cross this surface — the branded `SessionId`, the OPTIONAL durable `NonNegativeInt`
 * offset, and the owned `SessionEvent` (INV-BOUNDARY / INV-PORT). The single producer is the
 * journaling decorator — the one layer that mints the durable per-session offset — so the
 * live-tail offset (when present) shares ONE coordinate space with the durable replay
 * ({@link SessionLogStore.tail}), the property the `sinceOffset` reconnect resume relies on.
 */
import { Context, Effect, Layer, PubSub } from "effect";
import type { Scope } from "effect/Scope";
import type { NonNegativeInt, SessionEvent, SessionId } from "@sprinter/domain";

/**
 * One item on the {@link SessionEvents} feed: a {@link SessionEvent} paired with the
 * `sessionId` it belongs to and an OPTIONAL durable per-session `offset`. The `sessionId`
 * scopes the global feed so a per-session subscriber filters to its own session. `offset` is
 * PRESENT for a durable, transcript-grade event (the same durable coordinate the
 * `sessionEvents` replay reads, so live and replay agree on one offset per durable entry) and
 * ABSENT for an ephemeral live delta (never journaled, never resumable).
 */
export interface SessionFeedItem {
  /** The session this event belongs to (the feed's per-session scope). */
  readonly sessionId: SessionId;
  /**
   * The durable per-session offset the entry was journaled at — PRESENT for a
   * durable transcript-grade event, ABSENT for an ephemeral live delta.
   */
  readonly offset?: NonNegativeInt;
  /** The session event — durable transcript-grade (offset present) or ephemeral (offset absent). */
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
    /**
     * Publish one item to every current subscriber — a durable transcript entry
     * (offset-stamped) or an ephemeral live delta (offset-less).
     */
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
