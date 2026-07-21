/**
 * `ExecutionEvents` — the daemon's reactive UNIFIED execution feed, the
 * execution-channel analogue of {@link WorkGraphEvents}. It is the live half of the
 * `executionEvents` durable replay-then-tail AND the carrier of a live driving execution's full
 * reactive flow. As an execution's fold runs, the journaling `StateStore` decorator
 * (`./event-journal.ts`) fans out EVERY {@link ExecutionEvent} here:
 *
 * - DURABLE, transcript-grade events (`EntryAppended`/`Notice`) are journaled to the
 *   per-execution transcript log first, then fanned out stamped WITH the durable offset they
 *   committed at, so a subscribed `executionEvents` handler tails new entries the moment they
 *   persist and a reconnect can resume from that offset;
 * - EPHEMERAL live deltas (turn lifecycle, message/tool partials, `UiRequestRaised`, …) are
 *   fanned out offset-LESS — never persisted, never advancing the resume cursor — so a live
 *   subscriber still receives the fine-grained reactive flow.
 *
 * Either way there is no poll loop on this path (INV-REACTIVE).
 *
 * It is a thin, owned wrapper over an Effect `PubSub<ExecutionFeedItem>`: the feed is
 * GLOBAL across executions (one PubSub for the daemon), each item carrying the `executionId`
 * it belongs to so a per-execution subscriber filters to its own execution. Only owned domain
 * types cross this surface — the branded `ExecutionId`, the OPTIONAL durable `NonNegativeInt`
 * offset, and the owned `ExecutionEvent` (INV-BOUNDARY / INV-PORT). The single producer is the
 * journaling decorator — the one layer that mints the durable per-execution offset — so the
 * live-tail offset (when present) shares ONE coordinate space with the durable replay
 * ({@link ExecutionLogStore.tail}), the property the `sinceOffset` reconnect resume relies on.
 */
import { Context, Effect, Layer, PubSub } from "effect";
import type { Scope } from "effect/Scope";
import type { NonNegativeInt, ExecutionEvent, ExecutionId } from "@sprinter/domain";

/**
 * One item on the {@link ExecutionEvents} feed: an {@link ExecutionEvent} paired with the
 * `executionId` it belongs to and an OPTIONAL durable per-execution `offset`. The `executionId`
 * scopes the global feed so a per-execution subscriber filters to its own execution. `offset` is
 * PRESENT for a durable, transcript-grade event (the same durable coordinate the
 * `executionEvents` replay reads, so live and replay agree on one offset per durable entry) and
 * ABSENT for an ephemeral live delta (never journaled, never resumable).
 */
export interface ExecutionFeedItem {
  /** The execution this event belongs to (the feed's per-execution scope). */
  readonly executionId: ExecutionId;
  /**
   * The durable per-execution offset the entry was journaled at — PRESENT for a
   * durable transcript-grade event, ABSENT for an ephemeral live delta.
   */
  readonly offset?: NonNegativeInt;
  /** The execution event — durable transcript-grade (offset present) or ephemeral (offset absent). */
  readonly event: ExecutionEvent;
}

/**
 * The reactive execution-transcript feed PORT (INV-NAMING, `sprinter/<area>/<Name>`). A
 * consumer depends on THIS service, never on a concrete `PubSub`; {@link layer} provides
 * the backing (INV-PORT). The producer (the journaling `StateStore` decorator) is the one
 * layer that mints the durable per-execution offset, so it is also the one that publishes:
 * the live-tail offset shares one coordinate space with the durable replay. The strict
 * `> sinceOffset`, no-re-delivery guarantee is scoped to the RECONNECT RESUME (the durable
 * `tail`); a single live stream can overlap the durable replay at the boundary, harmless
 * under the consumer's id-keyed transcript reconciliation.
 */
export class ExecutionEvents extends Context.Service<
  ExecutionEvents,
  {
    /**
     * Publish one item to every current subscriber — a durable transcript entry
     * (offset-stamped) or an ephemeral live delta (offset-less).
     */
    readonly publish: (item: ExecutionFeedItem) => Effect.Effect<void>;
    /**
     * A scoped subscription to the feed. Resolving this effect establishes the
     * subscription EAGERLY, so a `executionEvents` handler can subscribe and only then read
     * the durable log without racing the producer (subscribe-before-replay).
     */
    readonly subscribe: Effect.Effect<PubSub.Subscription<ExecutionFeedItem>, never, Scope>;
  }
>()("sprinter/daemon/ExecutionEvents") {}

/**
 * The {@link ExecutionEvents} implementation over an unbounded {@link PubSub}
 * (`Layer.effect` + `Service.of`, per conventions). Unbounded so a slow `executionEvents`
 * subscriber never applies backpressure to — or drops a durable entry from — a running
 * execution's fold.
 */
export const layer: Layer.Layer<ExecutionEvents> = Layer.effect(
  ExecutionEvents,
  Effect.gen(function* () {
    const pubsub = yield* PubSub.unbounded<ExecutionFeedItem>();
    return ExecutionEvents.of({
      publish: (item) => PubSub.publish(pubsub, item).pipe(Effect.asVoid),
      subscribe: PubSub.subscribe(pubsub),
    });
  }),
);
