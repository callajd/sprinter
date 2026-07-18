/**
 * Durable event journaling + offset-based resync (Track A CVG, task CE1.2) — what
 * turns the daemon's `events` feed from snapshot-on-connect into a DETERMINISTIC,
 * offset-based catch-up (D17 reconciliation; the resync AE4 deferred to "AE5's to
 * wire", see `./rpc-handlers.ts`).
 *
 * Two pieces, both expressed only in owned types (INV-BOUNDARY / INV-PORT):
 *
 * - {@link layerJournaling} — a {@link StateStore} decorator that, after every
 *   successful `put*` mutation, APPENDS the matching owned {@link WorkGraphEvent}
 *   to the durable {@link EventLogStore} (`append`), stamping it a monotonic
 *   `offset`. Because it is itself a `StateStore` (same port), every writer through
 *   the port — the command handlers AND the `JobRunner` — journals for free, with
 *   no consumer knowing (INV-PORT). It is composed BENEATH the publishing decorator
 *   (`./store-publishing.ts`), so a mutation is journaled durably BEFORE it is fanned
 *   out live — the ordering the gap-free resync below relies on.
 *
 * - {@link resyncEvents} — the stream the `events` RPC returns: it EAGERLY subscribes
 *   to the live feed, THEN replays the durable log from a starting offset via
 *   {@link EventLogStore.tail}, then hands over to the live tail. Subscribing before
 *   reading closes the replay/live gap; `WorkGraphEvent` is upsert-idempotent (D8 —
 *   the carried node replaces any prior of the same id), so the small overlap at the
 *   boundary is harmless. A reconnecting client thus catches up on the whole durable
 *   history deterministically, not just the deltas that happen after it attaches.
 *
 * The wire `events` procedure carries no cursor (the contract is FROZEN,
 * INV-CONTRACT), so the served endpoint replays from the log ORIGIN (`tail(0)`);
 * {@link resyncFrom} is the offset-parameterized primitive a future
 * cursor-carrying transport (CE2.2) resumes from without re-deriving a snapshot.
 */
import { Array as Arr, Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { WorkGraphEvent } from "@sprinter/contract";
import { type PersistedEvent, StateStore, type StateStoreError } from "@sprinter/state";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Feed = Context.Service.Shape<typeof WorkGraphEvents>;

// ── journaling decorator ──────────────────────────────────────────────────────

/**
 * Append one owned {@link WorkGraphEvent} to the durable log, keyed by its `_tag`
 * (a producer-owned discriminator, D6) with the encoded event as payload. Encoding
 * an owned schema value cannot fail (`orDie`); an {@link EventLogStore} write can,
 * so — like the `put*` it follows — the effect surfaces {@link StateStoreError}.
 */
const journalDelta = (base: Store, event: WorkGraphEvent): Effect.Effect<void, StateStoreError> =>
  Schema.encodeEffect(WorkGraphEvent)(event).pipe(
    Effect.orDie,
    Effect.flatMap((payload) => base.events.append({ kind: event._tag, payload })),
    Effect.asVoid,
  );

/** Build the journaling store shape: delegate to `base`, then journal each `put*`'s delta. */
const journaling = (base: Store): Store =>
  StateStore.of({
    workGraph: {
      putWorkstream: (workstream) =>
        base.workGraph
          .putWorkstream(workstream)
          .pipe(Effect.andThen(journalDelta(base, { _tag: "WorkstreamChanged", workstream }))),
      putEpic: (epic) =>
        base.workGraph
          .putEpic(epic)
          .pipe(Effect.andThen(journalDelta(base, { _tag: "EpicChanged", epic }))),
      putIssue: (issue) =>
        base.workGraph
          .putIssue(issue)
          .pipe(Effect.andThen(journalDelta(base, { _tag: "IssueChanged", issue }))),
      getWorkstream: base.workGraph.getWorkstream,
      getEpic: base.workGraph.getEpic,
      getIssue: base.workGraph.getIssue,
      listWorkstreams: base.workGraph.listWorkstreams,
      listEpics: base.workGraph.listEpics,
      listIssues: base.workGraph.listIssues,
    },
    jobs: {
      putJob: (job) =>
        base.jobs.putJob(job).pipe(Effect.andThen(journalDelta(base, { _tag: "JobChanged", job }))),
      getJob: base.jobs.getJob,
      listJobsForIssue: base.jobs.listJobsForIssue,
      putSession: (session) =>
        base.jobs
          .putSession(session)
          .pipe(Effect.andThen(journalDelta(base, { _tag: "SessionChanged", session }))),
      getSession: base.jobs.getSession,
      getSessionForJob: base.jobs.getSessionForJob,
    },
    events: base.events,
  });

/**
 * Decorate `baseLayer` so every persisted mutation is journaled to the durable
 * {@link EventLogStore}. The base store is wired in via `Layer.provide`, so
 * consumers see only the decorated port (INV-PORT). Compose it BENEATH the
 * publishing decorator so journaling (durable) precedes live fan-out.
 */
export const layerJournaling = <RIn>(
  baseLayer: Layer.Layer<StateStore, StateStoreError, RIn>,
): Layer.Layer<StateStore, StateStoreError, RIn> =>
  Layer.effect(
    StateStore,
    Effect.gen(function* () {
      const base = yield* StateStore;
      return journaling(base);
    }),
  ).pipe(Layer.provide(baseLayer));

// ── offset-based resync ───────────────────────────────────────────────────────

/**
 * Decode one durable entry back to its owned {@link WorkGraphEvent}. An entry that
 * does not decode as a work-graph delta (a foreign `kind` a future producer may
 * append) is skipped, not a failure — the resync replays only work-graph history.
 */
const decodeDelta = (entry: PersistedEvent): Effect.Effect<Option.Option<WorkGraphEvent>> =>
  Schema.decodeUnknownEffect(WorkGraphEvent)(entry.payload).pipe(
    Effect.map(Option.some),
    Effect.catch(() => Effect.succeedNone),
  );

/**
 * The durable replay stream: every persisted {@link WorkGraphEvent} with an offset
 * strictly greater than `offset`, in order — the AE1 {@link EventLogStore.tail}
 * primitive a client resumes from its last-seen cursor. A store read failure is a
 * defect (`orDie`) rather than a stream error, keeping the feed's error channel
 * empty (matching the frozen contract's `events` success/never shape).
 */
export const resyncFrom = (store: Store, offset: number): Stream.Stream<WorkGraphEvent> =>
  Stream.unwrap(
    store.events.tail(offset).pipe(
      Effect.orDie,
      Effect.flatMap((entries) => Effect.forEach(entries, decodeDelta)),
      Effect.map((decoded) => Stream.fromIterable(Arr.getSomes(decoded))),
    ),
  );

/**
 * The `events` RPC feed: EAGERLY subscribe to the live work-graph feed, THEN replay
 * the whole durable log from the origin, THEN stream the live tail. Subscribing
 * before the durable read closes the replay/live gap; upsert-idempotent deltas make
 * the small boundary overlap harmless. The result is a deterministic catch-up from
 * durable history, not merely snapshot-on-connect (D17).
 */
export const resyncEvents = (store: Store, feed: Feed): Stream.Stream<WorkGraphEvent> =>
  Stream.unwrap(
    feed.subscribe.pipe(
      Effect.map((subscription) =>
        Stream.concat(resyncFrom(store, 0), Stream.fromSubscription(subscription)),
      ),
    ),
  );
