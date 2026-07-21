/**
 * Durable event journaling + offset-based resync (Track A CVG, task CE1.2) — what
 * turns the daemon's `events` feed from snapshot-on-connect into a DETERMINISTIC,
 * offset-based catch-up (D17 reconciliation; the resync AE4 deferred to "AE5's to
 * wire", see `./rpc-handlers.ts`).
 *
 * Two pieces, both expressed only in owned types (INV-BOUNDARY / INV-PORT):
 *
 * - {@link layerJournaling} — a {@link StateStore} decorator that, for every `put*`
 *   mutation, persists the node write AND appends the matching owned
 *   {@link WorkGraphEvent} to the durable {@link EventLogStore} (`append`, stamping a
 *   monotonic `offset`) inside a SINGLE {@link StateStore.withTransaction} — so the
 *   two commit together or neither does. A crash can never leave the node persisted
 *   with its delta un-journaled (which would make the offset feed an incomplete
 *   history — INV-RESTART). Because it mints the durable offset, it is ALSO the layer
 *   that fans the delta out live on the {@link WorkGraphEvents} feed, stamped with
 *   that same offset — the durable write commits BEFORE the live publish, and the
 *   live and replay offsets are one coordinate space by construction (the property
 *   the `sinceOffset` reconnect resume relies on). Being itself a `StateStore` (same
 *   port), every writer through the port — the command handlers AND the `JobRunner` — journals and
 *   fans out atomically for free, with no consumer knowing (INV-PORT).
 *
 * - {@link resyncEvents} — the stream the `events` RPC returns: it VALIDATES the
 *   resume cursor against this store generation, EAGERLY subscribes to the live feed,
 *   THEN replays the durable log from that offset via {@link EventLogStore.tail}, then
 *   hands over to the live tail. Each streamed item
 *   is an {@link OffsetEvent} carrying its durable offset. Subscribing before reading
 *   closes the replay/live gap; `WorkGraphEvent` is upsert-idempotent (D8 — the
 *   carried node replaces any prior of the same id), so the small overlap at the
 *   boundary is harmless. A reconnecting client thus catches up on the whole durable
 *   history deterministically, not just the deltas that happen after it attaches.
 *
 * The wire `events` procedure carries an OPTIONAL {@link ResumeContext} REQUEST value
 * (cursor AND generation, inseparably) and an
 * {@link OffsetEvent} RESPONSE envelope (CE2.0, INV-CONTRACT): a request
 * with NO `resume` (a present but empty `{}` payload) replays from the log ORIGIN
 * (`tail(0)`), present resumes STRICTLY AFTER that offset, and each streamed item
 * exposes the durable offset the client feeds back as its next cursor. {@link resyncFrom} is the
 * offset-parameterized primitive both cases drive (CE2.2 layers reconnect/
 * backpressure on top) without re-deriving a snapshot.
 *
 * A durable cursor outlives a STORE GENERATION, though, and the store never migrates
 * (INV-FRESH): a schema-version bump drops the database, restarting the log's offsets
 * at `1`. That is what {@link requireLiveCursor} exists for. It compares the
 * GENERATION IDENTITY the request carries against `StateStore.generation` — an
 * equality on an explicit id, not an inference from offsets, because a stale cursor
 * stops being detectable from the numbers the moment the new log outgrows it — and
 * fails with the contract's `ResyncRequired` so the client re-hydrates from `snapshot`
 * instead of being silently mis-resumed. The extent check survives as a cheap
 * secondary.
 */
import { Array as Arr, Context, Effect, Layer, Option, Schema, Stream } from "effect";
import {
  type OffsetEvent,
  type OffsetSessionEvent,
  type ResumeContext,
  ResyncRequired,
  WorkGraphEvent,
} from "@sprinter/contract";
import {
  type Agent,
  type NonNegativeInt,
  observationsAgree,
  type Repository,
  type SessionEvent,
  type SessionId,
} from "@sprinter/domain";
import {
  type AgentWrite,
  type PersistedEvent,
  type PersistedSessionEvent,
  StateStore,
  type StateStoreError,
} from "@sprinter/state";
import { SessionEvents } from "./session-events.ts";
import { WorkGraphEvents } from "./work-graph-events.ts";

type Store = Context.Service.Shape<typeof StateStore>;
type Feed = Context.Service.Shape<typeof WorkGraphEvents>;
type SessionFeed = Context.Service.Shape<typeof SessionEvents>;

// ── journaling + publishing decorator ─────────────────────────────────────────

/**
 * Append one owned {@link WorkGraphEvent} to the durable log, keyed by its `_tag`
 * (a producer-owned discriminator, D6) with the encoded event as payload, and
 * return the DURABLE `offset` it was stamped with. Encoding an owned schema value
 * cannot fail (`orDie`); an {@link EventLogStore} write can, so — like the `put*` it
 * follows — the effect surfaces {@link StateStoreError}. The returned offset is the
 * SAME coordinate {@link EventLogStore.tail} / {@link resyncFrom} read, so the live
 * fan-out and the durable replay agree on one offset per delta (CE2.0).
 */
const journalDelta = (
  base: Store,
  event: WorkGraphEvent,
): Effect.Effect<NonNegativeInt, StateStoreError> =>
  Schema.encodeEffect(WorkGraphEvent)(event).pipe(
    Effect.orDie,
    Effect.flatMap((payload) => base.events.append({ kind: event._tag, payload })),
    Effect.map((persisted) => persisted.offset),
  );

/**
 * Persist a node write AND its offset-log delta ATOMICALLY (INV-RESTART), THEN fan
 * the delta out live stamped with the durable offset it committed at. The put and
 * the journal append run inside a single {@link StateStore.withTransaction}, so a
 * crash can never leave the node persisted with its delta un-journaled (which would
 * make the offset feed an incomplete history); the live {@link WorkGraphEvents}
 * publish happens AFTER that transaction commits (never on a rolled-back write) and
 * carries the same offset, so the live tail and the durable replay share ONE
 * coordinate space — the property the `sinceOffset` reconnect resume relies on.
 * Because the live publish happens AFTER the commit rather than inside it, two
 * concurrent writers can commit in one order and publish in the other, so the LIVE
 * feed order is not guaranteed strictly monotonic by offset; that ordering slack is
 * absorbed by upsert idempotency, and the durable replay a reconnect resumes from is
 * still strictly ordered by offset.
 */
const putAndJournal = (
  base: Store,
  feed: Feed,
  put: Effect.Effect<void, StateStoreError>,
  event: WorkGraphEvent,
): Effect.Effect<void, StateStoreError> =>
  base
    .withTransaction(put.pipe(Effect.andThen(journalDelta(base, event))))
    .pipe(Effect.flatMap((offset) => feed.publish({ offset, event })));

/**
 * The {@link putAndJournal} variant for the APPEND-ONLY registry: journal and fan out
 * ONLY when the append actually wrote a row.
 *
 * `putAgent` is idempotent — re-appending a byte-identical revision under an id
 * already stored SUCCEEDS as a no-op, which is exactly what makes a crash-retry safe
 * — and it reports which of the two happened ({@link AgentWrite}). Journaling
 * unconditionally would throw that away: every retry of the same append would add
 * another `AgentChanged` to the durable log and fan another copy out to every client,
 * so a retry loop grows the log without bound and a "no-op" write is not one. Nothing
 * changed, so nothing is journaled.
 *
 * The decision is made INSIDE the transaction, from the store's own answer, so the
 * row write and its delta still commit together or not at all (INV-RESTART); the live
 * publish still happens after the commit, and is skipped entirely on the no-op path.
 */
const putAgentAndJournal = (
  base: Store,
  feed: Feed,
  agent: Agent,
): Effect.Effect<AgentWrite, StateStoreError> =>
  base
    .withTransaction(
      base.agents
        .putAgent(agent)
        .pipe(
          Effect.flatMap((write) =>
            write === "appended"
              ? journalDelta(base, { _tag: "AgentChanged", agent }).pipe(
                  Effect.map((offset) => Option.some(offset)),
                )
              : Effect.succeedNone,
          ),
        ),
    )
    .pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.succeed<AgentWrite>("unchanged"),
          onSome: (offset) =>
            feed
              .publish({ offset, event: { _tag: "AgentChanged", agent } })
              .pipe(Effect.as<AgentWrite>("appended")),
        }),
      ),
    );

/**
 * The {@link putAndJournal} variant for the OBSERVED `── STATE ──` layer: journal and
 * fan out only when the observation actually CHANGED something.
 *
 * The row write is unconditional — a refresh REPLACES the record wholesale under a new
 * `observedAt` (D7), and that stamp is what DE4.4 renders staleness from, so it must
 * land every time. What is suppressed is the DELTA, and the two are not the same
 * question: `observedAt` is not something the repository IS, it is when we last looked,
 * so two resolves of an unchanged repository produce records that always differ by it
 * and agree about everything a client mirrors ({@link observationsAgree}).
 *
 * Journaling unconditionally would therefore make every RE-OBSERVATION a durable log
 * entry and a broadcast to every client, for a repository that did not move. That is
 * reachable on the ordinary rejection path, not just in theory:
 * `createWorkstreamFromPlan` RESOLVES the repository before it can derive the
 * workstream id (the id is a function of `repository.id`), so a client retry-looping on
 * a `PlanRejected("a workstream already exists…")` re-observes and re-puts on EVERY
 * attempt. The event log is append-only with no trim or compaction, so that loop grows
 * it without bound and re-broadcasts an identical `RepositoryChanged` each time —
 * exactly the amplification {@link putAgentAndJournal} exists to suppress for the
 * registry.
 *
 * The prior read, the write and the journal append all happen INSIDE one transaction,
 * exactly as {@link putAgentAndJournal} decides from the store's own answer inside one:
 * the comparison is made against the row this write is about to replace, and the row
 * write and its delta still commit together or not at all (INV-RESTART). The live
 * publish still happens after the commit, and is skipped entirely on the agreeing path.
 *
 * ## What this closes, and what it does NOT
 *
 * Be precise about the scope, because the retry loop above has TWO halves and this
 * closes ONE. The DURABLE half is closed: the append-only event log stops growing and
 * the identical `RepositoryChanged` stops being broadcast to every client.
 *
 * The CODE-HOST half is untouched. Each rejected attempt still RESOLVES the repository
 * before it can derive the workstream id, and a resolve is two live GitHub requests (the
 * repository, then its branches). The adapter carries no retry budget, no backoff and no
 * cache (`packages/repository/src/github.ts`), and the daemon adds none — so a client
 * looping on `PlanRejected("a workstream already exists…")` issues two requests per
 * attempt against GitHub's 5,000 requests/hour authenticated REST quota, exhausting it in
 * roughly 2,500 attempts — about 42 minutes at one attempt per second, less for a tighter
 * loop. After that EVERY code-host operation the daemon makes is refused (403), so a
 * single misbehaving client's retry loop denies the whole daemon its code host until the
 * limit resets. Nothing here defends against that; issue **#98** is where a retry budget,
 * backoff and a short-lived resolve cache belong.
 *
 * The COST, stated because it is real: on a suppressed refresh a client's mirror keeps
 * the `observedAt` it was last told about while the durable row moves ahead, so a
 * mirror can render a repository as staler than it is until the next genuine change
 * republishes the whole record. That is the right side to err on — an unbounded log and
 * a broadcast storm are worse than a conservative staleness reading, and the reading is
 * conservative in the safe direction (too old, never too fresh). A refresh trigger
 * (DE4.4) that wants the stamp fanned out regardless will need a delta that carries the
 * observation time as its own event rather than a whole-record change; that is recorded
 * against DE4.4 in `docs/plan/domain-remodel.md`.
 */
const putRepositoryAndJournal = (
  base: Store,
  feed: Feed,
  repository: Repository,
): Effect.Effect<void, StateStoreError> => {
  const event: WorkGraphEvent = { _tag: "RepositoryChanged", repository };
  return base
    .withTransaction(
      base.repositories
        .getRepository(repository.id)
        .pipe(
          Effect.flatMap((stored) =>
            base.repositories
              .putRepository(repository)
              .pipe(
                Effect.andThen(() =>
                  Option.isSome(stored) && observationsAgree(stored.value, repository)
                    ? Effect.succeedNone
                    : journalDelta(base, event).pipe(Effect.map((offset) => Option.some(offset))),
                ),
              ),
          ),
        ),
    )
    .pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (offset) => feed.publish({ offset, event }),
        }),
      ),
    );
};

/**
 * Persist one durable, transcript-grade {@link SessionEvent} to the session's durable
 * transcript log (minting its per-session offset), THEN fan it out live on the
 * {@link SessionEvents} feed stamped with that same offset. The session log's `append` is a
 * single atomic write (no separate node write to bind), so there is no transaction to wrap;
 * the live publish happens AFTER the durable append commits, so the live tail and the
 * durable replay share ONE per-session coordinate space — the property the `sinceOffset`
 * reconnect resume relies on. Mirrors {@link putAndJournal} for the session channel.
 */
const appendAndPublish =
  (base: Store, sessionFeed: SessionFeed) =>
  (
    sessionId: SessionId,
    event: SessionEvent,
  ): Effect.Effect<PersistedSessionEvent, StateStoreError> =>
    base.sessionLog
      .append(sessionId, event)
      .pipe(
        Effect.tap((persisted) =>
          sessionFeed.publish({ sessionId, offset: persisted.offset, event }),
        ),
      );

/**
 * Fan one EPHEMERAL live session event out on the {@link SessionEvents} feed OFFSET-LESS,
 * WITHOUT persisting it — the live-only half of the session channel's dual modality.
 * Where {@link appendAndPublish} persists a durable entry and publishes it stamped with
 * its minted offset, this publishes with NO offset (the item omits the key), so the event
 * reaches every live subscriber to drive the reactive flow but never joins the durable
 * transcript and never advances the `sinceOffset` reconnect cursor. Total (it cannot fail).
 */
const publishEphemeralLive =
  (sessionFeed: SessionFeed) =>
  (sessionId: SessionId, event: SessionEvent): Effect.Effect<void> =>
    sessionFeed.publish({ sessionId, event });

/**
 * Build the journaling store shape: delegate to `base`, then for each `put*` journal
 * its delta durably AND publish it live stamped with the durable offset, and for each
 * durable session-transcript `append` publish it live on the {@link SessionEvents} feed
 * stamped with its per-session offset.
 */
const journaling = (base: Store, feed: Feed, sessionFeed: SessionFeed): Store =>
  StateStore.of({
    // The REGISTRY layer journals through the SAME seam as the work graph: an
    // append is durable-plus-live in one transaction, fanned out as `AgentChanged`.
    // Only the append needs decorating — the registry exposes no delete, so there is
    // no removal delta to journal (and none on the contract either). It takes its own
    // helper because it is the one write on the port that can legitimately do
    // NOTHING: an idempotent re-append must not journal a second delta.
    agents: {
      putAgent: (agent) => putAgentAndJournal(base, feed, agent),
      getAgent: base.agents.getAgent,
      listAgents: base.agents.listAgents,
    },
    // The STATE layer journals through the SAME seam as the work graph: a repository
    // observation is durable-plus-live in one transaction, fanned out as
    // `RepositoryChanged`, and there is no delete anywhere to journal. It takes its own
    // helper for the same reason the append-only registry does: a put here always
    // WRITES (a refresh REPLACES the record wholesale under a new `observedAt`, D7),
    // but a re-observation that changed nothing is still a NO-OP DELTA — the two
    // records agree about everything except when we looked — and journaling it would
    // grow the append-only log and re-broadcast on every retry. See
    // `putRepositoryAndJournal`.
    repositories: {
      putRepository: (repository) => putRepositoryAndJournal(base, feed, repository),
      getRepository: base.repositories.getRepository,
      findRepository: base.repositories.findRepository,
      listRepositories: base.repositories.listRepositories,
    },
    workGraph: {
      putWorkstream: (workstream) =>
        putAndJournal(base, feed, base.workGraph.putWorkstream(workstream), {
          _tag: "WorkstreamChanged",
          workstream,
        }),
      putEpic: (epic) =>
        putAndJournal(base, feed, base.workGraph.putEpic(epic), { _tag: "EpicChanged", epic }),
      putIssue: (issue) =>
        putAndJournal(base, feed, base.workGraph.putIssue(issue), { _tag: "IssueChanged", issue }),
      getWorkstream: base.workGraph.getWorkstream,
      getEpic: base.workGraph.getEpic,
      getIssue: base.workGraph.getIssue,
      listWorkstreams: base.workGraph.listWorkstreams,
      listEpics: base.workGraph.listEpics,
      listIssues: base.workGraph.listIssues,
    },
    jobs: {
      putJob: (job) =>
        putAndJournal(base, feed, base.jobs.putJob(job), { _tag: "JobChanged", job }),
      getJob: base.jobs.getJob,
      listJobsForIssue: base.jobs.listJobsForIssue,
      putSession: (session) =>
        putAndJournal(base, feed, base.jobs.putSession(session), {
          _tag: "SessionChanged",
          session,
        }),
      getSession: base.jobs.getSession,
      getSessionForJob: base.jobs.getSessionForJob,
    },
    events: base.events,
    sessionLog: {
      append: appendAndPublish(base, sessionFeed),
      publishEphemeral: publishEphemeralLive(sessionFeed),
      read: base.sessionLog.read,
      tail: base.sessionLog.tail,
      // Pure reads — no live fan-out — so they delegate straight to the base store.
      countEntries: base.sessionLog.countEntries,
      maxOffset: base.sessionLog.maxOffset,
    },
    // The generation is a property of the DURABLE store, not of journaling: this
    // decorator adds a live fan-out over the same tables, so it reports the base
    // store's identity unchanged (a decorator that minted its own would make the
    // offsets it publishes belong to a generation no cursor could ever match).
    generation: base.generation,
    withTransaction: base.withTransaction,
  });

/**
 * Decorate `baseLayer` so every persisted mutation is journaled to the durable
 * {@link EventLogStore} AND fanned out live on the {@link WorkGraphEvents} feed with
 * its durable offset. The base store is wired in via `Layer.provide`, so consumers
 * see only the decorated port (INV-PORT); the feed stays in the requirements (the
 * composition root provides it). Because the layer that mints the offset is also the
 * one that publishes, the live and replay offsets are identical by construction —
 * this decorator is the daemon's SINGLE reactive-plus-durable seam.
 */
export const layerJournaling = <RIn>(
  baseLayer: Layer.Layer<StateStore, StateStoreError, RIn>,
): Layer.Layer<StateStore, StateStoreError, RIn | WorkGraphEvents | SessionEvents> =>
  Layer.effect(
    StateStore,
    Effect.gen(function* () {
      const base = yield* StateStore;
      const feed = yield* WorkGraphEvents;
      const sessionFeed = yield* SessionEvents;
      return journaling(base, feed, sessionFeed);
    }),
  ).pipe(Layer.provide(baseLayer));

// ── offset-based resync ───────────────────────────────────────────────────────

/**
 * Decode one durable entry back to its owned {@link OffsetEvent} — the delta paired
 * with the entry's durable `offset`. An entry that does not decode as a work-graph
 * delta (a foreign `kind` a future producer may append) is skipped, not a failure —
 * the resync replays only work-graph history.
 */
const decodeDelta = (entry: PersistedEvent): Effect.Effect<Option.Option<OffsetEvent>> =>
  Schema.decodeUnknownEffect(WorkGraphEvent)(entry.payload).pipe(
    Effect.map((event) => Option.some<OffsetEvent>({ offset: entry.offset, event })),
    Effect.catch(() => Effect.succeedNone),
  );

/**
 * The durable replay stream: every persisted {@link OffsetEvent} with an offset
 * strictly greater than `offset`, in offset order — the AE1 {@link EventLogStore.tail}
 * primitive a client resumes from its last-seen cursor. This durable replay is the
 * strictly-ordered, `> offset` slice the `sinceOffset` reconnect resume relies on;
 * where {@link resyncEvents} hands over to the live tail, an event committed at the
 * boundary can also arrive on the eager live subscription, so the two feeds can
 * overlap (a repeated offset) — harmless under upsert idempotency. A store read
 * failure is a defect (`orDie`) rather than a stream error, keeping the feed's error
 * channel empty (matching the frozen contract's `events` success/never shape).
 */
export const resyncFrom = (store: Store, offset: number): Stream.Stream<OffsetEvent> =>
  decodedReplay(store.events.tail(offset).pipe(Effect.orDie));

/** Turn a read of durable entries into the stream of the owned deltas they decode to. */
const decodedReplay = (
  entries: Effect.Effect<ReadonlyArray<PersistedEvent>>,
): Stream.Stream<OffsetEvent> =>
  Stream.unwrap(
    entries.pipe(
      Effect.flatMap((read) => Effect.forEach(read, decodeDelta)),
      Effect.map((decoded) => Stream.fromIterable(Arr.getSomes(decoded))),
    ),
  );

/**
 * Decide whether `sinceOffset` belongs to THIS store generation, failing with the
 * contract's {@link ResyncRequired} when it does not.
 *
 * `EventLogStore.tail` is a strict `> offset` slice, which is exactly right while the
 * log's offset space is monotonic — but that space is not eternal. Bumping
 * `SCHEMA_VERSION` DROPS and recreates `event_log` (INV-FRESH never migrates), and a
 * recreated `AUTOINCREMENT` table restarts at offset 1. A client holding a durable
 * cursor from the previous generation is then holding a coordinate in a space that no
 * longer exists — AND a retained snapshot full of entities the reset destroyed, which
 * the upsert-only delta model can never remove (there is no `*Removed` variant).
 *
 * So the daemon does not try to repair it silently — neither by honouring the cursor
 * (the client goes quiet until the new log outgrows the stale mark) nor by replaying
 * from the origin behind the client's back (the client's contiguous cursor is still
 * stale, so it discards the whole replay AND every subsequent live event, and re-reads
 * the log on every reconnect — unbounded and undetectable).
 *
 * The generation is therefore EXPLICIT — an IDENTITY the request carries, not a
 * property inferred from the numbers. That distinction is the whole point. An extent
 * check (`sinceOffset > maxOffset`) is SUFFICIENT to catch a stale cursor but never
 * NECESSARY: the moment a new generation's log grows past the stale mark, the stale
 * cursor is `<= maxOffset` and an extent check sees nothing at all — the daemon would
 * resume the client incrementally against a coordinate space its cursor never belonged
 * to, silently and undetectably from either side.
 *
 * So the cursor and its generation arrive as ONE inseparable {@link ResumeContext},
 * and this refuses:
 *
 * - a resume whose generation DIFFERS from `store.generation` — the store was dropped
 *   and recreated under the client. Checked UNCONDITIONALLY, at every offset;
 * - a resume beyond {@link EventLogStore.maxOffset} — kept as a cheap SECONDARY check,
 *   since a cursor ahead of the log is impossible even within one generation (a client
 *   that mangled its own cursor, or a replayed request against a truncated log).
 *
 * An ABSENT `resume` is the ORIGIN request: it names no coordinate, so it is valid
 * against every generation — including an empty log — and never trips this. That
 * ABSENCE is the ONLY exemption, and it is the one the type system already states.
 *
 * There is deliberately no numeric exemption. `sinceOffset === 0` used to be treated
 * as "the origin" and skip the generation comparison entirely, which made the guard
 * bypassable by a request carrying a DEAD generation with a zero cursor — a shape a
 * real client reaches whenever its contiguous prefix never advanced past `0` during an
 * attempt (an out-of-order first delta is enough). Now `resume: { sinceOffset: 0, … }`
 * is a RESUME like any other and its generation is compared like any other; only the
 * absence of the whole value means origin. A cursor with no generation is not rejected
 * here because it cannot be built (INV-SUM).
 */
const requireLiveCursor = (
  store: Store,
  resume: ResumeContext | undefined,
): Effect.Effect<void, ResyncRequired> =>
  resume === undefined
    ? Effect.void
    : store.events.maxOffset.pipe(
        Effect.orDie,
        Effect.flatMap((maxOffset) =>
          resume.generation === store.generation && resume.sinceOffset <= maxOffset
            ? Effect.void
            : Effect.fail(
                new ResyncRequired({
                  sinceOffset: resume.sinceOffset,
                  maxOffset,
                  generation: store.generation,
                }),
              ),
        ),
      );

/**
 * The `events` RPC feed: VALIDATE the resume cursor against this store generation,
 * EAGERLY subscribe to the live work-graph feed, THEN replay the durable log strictly
 * after `sinceOffset` (the client's resume cursor — CE2.0; absent → `0`, the origin),
 * THEN stream the live tail. Every
 * streamed item is an {@link OffsetEvent} carrying its durable offset; replay and
 * live offsets share one coordinate space (the journaling decorator mints and
 * publishes the same offset it appended), so a reconnecting request with
 * `sinceOffset = N` replays only offsets `> N` and never re-delivers what the client
 * already acknowledged — the strict guarantee is scoped to that RESUME. WITHIN a
 * single stream the guarantee is looser: subscribing before the durable read closes
 * the replay/live gap, but an event committed at the boundary can appear in BOTH the
 * replay and the buffered live subscription, so an offset can repeat and momentarily
 * go backwards there; and because the live publish happens after the durable commit,
 * concurrent writers can make the live order not strictly monotonic. All of it is
 * absorbed by upsert-idempotent deltas, so the boundary overlap is harmless. The
 * result is a deterministic catch-up from durable history, not merely
 * snapshot-on-connect (D17).
 *
 * The cursor check runs BEFORE the subscription, so a {@link ResyncRequired} request
 * fails without ever attaching to the live feed. It is the stream's ONLY error: a
 * store read failure stays a defect (`orDie`), so the error channel says exactly one
 * thing — "your cursor is from a dead store generation; re-hydrate".
 */
export const resyncEvents = (
  store: Store,
  feed: Feed,
  resume?: ResumeContext,
): Stream.Stream<OffsetEvent, ResyncRequired> =>
  Stream.unwrap(
    requireLiveCursor(store, resume).pipe(
      Effect.andThen(feed.subscribe),
      Effect.map((subscription) =>
        Stream.concat(
          resyncFrom(store, resume?.sinceOffset ?? 0),
          Stream.fromSubscription(subscription),
        ),
      ),
    ),
  );

// ── session-transcript durable replay ─────────────────────────────────────────

/**
 * The session-transcript durable replay stream: every persisted {@link OffsetSessionEvent}
 * for `sessionId` with an offset strictly greater than `offset`, in offset order — the
 * per-session {@link SessionLogStore.tail} primitive a `sessionEvents` client resumes from
 * its last-seen cursor. The session-channel mirror of {@link resyncFrom}.
 *
 * A persisted transcript entry already IS an offset-paired owned value
 * ({@link PersistedSessionEvent} is structurally an {@link OffsetSessionEvent}), so no
 * per-entry decode is needed — unlike the work-graph log's open `kind`/`payload` envelope,
 * the transcript log is typed to the owned `SessionEvent`. A store read failure is a defect
 * (`orDie`) rather than a stream error, keeping the feed's error channel exactly the
 * contract's `SessionNotFound` (raised only by the liveness gate, never the durable read).
 */
export const resyncSessionFrom = (
  store: Store,
  sessionId: SessionId,
  offset: number,
): Stream.Stream<OffsetSessionEvent> =>
  Stream.unwrap(
    store.sessionLog.tail(sessionId, offset).pipe(
      Effect.orDie,
      Effect.map((entries) =>
        Stream.fromIterable(
          entries.map(
            (entry): OffsetSessionEvent => ({ offset: entry.offset, event: entry.event }),
          ),
        ),
      ),
    ),
  );
