/**
 * `StateStore` — the persistence-agnostic durable spine of the daemon (Track A,
 * task AE2.1), expressed as an Effect `Context.Service` (the PORT).
 *
 * This module is the SEAM the core and every consumer depend on (D5/D14,
 * INV-PORT): it describes WHAT the daemon persists, never HOW. It references no
 * backing store, no SQL, and no running instance — a concrete backing (the
 * SQLite adapter in {@link ./sqlite.ts}) is a `Layer` provided behind this port,
 * and NOTHING outside that adapter module may import `@effect/sql-sqlite-bun`,
 * `effect/unstable/sql`, `bun:sqlite`, or SQL strings.
 *
 * The port is composed of three capability groups (INV-NAMING, `*Store` suffix):
 *
 * - {@link WorkGraphStore} — the work graph `Workstream ⊃ Epic ⊃ Issue` plus the
 *   dependency DAG (`Issue.dependsOn` edges); upserts, reads, and the child lists
 *   that status roll-up (D13) is computed from.
 * - {@link JobStore} — the `Job` model and the `Issue → Job → session → PR`
 *   mapping (1 Job = 1 session = 1 transcript = 1 PR), plus {@link Session}.
 * - {@link EventLogStore} — an append-only durable event feed (append / ordered
 *   read / tail-from-offset): the durable event record distinct from the reactive
 *   in-memory stream, supporting D17 reconciliation and AE4's snapshot.
 *
 * Every group reuses the OWNED domain schemas from `@sprinter/domain`
 * (`read-model.ts` / `ids.ts`) — they are not redefined here. All failures are
 * the owned {@link StateStoreError}; no backing-specific error (e.g. a SQL error)
 * ever crosses the port.
 */
import { Context, Effect, Option, Schema } from "effect";
import {
  type Epic,
  EpicId,
  type Issue,
  IssueId,
  type Job,
  JobId,
  NonNegativeInt,
  type Session,
  SessionEvent,
  SessionId,
  type Workstream,
  WorkstreamId,
} from "@sprinter/domain";

// ============================================================================
// Errors
// ============================================================================

/**
 * The single owned failure raised by every {@link StateStore} operation
 * (INV-NAMING, `*Error` via `Schema.TaggedErrorClass`). `operation` names the
 * store method that failed; `detail` carries a neutral, human-readable cause.
 *
 * This is the ONLY error the port exposes: an adapter translates its
 * backing-specific failures (SQL errors, schema-decode failures, migration
 * failures) into this type at the boundary, so no consumer ever depends on a
 * concrete backing's error shape (INV-PORT).
 */
export class StateStoreError extends Schema.TaggedErrorClass<StateStoreError>()("StateStoreError", {
  /** The store operation that failed, e.g. `"putIssue"` or `"append"`. */
  operation: Schema.String,
  /** A neutral, human-readable description of the cause. */
  detail: Schema.String,
}) {}

// ============================================================================
// Event feed
// ============================================================================

/**
 * An event to append to the durable feed. The feed is a minimal, open envelope
 * (D6): `kind` is a producer-owned discriminator and `payload` is arbitrary,
 * JSON-serialisable data whose shape the producer owns and narrows. The store is
 * agnostic to what an event means — it persists and orders them.
 */
export const AppendEvent = Schema.Struct({
  /** A producer-owned discriminator for the event, e.g. `"IssueStatusChanged"`. */
  kind: Schema.NonEmptyString,
  /** Arbitrary JSON-serialisable payload; the producer owns and narrows its shape. */
  payload: Schema.Unknown,
});
export type AppendEvent = (typeof AppendEvent)["Type"];

/**
 * A persisted feed entry: an {@link AppendEvent} stamped with its monotonic
 * `offset`. The `offset` is a stable, gap-free-per-append cursor used to read the
 * feed in order and to tail it from a known position (D17 reconciliation).
 */
export const PersistedEvent = Schema.Struct({
  /** The event's monotonic position in the feed; the tail cursor. */
  offset: NonNegativeInt,
  /** The producer-owned discriminator, echoed from the appended event. */
  kind: Schema.NonEmptyString,
  /** The event payload, echoed from the appended event. */
  payload: Schema.Unknown,
});
export type PersistedEvent = (typeof PersistedEvent)["Type"];

/**
 * A persisted DURABLE session-transcript entry: an owned, transcript-grade
 * {@link SessionEvent} stamped with its monotonic per-session `offset` — the
 * session-channel analogue of {@link PersistedEvent}. The `offset` is the durable
 * cursor a consumer reads the session's transcript in order by and tails from a known
 * position (the `sessionEvents` durable replay). Unlike the open
 * {@link PersistedEvent} envelope, the transcript log is TYPED to the owned
 * `SessionEvent` — its only producer is the session fold, so it persists the owned value
 * directly rather than an opaque `kind`/`payload` pair.
 */
export const PersistedSessionEvent = Schema.Struct({
  /** The entry's monotonic position in the session's transcript; the tail cursor. */
  offset: NonNegativeInt,
  /** The durable, transcript-grade session event persisted at this offset. */
  event: SessionEvent,
});
export type PersistedSessionEvent = (typeof PersistedSessionEvent)["Type"];

// ============================================================================
// Capability groups
// ============================================================================

/**
 * Persist and read the work graph `Workstream ⊃ Epic ⊃ Issue` and its dependency
 * DAG. `put*` upserts a node by id (idempotent); the reads hydrate the owned
 * domain schemas back out, including reconstructing each issue's `dependsOn`
 * edges. `listEpics` / `listIssues` read a node's children so a consumer can roll
 * status up the hierarchy (D13).
 *
 * Parentage is stored twice — a parent's child list (`Workstream.epics`,
 * `Epic.issues`) AND each child's parent reference (`Epic.workstreamId`,
 * `Issue.epicId`). `getWorkstream`/`getEpic` echo the stored child list, while
 * `listEpics`/`listIssues` read by the parent reference. Keeping the two consistent
 * on upsert is the CALLER's responsibility: when adding a child, upsert both the
 * child (with its parent reference) and the parent (with the child in its list).
 */
export interface WorkGraphStore {
  /** Upsert a {@link Workstream} (including its `epics` child list). */
  readonly putWorkstream: (workstream: Workstream) => Effect.Effect<void, StateStoreError>;
  /** Upsert an {@link Epic} (including its `issues` child list). */
  readonly putEpic: (epic: Epic) => Effect.Effect<void, StateStoreError>;
  /** Upsert an {@link Issue}, replacing its `dependsOn` dependency edges. */
  readonly putIssue: (issue: Issue) => Effect.Effect<void, StateStoreError>;
  /** Read a {@link Workstream} by id, if present. */
  readonly getWorkstream: (
    id: WorkstreamId,
  ) => Effect.Effect<Option.Option<Workstream>, StateStoreError>;
  /** Read an {@link Epic} by id, if present. */
  readonly getEpic: (id: EpicId) => Effect.Effect<Option.Option<Epic>, StateStoreError>;
  /** Read an {@link Issue} by id (with its reconstructed `dependsOn`), if present. */
  readonly getIssue: (id: IssueId) => Effect.Effect<Option.Option<Issue>, StateStoreError>;
  /** All persisted workstreams, ordered by id. */
  readonly listWorkstreams: Effect.Effect<ReadonlyArray<Workstream>, StateStoreError>;
  /** The epics of a workstream, ordered by id. */
  readonly listEpics: (
    workstreamId: WorkstreamId,
  ) => Effect.Effect<ReadonlyArray<Epic>, StateStoreError>;
  /** The issues of an epic, ordered by issue number. */
  readonly listIssues: (epicId: EpicId) => Effect.Effect<ReadonlyArray<Issue>, StateStoreError>;
}

/**
 * Persist and read the {@link Job} model and the `Issue → Job → session → PR`
 * mapping (1 Job = 1 session = 1 transcript = 1 PR). `putJob` carries a job's
 * optional `sessionId` / `transcriptRef` / `pr`; {@link Session} rows are stored
 * alongside so a job can be resolved to its running session (and back).
 */
export interface JobStore {
  /** Upsert a {@link Job} (with its optional session / transcript / PR links). */
  readonly putJob: (job: Job) => Effect.Effect<void, StateStoreError>;
  /** Read a {@link Job} by id, if present. */
  readonly getJob: (id: JobId) => Effect.Effect<Option.Option<Job>, StateStoreError>;
  /** All jobs advancing a given issue (the `Issue → Job` mapping), ordered by id. */
  readonly listJobsForIssue: (
    issueId: IssueId,
  ) => Effect.Effect<ReadonlyArray<Job>, StateStoreError>;
  /**
   * Upsert a {@link Session} (by session id). At most one session may exist per
   * job (1 Job = 1 session); attaching a second, distinct session to a job that
   * already has one fails with {@link StateStoreError}. A restart re-attaches by
   * upserting the SAME session id, not a new one.
   */
  readonly putSession: (session: Session) => Effect.Effect<void, StateStoreError>;
  /** Read a {@link Session} by id, if present. */
  readonly getSession: (id: SessionId) => Effect.Effect<Option.Option<Session>, StateStoreError>;
  /** Read the session executing a given job (the `Job → session` mapping), if present. */
  readonly getSessionForJob: (
    jobId: JobId,
  ) => Effect.Effect<Option.Option<Session>, StateStoreError>;
}

/**
 * The append-only durable event feed. `append` stamps an event with the next
 * monotonic `offset` and returns the persisted entry; `read` returns the whole
 * feed in order; `tail` returns every entry strictly after a given offset — the
 * primitive a consumer uses to resume reading where it left off (D17).
 *
 * `tail(offset)` is the INCREMENTAL primitive and the one to reach for on a live
 * or large feed; `read` materialises the ENTIRE feed in memory and is intended for
 * bounded snapshot use (e.g. AE4's snapshot-on-connect), not for streaming an
 * unbounded log.
 */
export interface EventLogStore {
  /** Append an event, returning it stamped with its assigned {@link PersistedEvent.offset}. */
  readonly append: (event: AppendEvent) => Effect.Effect<PersistedEvent, StateStoreError>;
  /** The entire feed in memory, ordered by ascending offset — bounded snapshot use (see above). */
  readonly read: Effect.Effect<ReadonlyArray<PersistedEvent>, StateStoreError>;
  /** Every entry with an offset strictly greater than `offset`, ordered ascending. */
  readonly tail: (offset: number) => Effect.Effect<ReadonlyArray<PersistedEvent>, StateStoreError>;
}

/**
 * The append-only durable SESSION-TRANSCRIPT log, keyed per session (1 Job = 1 session
 * = 1 transcript). It is the session-channel analogue of {@link EventLogStore}: `append`
 * stamps a session's next transcript entry with a monotonic `offset` and returns the
 * persisted entry; `read` returns a session's whole transcript in order; `tail` returns a
 * session's entries strictly after a given offset — the primitive the `sessionEvents`
 * durable replay resumes from. It makes a SETTLED session's transcript
 * viewable: the entries persist independently of any live handle, so they replay after the
 * session ends.
 *
 * Only DURABLE, transcript-grade {@link SessionEvent}s are APPENDED here (the
 * `EntryAppended` records and reconcilable `Notice`s the transcript folds), never the
 * ephemeral streaming deltas. Offsets are monotonic per session; a re-dispatch of the same
 * session id APPENDS (never resets the sequence), so the offset sequence is never
 * duplicated or corrupted.
 *
 * The EPHEMERAL live deltas (turn lifecycle, message/tool partials, `UiRequestRaised`, …)
 * are NOT persisted — they take {@link publishEphemeral}, the offset-less live-only path that
 * lets the session fold tee its WHOLE reactive flow to the reactive feed without bloating the
 * durable transcript. The base store no-ops it (it owns durability, not the
 * live feed); the daemon's journaling decorator overrides it to fan the delta out on the
 * `SessionEvents` feed offset-less, exactly as its `append` override fans a durable entry out
 * offset-stamped.
 */
export interface SessionLogStore {
  /**
   * Append one durable transcript entry for `sessionId`, returning it stamped with its
   * assigned {@link PersistedSessionEvent.offset}.
   */
  readonly append: (
    sessionId: SessionId,
    event: SessionEvent,
  ) => Effect.Effect<PersistedSessionEvent, StateStoreError>;
  /**
   * Fan out one EPHEMERAL, non-durable live session event WITHOUT persisting it — the
   * offset-less live-delta path. Total (it cannot fail): the base store no-ops
   * it, and the decorator's feed publish cannot fail. It NEVER mints an offset and NEVER
   * touches the durable transcript, so it does not perturb the `sinceOffset` reconnect resume.
   */
  readonly publishEphemeral: (sessionId: SessionId, event: SessionEvent) => Effect.Effect<void>;
  /** A session's entire transcript in memory, ordered by ascending offset. */
  readonly read: (
    sessionId: SessionId,
  ) => Effect.Effect<ReadonlyArray<PersistedSessionEvent>, StateStoreError>;
  /** A session's entries with an offset strictly greater than `offset`, ordered ascending. */
  readonly tail: (
    sessionId: SessionId,
    offset: number,
  ) => Effect.Effect<ReadonlyArray<PersistedSessionEvent>, StateStoreError>;
  /**
   * The count of DURABLE `EntryAppended` records in a session's transcript — computed in the
   * store WITHOUT materializing or decoding the transcript rows (cheap for a long / retried
   * session, unlike counting over {@link read}). Counts the whole merged log (a re-dispatch
   * APPENDS), so it matches the transcript the Inspector renders.
   */
  readonly countEntries: (sessionId: SessionId) => Effect.Effect<number, StateStoreError>;
}

// ============================================================================
// Port
// ============================================================================

/**
 * The persistence-agnostic `StateStore` port — the durable spine composed of the
 * three capability groups. The core and every consumer (AE3 Job runner, AE4
 * `RpcServer` snapshot) depend on THIS service; a backing is chosen by providing
 * one of its adapter `Layer`s (the SQLite adapter in {@link ./sqlite.ts}). The
 * tag id follows INV-NAMING: `sprinter/<area>/<Name>`.
 */
export class StateStore extends Context.Service<
  StateStore,
  {
    /** The work-graph capability group. */
    readonly workGraph: WorkGraphStore;
    /** The Job / Session / mapping capability group. */
    readonly jobs: JobStore;
    /** The durable event-feed capability group. */
    readonly events: EventLogStore;
    /** The durable per-session transcript-log capability group. */
    readonly sessionLog: SessionLogStore;
    /**
     * Run `effect` in a SINGLE durable transaction: every `put*`/`append` write
     * it performs commits together or rolls back together (nested transactions
     * fold into the outer one). This is the atomicity primitive a decorator uses
     * to keep a node write and its offset-log delta gap-free under a crash (D17 /
     * INV-RESTART) — without it, the two are separate commits and a crash between
     * them leaves the node persisted but the event feed missing its delta.
     *
     * The adapter maps its backing transaction failure to the owned
     * {@link StateStoreError}; the combinator adds no error beyond that (INV-PORT).
     */
    readonly withTransaction: <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | StateStoreError, R>;
  }
>()("sprinter/state/StateStore") {}
