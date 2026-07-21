/**
 * `StateStore` — the persistence-agnostic durable spine of the daemon (Track A,
 * task AE2.1), expressed as an Effect `Context.Service` (the PORT).
 *
 * This module is the SEAM the core and every consumer depend on (D5/D14,
 * INV-PORT): it describes WHAT the daemon persists, never HOW. It references no
 * backing store, no SQL, and no running instance — a concrete backing (the
 * SQLite adapter in {@link ./sqlite.ts}) is a `Layer` provided behind this port,
 * and NOTHING outside that adapter module may import `@effect/sql-sqlite-bun`,
 * `effect/unstable/sql`, `bun:sqlite`, or SQL strings. (The adapter's own
 * white-box tests, which live beside it in this package, may probe the raw
 * database to assert the schema-version reset — no CONSUMER ever can.)
 *
 * The port is composed of these capability groups (INV-NAMING, `*Store` suffix):
 *
 * - {@link AgentStore} — the append-only `Agent` REGISTRY: global, scoped to no
 *   repository, exposing append + read and NO delete.
 * - {@link RepositoryStore} — the `── STATE ──` layer's OBSERVED `Repository`
 *   entity: put + read, no delete, and the anchor `Workstream.repositoryId`
 *   references.
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
  type Agent,
  AgentId,
  type Epic,
  EpicId,
  type Issue,
  IssueId,
  type Job,
  JobId,
  NonNegativeInt,
  type Repository,
  RepositoryId,
  type RepositoryKey,
  type Session,
  SessionEvent,
  SessionId,
  type StoreGenerationId,
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
 * The outcome of an {@link AgentStore.putAgent} append: whether a row was actually
 * written, or the revision was already stored byte-identically.
 *
 * `putAgent` succeeds in both cases — it is idempotent by design — but the two are
 * NOT interchangeable to a decorator of this port. The daemon's journaling layer
 * must emit exactly one `AgentChanged` delta per REAL append: journaling an
 * `"unchanged"` no-op too would make a crash-retry loop grow the durable event log
 * (and fan out redundant deltas) without bound, contradicting the very no-op
 * semantics that make the retry safe. Reporting the outcome is what lets the
 * decorator tell them apart without re-reading the row it just wrote.
 */
export type AgentWrite = "appended" | "unchanged";

/**
 * Persist and read the append-only {@link Agent} REGISTRY — owned, global, scoped
 * to NO repository (there is deliberately no repository- or workstream-scoped read
 * here: "the agents used in this repo" is a FOLD over that repo's executions,
 * never a stored per-repo list — INV-DERIVED).
 *
 * The surface is APPEND + READ, and there is deliberately **no delete** — not
 * here, and not on the contract. A stored revision is IMMUTABLE history: editing
 * appends a NEW revision, under a NEW id, whose `supersedes` names the current head
 * of the lineage; retiring appends one carrying BOTH `supersedes` AND `retiredAt`.
 * Removing — or rewriting — a record would strand every past execution that ran on
 * it.
 *
 * `putAgent` is therefore an APPEND, not an upsert. It is idempotent for a
 * BYTE-IDENTICAL re-append of an id already stored (the crash-retry case: the write
 * is a no-op and succeeds), and it FAILS with {@link StateStoreError} when the id
 * is already stored with DIFFERENT content. It never rewrites a column, so it can
 * neither silently replace a revision's content nor un-retire a retired agent.
 * There is no edit path here at all, because an edit mints a new id.
 *
 * Because "appended" and "already there" are genuinely different events for a
 * DECORATOR of this port — the daemon's journaling layer must emit an
 * `AgentChanged` delta for the first and NOTHING for the second, or a retry loop
 * grows the durable log without bound — `putAgent` REPORTS which happened
 * ({@link AgentWrite}) rather than collapsing both to `void`.
 *
 * Every `supersedes` rule from `registry.ts` is ENFORCED here rather than left to
 * review — none of them is a writer obligation any more:
 *
 * - `supersedes` must name an ALREADY-STORED revision. This is the load-bearing one,
 *   because the three rules below are all decided by READING the superseded row: while
 *   a DANGLING `supersedes` was storable, a writer bypassed all of them for free by
 *   reversing the write order — append the successor first (nothing to check against)
 *   and its predecessor second (an ordinary first revision) — and assembled a
 *   resurrection, a content-rewriting retirement, or a `supersedes` CYCLE out of
 *   appends each of which this port accepted. Enforced by the adapter as a real
 *   FOREIGN KEY, so the shape those attacks are built from does not exist and every
 *   rule below is order-independent.
 * - `supersedes !== id` — a revision cannot supersede itself. This is the ONE edge a
 *   referential constraint accepts (a row satisfies a key against itself), and with
 *   it closed no cycle of any length is constructible: a longer one needs an edge
 *   naming a revision that does not exist yet. Acyclicity is therefore structural,
 *   not a precondition anyone owes.
 * - a RETIRING revision (one carrying BOTH `supersedes` and `retiredAt`) must carry
 *   the SAME `name` / `model` / `version` / `tools` as the revision it supersedes.
 *   Retirement is LIFECYCLE-ONLY; fusing a content edit into it would make the two
 *   operations indistinguishable in the history.
 * - NOTHING may supersede an ALREADY-RETIRED revision — an edit no more than a
 *   retirement. A lineage goes out of service ONCE, and `retiredAt` is its terminal
 *   state. A second RETIRING append would leave two revisions each carrying a
 *   `retiredAt` and no way to say which instant the lineage stopped; a NON-RETIRING
 *   one is worse, because it carries no stamp at all, so `isLineageRetired` walks
 *   forward off the retired revision onto a live head and the lineage reads as back
 *   in service — un-retiring it, which this port's second paragraph promises cannot
 *   happen. Neither is caught by the rule above, which ignores the lifecycle columns
 *   by design, so it is enforced on `supersedes` alone.
 * - a revision may be superseded AT MOST ONCE. A lineage is a CHAIN; two successors
 *   are a fork with no defined head, and a fork makes `isLineageRetired`'s answer
 *   depend on the order of the collection it is handed. Enforced by the adapter's
 *   `agent_supersedes` UNIQUE index rather than by a read-then-insert check, so it
 *   holds atomically against a concurrent racing append.
 *
 * DURABILITY SCOPE: "nothing is ever removed" holds for the life of a STORE
 * GENERATION. `SCHEMA_VERSION` ({@link ./sqlite.ts}) never migrates — bumping it
 * drops the database and starts a new generation, discarding this registry's whole
 * history. `putAgent` is the sole source of truth for its content (there is no
 * manifest to re-seed from), so that is accepted, permanent data loss — see
 * INV-FRESH's rationale in the adapter.
 *
 * GROWTH IS UNBOUNDED, and that same reset is its ONLY bound. Every revision ever
 * appended stays: superseded and retired ones are exactly what the append-only model
 * exists to keep, so there is no pruning, no compaction, no retention window, and no
 * delete anywhere on this path. {@link AgentStore.listAgents} materialises the WHOLE
 * table, the daemon's `snapshot` ships all of it on every connect, and every client
 * retains all of it — the cost of one revision is paid on three surfaces at once,
 * forever, until a `SCHEMA_VERSION` bump discards the generation. That is acceptable
 * at pre-release scale (a registry grows by human edits, not by execution volume) and
 * is RECORDED here rather than solved. If it ever stops being acceptable, the remedy
 * is a bounded READ (a lineage-scoped or head-only list), never a delete — deleting
 * would strand the executions that ran on a revision, the one thing this table
 * promises not to do.
 */
export interface AgentStore {
  /**
   * Append an {@link Agent} revision, reporting whether the row was actually
   * written ({@link AgentWrite}). A byte-identical re-append of an already stored id
   * is an idempotent no-op and answers `"unchanged"`; a DIFFERING re-append of that
   * id fails with {@link StateStoreError}, as does a revision whose `supersedes`
   * names itself or names a revision that is NOT STORED, a RETIRING revision that
   * rewrites the superseded revision's content, and ANY revision superseding an
   * already-retired one. It never rewrites or removes a stored revision.
   */
  readonly putAgent: (agent: Agent) => Effect.Effect<AgentWrite, StateStoreError>;
  /** Read one {@link Agent} revision by id, if present. */
  readonly getAgent: (id: AgentId) => Effect.Effect<Option.Option<Agent>, StateStoreError>;
  /**
   * Every persisted {@link Agent} revision — retired and superseded included —
   * ordered by `id` under SQLite's default `BINARY` collation, i.e. by the BYTE
   * sequence of the id (a `TEXT` column with no `COLLATE` clause compares
   * `memcmp`-style over UTF-8 bytes). It is deliberately NOT a locale-aware or
   * human "alphabetical" order: `"agt-10"` precedes `"agt-2"`, and any non-ASCII id
   * orders by its UTF-8 encoding.
   *
   * That byte-order is the PINNED contract of this read, but it is PRESENTATIONAL,
   * not semantic: it is neither insertion order nor lineage order, and consumers
   * upsert by id, so nothing may depend on it to reconstruct a lineage (walk
   * `supersedes` for that, or fold with `isLineageRetired`).
   */
  readonly listAgents: Effect.Effect<ReadonlyArray<Agent>, StateStoreError>;
}

/**
 * Persist and read the `── STATE ──` layer's {@link Repository} entity — a repository
 * as OBSERVED on a code host (DE1.2), and the anchor `Workstream.repositoryId`
 * references.
 *
 * The surface is PUT + READ. There is deliberately **no delete**: the only removal a
 * repository record ever undergoes is the `ON DELETE CASCADE` that clears its observed
 * refs when the row itself goes, and the row itself goes only when the whole store
 * generation does (INV-FRESH). Exposing a delete would let a caller strand every
 * workstream anchored to a repository — which the `workstream.repositoryId` FOREIGN
 * KEY would refuse anyway, so the operation could never succeed on a repository that
 * mattered.
 *
 * `putRepository` REPLACES the record WHOLESALE (D7): the scalar columns AND the whole
 * observed ref set, under the incoming `observedAt`. It is not a merge, and that is the
 * point — a refresh is a NEW OBSERVATION of the same repository, so a branch deleted
 * upstream must DISAPPEAR from `refs` rather than linger from an older read. A record
 * therefore always describes ONE coherent moment.
 *
 * It upserts on the {@link Repository.id}, while the store also holds the NATURAL key
 * `(host, owner, name)` UNIQUE. Both are load-bearing and they are not redundant: the
 * id is what other rows reference (so it must be stable across refreshes), and the
 * triple is what actually identifies a repository BY NAME (so two records disagreeing
 * about one repository must be unconstructible — INV-ENFORCE).
 *
 * The two can DISAGREE, and handling that is part of this contract: the natural key is
 * MUTABLE (a rename, a transfer) while the id an adapter mints is derived from the
 * host's own stable identifier and is not (see {@link RepositoryId}). So a refresh
 * after a rename arrives with the SAME id and a DIFFERENT triple, and the id-keyed
 * upsert must UPDATE the existing row's `host`/`owner`/`name` in place rather than
 * insert a second one. The `UNIQUE (host, owner, name)` index does not stand in the way:
 * the old triple leaves the table in the same statement that writes the new one. A
 * rename therefore keeps ONE row and every reference to it valid.
 *
 * Reads NEVER gate on staleness (D7). Nothing here refuses to return a repository
 * because its `observedAt` is old — staleness is RENDERED from that stamp (DE4.4), and
 * a store that hid stale records would make "how old is this?" unanswerable by removing
 * the evidence.
 *
 * KNOWN GAP (DE1.2): nothing in production ever calls `putRepository` a SECOND time for
 * a given repository — new-plan materialisation is its only caller, so no record is ever
 * refreshed and `observedAt`/`refs` freeze at first sighting. The refresh MECHANISM here
 * is complete and tested; the TRIGGER does not exist and is out of scope for this task.
 * DE4.4's staleness rendering needs one. See the module docstring of
 * `packages/domain/src/repository.ts`.
 *
 * That gap also bites the rename path above, which only holds when the refresh carries
 * the SAME id: a never-refreshed record keeps holding a natural key the host has already
 * freed, so a DIFFERENT repository renamed INTO that key hits the failure documented on
 * `putRepository` below and stays unstorable until something refreshes the stale record.
 * A trigger is what resolves it; the current behaviour is pinned by a test rather than
 * worked around here.
 */
export interface RepositoryStore {
  /**
   * Upsert a {@link Repository} observation by id, REPLACING its scalar columns —
   * INCLUDING the natural key, so a RENAME moves the existing row rather than adding
   * one — and its ENTIRE observed ref set (D7). Fails with {@link StateStoreError} when
   * the record's natural key `(host, owner, name)` is already held by a DIFFERENT id —
   * the backing's UNIQUE constraint, not a read-then-check.
   */
  readonly putRepository: (repository: Repository) => Effect.Effect<void, StateStoreError>;
  /** Read one {@link Repository} (with its observed refs, ordered by name) by id, if present. */
  readonly getRepository: (
    id: RepositoryId,
  ) => Effect.Effect<Option.Option<Repository>, StateStoreError>;
  /**
   * Read one {@link Repository} by its NATURAL key `(host, owner, name)`, if present —
   * the read a caller holding no id has (a plan naming `github / callajd / sprinter`).
   * At most one row can match: the triple is UNIQUE in the backing, so this is
   * deterministic by construction rather than by convention.
   */
  readonly findRepository: (
    key: RepositoryKey,
  ) => Effect.Effect<Option.Option<Repository>, StateStoreError>;
  /** Every stored repository, ordered by id. */
  readonly listRepositories: Effect.Effect<ReadonlyArray<Repository>, StateStoreError>;
}

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
 * unbounded log. `maxOffset` answers "how far does this log go?" WITHOUT
 * materialising it — the cheap read a caller needs to validate a client's resume
 * cursor against this log's extent.
 */
export interface EventLogStore {
  /** Append an event, returning it stamped with its assigned {@link PersistedEvent.offset}. */
  readonly append: (event: AppendEvent) => Effect.Effect<PersistedEvent, StateStoreError>;
  /** The entire feed in memory, ordered by ascending offset — bounded snapshot use (see above). */
  readonly read: Effect.Effect<ReadonlyArray<PersistedEvent>, StateStoreError>;
  /** Every entry with an offset strictly greater than `offset`, ordered ascending. */
  readonly tail: (offset: number) => Effect.Effect<ReadonlyArray<PersistedEvent>, StateStoreError>;
  /**
   * The highest offset this log currently holds, or `0` when it is EMPTY (durable
   * offsets are strictly `> 0`, so `0` is unambiguously "no entries"). The extent of
   * the log, computed by the backing rather than by reading every entry — so a
   * caller validating a resume cursor never has to materialise the feed to do it.
   */
  readonly maxOffset: Effect.Effect<NonNegativeInt, StateStoreError>;
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
  /**
   * The highest offset this session's transcript currently holds (`0` when it has none)
   * — the per-session mirror of {@link EventLogStore.maxOffset}, and for the same
   * reason: the `sessionEvents` feed must validate a client's resume cursor BEFORE it
   * does any work, and reading the whole transcript just to look at its last offset
   * makes a request that is about to be REFUSED the most expensive one the channel
   * serves. Answered from the backing's index, without materializing or decoding a row.
   */
  readonly maxOffset: (sessionId: SessionId) => Effect.Effect<NonNegativeInt, StateStoreError>;
}

// ============================================================================
// Port
// ============================================================================

/**
 * The persistence-agnostic `StateStore` port — the durable spine composed of its
 * capability groups. The core and every consumer (AE3 Job runner, AE4
 * `RpcServer` snapshot) depend on THIS service; a backing is chosen by providing
 * one of its adapter `Layer`s (the SQLite adapter in {@link ./sqlite.ts}). The
 * tag id follows INV-NAMING: `sprinter/<area>/<Name>`.
 */
export class StateStore extends Context.Service<
  StateStore,
  {
    /** The append-only, globally-scoped `Agent` registry capability group. */
    readonly agents: AgentStore;
    /** The observed `Repository` entity capability group — the STATE layer's anchor. */
    readonly repositories: RepositoryStore;
    /** The work-graph capability group. */
    readonly workGraph: WorkGraphStore;
    /** The Job / Session / mapping capability group. */
    readonly jobs: JobStore;
    /** The durable event-feed capability group. */
    readonly events: EventLogStore;
    /** The durable per-session transcript-log capability group. */
    readonly sessionLog: SessionLogStore;
    /**
     * The identity of the STORE GENERATION this instance is open on — minted when
     * the schema was CREATED and destroyed with it.
     *
     * It is a PLAIN VALUE, not an effect, and that is a statement about the model
     * rather than an optimisation: a generation cannot change while a `StateStore`
     * exists. The schema is applied once, at layer construction (INV-FRESH's
     * drop-and-recreate is part of building the adapter), so starting a new
     * generation means building a new store — in practice, restarting the daemon.
     * Nothing can observe it changing under a live consumer, so nothing needs to
     * re-read it.
     *
     * It is what makes a durable OFFSET interpretable. Offsets (`EventLogStore` /
     * `SessionLogStore`) are positions in THIS generation's logs and restart at `1`
     * when a generation is replaced, so an offset alone cannot say which coordinate
     * space it belongs to. Pairing an offset with this id is what lets the daemon
     * refuse a client's cursor from a destroyed generation instead of resuming it
     * against a log it never belonged to. The port exposes the IDENTITY only — no
     * SQL, no file, no version number (INV-PORT); it is opaque, and equality is its
     * only defined operation.
     */
    readonly generation: StoreGenerationId;
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
