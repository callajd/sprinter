/**
 * The SQLite ADAPTER behind the {@link StateStore} port (Track A, task AE2.1).
 *
 * This is the ONLY module in the codebase permitted to reference a concrete
 * backing (INV-PORT): `@effect/sql-sqlite-bun` (`SqliteClient`) driven through
 * Effect's own SQL layer `effect/unstable/sql` (`SqlClient`, `SqlSchema`), and SQL
 * strings. It is Bun-native (`bun:sqlite` under
 * the client) — never `node:*`. The port ({@link ./store.ts}) and every consumer
 * build to the Service, never to this instance; the exported {@link layer} hides
 * the backing entirely behind a `Layer<StateStore, StateStoreError>`.
 *
 * ## The schema version is the ONLY schema-evolution mechanism (INV-FRESH)
 *
 * This store is treated as GREENFIELD: **it never migrates**. There is no migration
 * ladder and no `SqliteMigrator` — there is exactly ONE constant,
 * {@link SCHEMA_VERSION}, and one DDL definition ({@link createSchema}) describing
 * the schema AT that version. On open, the adapter compares the database's
 * `PRAGMA user_version` against the constant; on ANY mismatch it DROPS every schema
 * object and recreates the schema from scratch, then stamps the new version. No data
 * is preserved and no migration is ever written.
 *
 * **What that costs, stated plainly.** Most of what this store holds IS derived,
 * re-derivable daemon state (the work graph is re-observed from GitHub; the event
 * log is a replayable journal of it). The `agent` REGISTRY is NOT: `Agent` is an
 * OWNED entity and `putAgent` is its SOLE source of truth — there is no manifest, no
 * config file, and no upstream system to re-derive a registry from. So a version
 * bump is not merely a cache eviction: it is PERMANENT DATA LOSS of every agent
 * revision, and of the lineage history the append-only design exists to preserve.
 * That loss is ACCEPTED, not overlooked — `DMR` treats the store as greenfield
 * pre-release, and this constant is exactly the switch that says so. The
 * corresponding downstream obligation is recorded in `@sprinter/domain`'s
 * `registry.ts`: `Execution.agentId` (DE2.2) can DANGLE across a reset, so nothing
 * may assume its referent survives.
 *
 * The reset is also not free for a LIVE client: the local app and the local daemon
 * run together, so a bump can drop the database underneath a connected client that
 * still holds the pre-reset state and a durable resume cursor. That is a real
 * hazard, not bookkeeping, and it is why a generation IDENTITY is minted here (see
 * {@link createSchema}), carried on `Snapshot` and on every cursor-bearing request,
 * and enforced by the `ResyncRequired` contract error — rather than leaving a stale
 * cursor to be INFERRED from offsets, which cannot work once a new generation's log
 * outgrows the stale mark (see `packages/contract/src/rpc.ts`).
 *
 * For a SINGLE process, a bump only ever takes effect across a daemon RESTART:
 * {@link applySchema} runs at layer construction, so a live store keeps the
 * generation it opened with and the reset is observed by a client at its next
 * reconnect. That bounds WHEN the hazard can be met, not whether it must be handled.
 *
 * That bound is about the PROCESS, not the file, and nothing here extends it further:
 * there is no lock and no version negotiation, so a SECOND process opening the same
 * file-backed database at a different {@link SCHEMA_VERSION} — an older or newer
 * binary, a CLI, a test harness — drops and recreates the tables underneath a running
 * daemon, mid-connection, at a moment the daemon cannot observe. Concurrent
 * multi-process access at differing schema versions is UNGUARDED. Stated plainly
 * because the mitigation above reads like more than it is; single-process access is
 * the deployment `DMR` assumes pre-release, and locking is not in scope here.
 *
 * **{@link SCHEMA_VERSION} is the guard.** Any change to the persisted shape —
 * a new table, a new column, a changed column — is landed by editing
 * {@link createSchema} AND bumping {@link SCHEMA_VERSION}. Bumping it is what makes
 * existing stores reset; forgetting to bump it is the one way to get a stale
 * database, so the bump is part of every schema change, not an afterthought. That
 * pairing is MECHANICALLY GUARDED: `schema-version.test.ts` pins a digest of the
 * emitted DDL alongside the version, so editing {@link createSchema} without
 * bumping {@link SCHEMA_VERSION} fails the gate.
 *
 * Persistence strategy (all reads decode raw driver rows back through `Schema` —
 * never `as` / `!` / `any`, INV-NOCAST):
 *
 * - Each node is one row keyed by its id; child lists (`Workstream.epics`,
 *   `Epic.issues`) are stored as JSON columns via {@link Schema.fromJsonString}.
 * - The dependency DAG (`Issue.dependsOn`) is stored as real edges in a dedicated
 *   `issue_dependency` table and reconstructed, ordered, on read — the DAG is
 *   persisted as edges, not as an opaque blob.
 * - Optional links (`Issue.pr`, `Job.sessionId` / `Job.transcriptRef` / `Job.pr`)
 *   are nullable columns; `PullRequestRef` is stored as a JSON column.
 * - The event feed is an `AUTOINCREMENT` table; the auto-assigned rowid is the
 *   monotonic offset.
 *
 * Query building and row decoding are all Effect's own SQL layer — nothing
 * hand-rolled.
 */
import { Array as Arr, Effect, Layer, Option, Schema } from "effect";
import { SqlClient, SqlError, SqlSchema } from "effect/unstable/sql";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import {
  Agent,
  AgentId,
  EpicId,
  type Issue,
  IssueId,
  type Job,
  JobId,
  JobKind,
  JobStatus,
  NonNegativeInt,
  PositiveInt,
  PullRequestRef,
  Session,
  SessionEvent,
  SessionId,
  SessionStatus,
  StoreGenerationId,
  Timestamp,
  WorkstreamId,
  WorkStatus,
  IssueStatus,
} from "@sprinter/domain";
import {
  type AgentStore,
  type AppendEvent,
  type EventLogStore,
  type JobStore,
  type PersistedEvent,
  type PersistedSessionEvent,
  type SessionLogStore,
  StateStore,
  StateStoreError,
  type WorkGraphStore,
} from "./store.ts";

// ============================================================================
// Error mapping — backing failures → the owned StateStoreError (INV-PORT)
// ============================================================================

/**
 * Translate any backing failure (a `SqlError`, a `Schema.SchemaError`, a
 * `MigrationError`) into the owned {@link StateStoreError}. Every backing error
 * carries a `message`; that is the only surface this adapter leaks upward, so no
 * consumer ever sees a SQL/SQLite error type (INV-PORT).
 */
const fail =
  (operation: string) =>
  (error: { readonly message: string }): StateStoreError =>
    new StateStoreError({ operation, detail: error.message });

// ============================================================================
// Persistence schemas — `.Type` is the owned domain shape, `.Encoded` is the row
// ============================================================================

/** A workstream row: `epics` is a JSON-encoded child list. */
const WorkstreamRow = Schema.Struct({
  id: WorkstreamId,
  name: Schema.NonEmptyString,
  repo: Schema.NonEmptyString,
  status: WorkStatus,
  epics: Schema.fromJsonString(Schema.Array(EpicId)),
});

/** An epic row: `issues` is a JSON-encoded child list. */
const EpicRow = Schema.Struct({
  id: EpicId,
  workstreamId: WorkstreamId,
  name: Schema.NonEmptyString,
  status: WorkStatus,
  issues: Schema.fromJsonString(Schema.Array(IssueId)),
});

/**
 * An `Agent` registry row: `tools` is a JSON-encoded list; `supersedes` and
 * `retiredAt` are nullable columns (SQL `NULL` ⇔ the domain's absent optional key).
 * There is deliberately NO repository/workstream column — the registry is global
 * (INV-DERIVED) — and no status column: retired-ness IS `retiredAt` (INV-SUM).
 */
const AgentRow = Schema.Struct({
  id: AgentId,
  name: Schema.NonEmptyString,
  model: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  tools: Schema.fromJsonString(Schema.Array(Schema.NonEmptyString)),
  supersedes: Schema.NullOr(AgentId),
  retiredAt: Schema.NullOr(Timestamp),
});

/**
 * The RAW columns of an `agent` row, exactly as stored — `tools` stays the encoded
 * JSON string rather than a decoded list. This is the shape the append-only write
 * path compares against, so "already stored with identical content" is decided on
 * the STORED BYTES (both sides are produced by the same encoder), never on a
 * re-decoded value whose comparison could be looser than the column's.
 */
const AgentColumns = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  model: Schema.String,
  version: Schema.String,
  tools: Schema.String,
  supersedes: Schema.NullOr(Schema.String),
  retiredAt: Schema.NullOr(Schema.String),
});
type AgentColumns = (typeof AgentColumns)["Type"];

/**
 * True when two `agent` rows agree on every NON-LIFECYCLE column — the content a
 * revision describes (`name` / `model` / `version` / `tools`), as opposed to its
 * place in the lineage (`id` / `supersedes`) and its lifecycle stamp (`retiredAt`).
 * This is the equality a RETIREMENT must satisfy against the revision it retires.
 */
const isSameAgentContent = (stored: AgentColumns, next: AgentColumns): boolean =>
  stored.name === next.name &&
  stored.model === next.model &&
  stored.version === next.version &&
  stored.tools === next.tools;

/** True when a stored `agent` row is byte-identical to the row about to be written. */
const isSameAgentRow = (stored: AgentColumns, next: AgentColumns): boolean =>
  stored.id === next.id &&
  isSameAgentContent(stored, next) &&
  stored.supersedes === next.supersedes &&
  stored.retiredAt === next.retiredAt;

/** A session row — no optional or JSON columns. */
const SessionRow = Schema.Struct({
  id: SessionId,
  jobId: JobId,
  status: SessionStatus,
});

/**
 * A `PullRequestRef` column: a JSON-encoded ref, or SQL `NULL` when the owning
 * node has no PR yet. Decodes to `PullRequestRef | null`; the owning node's
 * `optionalKey` field is assembled from that (absent when null).
 */
const PrColumn = Schema.NullOr(Schema.fromJsonString(PullRequestRef));

/**
 * An issue's scalar columns (its `dependsOn` edges live in `issue_dependency` and
 * are joined in separately). `pr` is a nullable JSON column.
 */
const IssueRow = Schema.Struct({
  id: IssueId,
  epicId: EpicId,
  number: PositiveInt,
  title: Schema.NonEmptyString,
  status: IssueStatus,
  pr: PrColumn,
});

/** A single `issue_dependency` edge row (the `depends_on` target). */
const DependencyRow = Schema.Struct({ dependsOn: IssueId });

/** A job row: `sessionId` / `transcriptRef` are nullable; `pr` is a nullable JSON column. */
const JobRow = Schema.Struct({
  id: JobId,
  issueId: IssueId,
  kind: JobKind,
  status: JobStatus,
  sessionId: Schema.NullOr(SessionId),
  transcriptRef: Schema.NullOr(Schema.NonEmptyString),
  pr: PrColumn,
});

/** An event-feed row: `payload` is a JSON column; `offset` is the auto-assigned rowid. */
const EventRow = Schema.Struct({
  offset: NonNegativeInt,
  kind: Schema.NonEmptyString,
  payload: Schema.UnknownFromJsonString,
});

/**
 * A session-transcript-log row: `event` is a JSON column holding the owned
 * {@link SessionEvent}; `offset` is the auto-assigned rowid (monotonic per session by
 * ascending read, globally unique across sessions). The `sessionId` scoping column is
 * a query predicate, not part of the reconstructed {@link PersistedSessionEvent}.
 */
const SessionEventRow = Schema.Struct({
  offset: NonNegativeInt,
  event: Schema.fromJsonString(SessionEvent),
});

/** The single row returned by an event append's `RETURNING "offset"`. */
const OffsetRow = Schema.Struct({ offset: NonNegativeInt });
const CountRow = Schema.Struct({ n: NonNegativeInt });

/**
 * The `store_meta` row holding this generation's identity. `store_meta` is a
 * single-purpose key/value table; the only key today is {@link GENERATION_KEY}.
 */
const GenerationRow = Schema.Struct({ value: StoreGenerationId });

/** The `store_meta` key the generation identity is stored under. */
const GENERATION_KEY = "generation";

// ============================================================================
// Schema (DDL) — ONE versioned definition; drop-and-recreate, never migrate
// ============================================================================

/**
 * The version of the persisted schema below — the ONE constant that governs
 * schema evolution (INV-FRESH). It is compared against the database's
 * `PRAGMA user_version` on open: on ANY mismatch the store is DROPPED and
 * recreated at this version. It is never migrated, so no data survives a bump.
 *
 * **Bump this in the same change as any edit to {@link createSchema}.** A new
 * table, a new column, a changed column type, a changed index — all of them are a
 * new version. This constant is the guard the rest of the domain remodel bumps.
 *
 * Bumping it DESTROYS the store, including the OWNED, non-re-derivable `agent`
 * registry (see the module docstring): that is the accepted, deliberate cost of a
 * shape change while `DMR` treats the store as greenfield pre-release — not a
 * cost-free cache eviction.
 *
 * Version 1 was the greenfield reset that REPLACED the previous incremental
 * migration ladder (`1_initial` / `2_session_event_log`). A database left by that
 * ladder has `user_version = 0`, so it mismatches and is reset like any other
 * stale store. Version 2 adds `store_meta` and the generation identity minted into
 * it (see {@link createSchema}). Version 3 adds the `agent_supersedes` UNIQUE index,
 * making a FORKED lineage (one revision superseded twice) unstorable. Version 4 makes
 * `agent.supersedes` a real FOREIGN KEY onto `agent (id)` (enforced per-connection by
 * {@link configureConnection}), making a DANGLING `supersedes` unstorable — which is
 * what makes every other lineage rule order-independent instead of bypassable by
 * appending a successor before its predecessor.
 *
 * The bump is observable only across a daemon RESTART: {@link applySchema} runs at
 * LAYER CONSTRUCTION, so a running daemon holds the generation it opened with and
 * a bump reaches a client at its next reconnect, never mid-connection. That bounds
 * WHEN the stale-cursor hazard can occur; it does not reduce it, which is why the
 * generation identity below is on the wire. The bound is also SINGLE-PROCESS only —
 * see {@link applySchema} for what a second process at a different version can do.
 */
export const SCHEMA_VERSION = 4;

/**
 * A row of `sqlite_master` naming one existing schema object and its kind. The kind
 * selects the `DROP` verb — SQLite has no polymorphic drop.
 */
const SchemaObjectRow = Schema.Struct({
  type: Schema.Literals(["table", "view", "trigger"]),
  name: Schema.NonEmptyString,
});

/** The `DROP` verb for each droppable `sqlite_master` object kind. */
const DROP_VERB = { table: "TABLE", view: "VIEW", trigger: "TRIGGER" } as const;

/**
 * Drop EVERY schema object in the database — the first half of a version reset.
 *
 * The list is read from `sqlite_master` rather than hardcoded, so the reset needs
 * no maintenance when {@link createSchema} gains, renames, or drops an object (a
 * hardcoded list that fell out of step would silently leave a stale object behind,
 * which is precisely the failure INV-FRESH exists to prevent). It also sweeps
 * objects this schema does not own — notably `effect_sql_migrations`, the
 * bookkeeping the RETIRED migration ladder left behind — with no special case.
 *
 * It sweeps TABLES, VIEWS and TRIGGERS, because those are the three droppable kinds
 * `sqlite_master` can hold: a table-only sweep would claim to clear "whatever the
 * database actually holds" while leaving a view or a standalone trigger from a
 * previous version behind, and the recreated schema would then collide with it (or,
 * worse, keep firing it). `index` is deliberately absent from that list, not
 * missed: an index is always attached to a table and SQLite drops it with its
 * table, and the auto-indexes backing `PRIMARY KEY`/`UNIQUE` are not droppable at
 * all. Triggers are dropped FIRST for the same reason in reverse — a trigger
 * attached to a table about to be dropped goes with it, and dropping it explicitly
 * up front keeps the sweep independent of the order `sqlite_master` returns.
 * SQLite's internal `sqlite_%` objects (e.g. `sqlite_sequence`) are excluded because
 * they are not droppable; the engine maintains them, and dropping the AUTOINCREMENT
 * tables clears their `sqlite_sequence` rows for us.
 */
const dropSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows =
    yield* sql`SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view', 'trigger') AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 ELSE 2 END`;
  const objects = yield* Schema.decodeUnknownEffect(Schema.Array(SchemaObjectRow))(rows);
  // Names come from the database's own catalogue, never from user input, and `DROP`
  // takes no bound parameters — hence `unsafe` with a quoted identifier.
  for (const object of objects) {
    yield* sql.unsafe(`DROP ${DROP_VERB[object.type]} IF EXISTS "${object.name}"`);
  }
});

/**
 * The COMPLETE schema at {@link SCHEMA_VERSION} — one definition, not a ladder.
 * Every table lands here (the `agent` registry included); nothing is expressed as
 * an incremental step off a previous version.
 *
 * It also MINTS this generation's identity. `createSchema` runs exactly once per
 * store generation — it is the second half of the drop-and-recreate — so it is the
 * one place that can mint an id which is fresh by construction: a new generation
 * cannot come into existence without passing through here, and re-running it means
 * the previous generation's tables have just been dropped. A random UUID is
 * sufficient (the only defined operation on the id is equality, so it needs
 * uniqueness, not order or meaning).
 */
const createSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Store-wide metadata as key/value, so a future scalar needs no new table (and no
  // new shape for the reset to sweep). Its ONLY key today is the generation identity.
  yield* sql`CREATE TABLE store_meta (
      key TEXT PRIMARY KEY NOT NULL,
      value TEXT NOT NULL
    )`;
  // Minted HERE, from the platform CSPRNG, and never rewritten afterwards: the row
  // lives and dies with the schema that created it, so "a fresh generation on every
  // drop-and-recreate" holds by construction rather than by a caller remembering to
  // rotate it. `crypto` is the Web-standard global (Bun-native, never `node:*`).
  const generation = yield* Effect.sync(() => globalThis.crypto.randomUUID());
  yield* sql`INSERT INTO store_meta ${sql.insert({ key: GENERATION_KEY, value: generation })}`;
  // The append-only Agent REGISTRY: owned, global, scoped to NO repository — hence
  // no repositoryId/workstreamId column and no per-repo join table ("agents used in
  // this repo" is a fold over that repo's executions, INV-DERIVED). `retiredAt` is a
  // nullable stamp, not a status column (INV-SUM). Nothing DELETEs from this table.
  //
  // `supersedes` is a REAL foreign key onto this same table, enforced by the engine
  // (see {@link configureConnection}, which turns `PRAGMA foreign_keys` ON and
  // verifies it). That is not tidiness — it is what makes every OTHER rule on this
  // table hold. All three port-enforced `supersedes` rules (no successor to a
  // retired revision, a retirement may not rewrite content, at most one successor)
  // are decided by READING the superseded row, so while a DANGLING `supersedes` was
  // storable a writer could bypass all of them simply by appending the successor
  // FIRST and its predecessor second: the successor's check found nothing to check
  // against, and the predecessor's append was an ordinary first revision. That made
  // every rule order-dependent — a resurrection, a content-rewriting retirement and
  // a `supersedes` CYCLE were all constructible out of appends each of which the
  // port accepted. Referential integrity removes the shape those attacks are built
  // from: a revision cannot name a predecessor that is not already stored, so the
  // predecessor is ALWAYS there to be checked, and a cycle is unconstructible
  // because closing one requires naming a row that does not exist yet.
  //
  // (Self-reference is the one edge a foreign key does NOT reject — SQLite checks
  // the constraint against the table INCLUDING the row being inserted — so the
  // port's explicit `supersedes !== id` rejection in `putAgent` stays, and together
  // they make the whole relation acyclic by construction.)
  yield* sql`CREATE TABLE agent (
      id TEXT PRIMARY KEY NOT NULL,
      name TEXT NOT NULL,
      model TEXT NOT NULL,
      version TEXT NOT NULL,
      tools TEXT NOT NULL,
      supersedes TEXT,
      "retiredAt" TEXT,
      FOREIGN KEY (supersedes) REFERENCES agent (id)
    )`;
  // A revision may be superseded AT MOST ONCE: the lineage is a CHAIN, and a
  // revision with two successors is a fork with no defined head. It is checkable
  // from a single append (exactly like the self-reference and retirement rules the
  // port already enforces), so it is enforced rather than documented — and it is
  // enforced HERE, by the backing, because a UNIQUE index is atomic against a
  // concurrent racing append in a way a read-then-insert check is not.
  //
  // Without it a fork is not merely untidy, it makes a DERIVED ANSWER ambiguous:
  // `isLineageRetired` builds the reverse `supersedes` index and keeps the FIRST
  // successor it encounters, so on a forked lineage the same registry returns
  // "retired" or "not retired" depending on `listAgents`' (presentational, id-ordered)
  // order. With this index that history cannot be stored, so the predicate is
  // order-independent for every input the store can produce.
  //
  // PARTIAL (`WHERE supersedes IS NOT NULL`) because NULL means "supersedes nothing":
  // every lineage's FIRST revision carries it, and those must not collide with each
  // other. (SQLite treats NULLs as distinct in a UNIQUE index anyway; the predicate
  // says so explicitly and keeps the index off the rows that never need it.)
  yield* sql`CREATE UNIQUE INDEX agent_supersedes ON agent (supersedes) WHERE supersedes IS NOT NULL`;
  yield* sql`CREATE TABLE workstream (
      id TEXT PRIMARY KEY NOT NULL,
      name TEXT NOT NULL,
      repo TEXT NOT NULL,
      status TEXT NOT NULL,
      epics TEXT NOT NULL
    )`;
  yield* sql`CREATE TABLE epic (
      id TEXT PRIMARY KEY NOT NULL,
      "workstreamId" TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      issues TEXT NOT NULL
    )`;
  yield* sql`CREATE INDEX epic_workstream ON epic ("workstreamId")`;
  yield* sql`CREATE TABLE issue (
      id TEXT PRIMARY KEY NOT NULL,
      "epicId" TEXT NOT NULL,
      number INTEGER NOT NULL,
      title TEXT NOT NULL,
      status TEXT NOT NULL,
      pr TEXT
    )`;
  yield* sql`CREATE INDEX issue_epic ON issue ("epicId")`;
  yield* sql`CREATE TABLE issue_dependency (
      "issueId" TEXT NOT NULL,
      seq INTEGER NOT NULL,
      "dependsOn" TEXT NOT NULL,
      PRIMARY KEY ("issueId", seq)
    )`;
  yield* sql`CREATE TABLE job (
      id TEXT PRIMARY KEY NOT NULL,
      "issueId" TEXT NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL,
      "sessionId" TEXT,
      "transcriptRef" TEXT,
      pr TEXT
    )`;
  yield* sql`CREATE INDEX job_issue ON job ("issueId")`;
  yield* sql`CREATE TABLE session (
      id TEXT PRIMARY KEY NOT NULL,
      "jobId" TEXT NOT NULL,
      status TEXT NOT NULL
    )`;
  // UNIQUE enforces the domain invariant 1 Job = 1 session (conventions): at most
  // one session per job, so `getSessionForJob` is deterministic by construction and
  // a stray second session for a job fails at the backing (surfacing as a
  // StateStoreError) rather than silently returning an arbitrary row.
  yield* sql`CREATE UNIQUE INDEX session_job ON session ("jobId")`;
  yield* sql`CREATE TABLE event_log (
      "offset" INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL,
      payload TEXT NOT NULL
    )`;
  // The durable per-session transcript log: one append-only row per
  // durable, transcript-grade session event, scoped by "sessionId". The AUTOINCREMENT
  // rowid is the monotonic offset — globally unique, so a re-dispatch of the same session
  // id APPENDS with fresh higher offsets rather than reusing or resetting the sequence
  // (never a duplicated/corrupted offset). The `session_event_log_session` index keeps a
  // per-session ordered read/tail cheap.
  yield* sql`CREATE TABLE session_event_log (
      "offset" INTEGER PRIMARY KEY AUTOINCREMENT,
      "sessionId" TEXT NOT NULL,
      event TEXT NOT NULL
    )`;
  yield* sql`CREATE INDEX session_event_log_session ON session_event_log ("sessionId")`;
});

/** The single row returned by `PRAGMA user_version`. */
const UserVersionRow = Schema.Struct({ user_version: NonNegativeInt });

/** The single row returned by `PRAGMA foreign_keys` — `1` when enforcement is on. */
const ForeignKeysRow = Schema.Struct({ foreign_keys: NonNegativeInt });

/**
 * Turn FOREIGN KEY ENFORCEMENT ON for this connection, and PROVE it took.
 *
 * SQLite defaults `foreign_keys` OFF, and the setting is PER-CONNECTION, not a
 * property of the file: a database whose DDL declares foreign keys silently
 * enforces none of them unless every connection that writes to it has asked for
 * enforcement. The `agent` table's self-referential `supersedes` key is load-bearing
 * (see {@link createSchema}) — with it off, a dangling `supersedes` is storable again
 * and every lineage rule reverts to being bypassable by write ordering — so this is
 * not a nicety that may quietly fail to apply.
 *
 * Hence the read-back: the pragma is SET and then QUERIED, and a connection that
 * still reports it off fails the store's CONSTRUCTION rather than serving writes
 * under rules it is not enforcing. (SQLite answers a `PRAGMA foreign_keys = ON` it
 * cannot honour — e.g. inside a transaction — by doing nothing at all, with no
 * error, which is exactly the silent case a fire-and-forget statement would miss.)
 * The adapter opens ONE connection per built layer ({@link SqliteClient.layer} holds
 * a single `Database`), and this runs at layer construction ahead of every other
 * statement, so "every connection the adapter opens" is covered by construction.
 */
const configureConnection = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // `PRAGMA` takes no bound parameters and this one has no user input in it.
  yield* sql.unsafe(`PRAGMA foreign_keys = ON`);
  const rows = yield* sql`PRAGMA foreign_keys`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(ForeignKeysRow))(rows);
  if (decoded[0].foreign_keys !== 1) {
    // Reported in the same `{ message }` shape every backing failure arrives in, so
    // the caller's single `fail("applySchema")` mapping covers it too.
    return yield* Effect.fail({
      message:
        "SQLite refused `PRAGMA foreign_keys = ON`; the agent registry's `supersedes` foreign key would not be enforced, leaving a dangling reference storable",
    });
  }
});

/**
 * Bring the database to {@link SCHEMA_VERSION}, the ONLY schema-evolution path
 * (INV-FRESH): read `PRAGMA user_version`, and if it does not already equal the
 * constant, DROP every table and recreate the schema from {@link createSchema},
 * then stamp the new version. A fresh database reports `0`, so it takes the same
 * reset path — there is exactly one code path, and it never migrates.
 *
 * The whole reset runs in a transaction so a crash mid-reset cannot leave a
 * half-built schema stamped with the new version. `PRAGMA` statements take no
 * bound parameters, so the version is interpolated from our own numeric constant
 * (never user input).
 *
 * `PRAGMA defer_foreign_keys` holds the reset's own foreign keys to COMMIT time.
 * {@link configureConnection} has already turned enforcement on, and `DROP TABLE`
 * under enforcement performs an implicit `DELETE FROM` first — so dropping the
 * self-referential `agent` table would trip its own key on the intermediate state.
 * Deferring is correct rather than a loophole: at commit the table is gone, so
 * there is nothing left to violate, and the setting resets itself at the end of the
 * transaction (unlike `foreign_keys`, which is a no-op inside one).
 *
 * **The restart bound holds for a SINGLE PROCESS only.** A bump takes effect at
 * layer construction, so a live store keeps the generation it opened with and a
 * running daemon observes no reset mid-connection — but that is a statement about
 * THIS process, not about the file. Nothing here takes a lock or negotiates a
 * version, so a SECOND process opening the same file-backed database at a different
 * {@link SCHEMA_VERSION} — an older or newer binary, a CLI, a test harness — will
 * drop and recreate the tables underneath a running daemon, which keeps serving from
 * a generation whose rows no longer exist. Concurrent multi-process access at
 * differing schema versions is UNGUARDED; it is out of scope for the greenfield
 * store and is stated here rather than implied away (tracked in #91).
 */
const applySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`PRAGMA user_version`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(UserVersionRow))(rows);
  if (decoded[0].user_version === SCHEMA_VERSION) return;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql.unsafe(`PRAGMA defer_foreign_keys = ON`);
      yield* dropSchema;
      yield* createSchema;
      yield* sql.unsafe(`PRAGMA user_version = ${SCHEMA_VERSION}`);
    }),
  );
});

// ============================================================================
// Service construction
// ============================================================================

/**
 * Build the {@link StateStore} implementation over the ambient {@link SqlClient}.
 * Each method encodes its request, runs a parameterised statement, and decodes
 * rows back through `Schema`; all backing failures are mapped to
 * {@link StateStoreError} at the boundary.
 */
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // ── the store generation ────────────────────────────────────────────────
  //
  // Read ONCE, here, and exposed as a plain value: {@link applySchema} has already
  // run (the schema layer is provided beneath this one), so the row exists and its
  // value cannot change while this service lives — a new generation is a new
  // `createSchema`, hence a new layer build. Reading it eagerly is therefore not a
  // cache but a statement of the model, and it means a missing/corrupt row fails the
  // store's CONSTRUCTION rather than some later resume request.
  const generationRows =
    yield* sql`SELECT value FROM store_meta WHERE key = ${GENERATION_KEY}`.pipe(
      Effect.mapError(fail("generation")),
    );
  const generation = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(GenerationRow))(
    generationRows,
  ).pipe(
    Effect.mapError(() =>
      fail("generation")({
        message: `store_meta has no "${GENERATION_KEY}" row; the store generation is minted by createSchema and cannot be reconstructed`,
      }),
    ),
    Effect.map((rows) => rows[0].value),
  );

  // ── AgentStore ──────────────────────────────────────────────────────────
  //
  // Append + read only: there is no DELETE and no UPDATE statement here, by design.
  // An edit appends a NEW revision (a new id linked by `supersedes`) and a
  // retirement appends one carrying `supersedes` AND `retiredAt`, so the table is
  // immutable history — a stored row's columns are never rewritten.

  /**
   * Reconstruct one {@link Agent} from its row, dropping SQL `NULL`s to absent
   * optionals. `operation` is the calling store method so a failure reports the
   * caller, not a fixed label.
   */
  const hydrateAgent = (row: unknown, operation: string): Effect.Effect<Agent, StateStoreError> =>
    Schema.decodeUnknownEffect(AgentRow)(row).pipe(
      Effect.map(
        (r): Agent => ({
          id: r.id,
          name: r.name,
          model: r.model,
          version: r.version,
          tools: r.tools,
          ...(r.supersedes !== null ? { supersedes: r.supersedes } : {}),
          ...(r.retiredAt !== null ? { retiredAt: r.retiredAt } : {}),
        }),
      ),
      Effect.mapError(fail(operation)),
    );

  /**
   * Enforce the rules an append carrying `supersedes` must satisfy against the
   * revision it names — for EVERY such append, edit and retirement alike:
   *
   * 1. THE SUPERSEDED REVISION MUST NOT BE RETIRED. A `retiredAt` stamp is the
   *    lineage's terminal state, so nothing may be appended after it. This is scoped
   *    to the WHOLE `supersedes` relation, not to retiring appends only, because both
   *    ways of extending a dead lineage are wrong and only one of them looks like it:
   *    a RETIRING successor would leave two revisions each claiming to be the
   *    retirement, with no way to say which instant the lineage stopped; a
   *    NON-RETIRING one silently RESURRECTS it — the successor carries no stamp, so
   *    `isLineageRetired` walks forward off the retired revision onto a live head and
   *    the lineage reads as back in service. Un-retiring is precisely what the port
   *    promises cannot happen (see `store.ts`: "a lineage goes out of service once"),
   *    and gating this rule on the incoming revision's own `retiredAt` left that door
   *    open. It is checked on `supersedes` alone. (A revision naming a MID-lineage
   *    revision that was later superseded is a different shape, and it is not left to
   *    the writer either: the `agent_supersedes` UNIQUE index makes that branch
   *    unstorable outright.)
   * 2. A RETIRING revision (one carrying `retiredAt` as well) must additionally be
   *    LIFECYCLE-ONLY — it must repeat the superseded revision's non-lifecycle
   *    columns verbatim (retirement sets `retiredAt`, never content). Rule 1 cannot
   *    catch a fused edit, because it looks only at the lifecycle columns; rule 2
   *    cannot catch a resurrection, because it deliberately ignores them. Both are
   *    needed, and they apply to overlapping — not identical — sets of appends.
   *
   * Compared on the STORED BYTES (`tools` stays the encoded JSON string, as
   * {@link AgentColumns} models it), so "same content" is decided exactly as the
   * column stores it rather than by a looser re-decoded comparison — the same basis
   * {@link isSameAgentRow} uses for the idempotent re-append.
   *
   * ## Why these rules are ORDER-INDEPENDENT
   *
   * Every rule here is decided by READING the superseded row, so each one is only as
   * strong as the guarantee that the row is there to be read. It is: `supersedes` is a
   * FOREIGN KEY (see {@link createSchema}) and enforcement is on (see
   * {@link configureConnection}), so an append naming an unstored predecessor is
   * rejected by the ENGINE, on the INSERT, inside this same transaction.
   *
   * That is load-bearing, not belt-and-braces. While a dangling `supersedes` was
   * storable, a writer bypassed all of these rules for free by reversing the write
   * order — append the successor first (nothing to check against, accepted), then its
   * predecessor (an ordinary first revision, accepted) — and landed a resurrection, a
   * content-rewriting retirement, or a `supersedes` cycle out of two individually
   * legal appends. With the key in place there is no ordering that reaches those
   * shapes: a predecessor always exists BEFORE anything can name it.
   *
   * The `onNone` branch below is therefore not a permissive fallthrough but a
   * transient: the SELECT can still miss, and the INSERT that follows is what rejects
   * it, with the engine's referential-integrity error surfaced as a
   * {@link StateStoreError}. Nothing is accepted on that path.
   *
   * The check runs inside the caller's transaction, so the row it reads is the one the
   * insert commits against.
   */
  const assertSupersedesIsWellFormed = (
    supersedes: AgentId,
    row: AgentColumns,
  ): Effect.Effect<void, StateStoreError> =>
    sql`SELECT * FROM agent WHERE id = ${supersedes}`.pipe(
      Effect.mapError(fail("putAgent")),
      Effect.flatMap((rows) =>
        Option.match(Arr.head(rows), {
          // Not "allowed": UNCHECKABLE, and about to be rejected anyway — the INSERT
          // that follows trips the `supersedes` FOREIGN KEY. See the docstring.
          onNone: () => Effect.void,
          onSome: (found) =>
            Schema.decodeUnknownEffect(AgentColumns)(found).pipe(
              Effect.mapError(fail("putAgent")),
              Effect.flatMap((head) => {
                if (head.retiredAt !== null) {
                  return Effect.fail(
                    new StateStoreError({
                      operation: "putAgent",
                      detail: `agent "${row.id}" supersedes "${supersedes}", which is RETIRED (at ${head.retiredAt}); a lineage goes out of service ONCE and nothing may be appended after its retirement — a further revision would either double-stamp it or silently un-retire it`,
                    }),
                  );
                }
                if (row.retiredAt !== null && !isSameAgentContent(head, row)) {
                  return Effect.fail(
                    new StateStoreError({
                      operation: "putAgent",
                      detail: `agent "${row.id}" retires "${supersedes}" but changes its content; a retirement sets retiredAt ONLY — append the edit as its own revision first, then retire that revision`,
                    }),
                  );
                }
                return Effect.void;
              }),
            ),
        }),
      ),
    );

  const agents: AgentStore = {
    putAgent: (agent) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            // A revision that supersedes ITSELF is the one cycle the `supersedes`
            // FOREIGN KEY does NOT reject — SQLite checks the constraint against the
            // table INCLUDING the row being inserted, so a self-reference satisfies
            // it. Every LONGER cycle needs an edge naming a row that does not exist
            // yet, which the key rejects; closing that last gap here makes the whole
            // relation acyclic by construction, so a consumer's backwards walk
            // (`isOriginalRevision`) terminates on every history the store can hold.
            if (agent.supersedes === agent.id) {
              return yield* Effect.fail(
                new StateStoreError({
                  operation: "putAgent",
                  detail: `agent "${agent.id}" supersedes itself; a revision must supersede a DIFFERENT revision`,
                }),
              );
            }
            const tools = yield* Schema.encodeEffect(
              Schema.fromJsonString(Schema.Array(Schema.NonEmptyString)),
            )(agent.tools).pipe(Effect.mapError(fail("putAgent")));
            const row = {
              id: agent.id,
              name: agent.name,
              model: agent.model,
              version: agent.version,
              tools,
              supersedes: agent.supersedes ?? null,
              retiredAt: agent.retiredAt ?? null,
            };
            // The registry is APPEND-ONLY, so this is an INSERT with no `ON
            // CONFLICT ... DO UPDATE`: a stored row is immutable and is never
            // rewritten. The stored row is read first so the two legitimate
            // outcomes stay distinguishable — a byte-identical re-append is the
            // crash-retry case and succeeds as a no-op, while a DIFFERING append
            // under an existing id is a mutation attempt and fails. Without that,
            // an upsert would silently replace a revision's content or drop a
            // `retiredAt` stamp (un-retiring an agent) and then fan that mutation
            // out to every client as `AgentChanged`. Read-then-insert runs inside
            // the transaction, so a concurrent racing append cannot slip between
            // them; and if one did, the PRIMARY KEY conflict fails the write
            // rather than rewriting the row.
            const existing = yield* sql`SELECT * FROM agent WHERE id = ${agent.id}`.pipe(
              Effect.mapError(fail("putAgent")),
            );
            const stored = Arr.head(existing);
            if (Option.isSome(stored)) {
              const current = yield* Schema.decodeUnknownEffect(AgentColumns)(stored.value).pipe(
                Effect.mapError(fail("putAgent")),
              );
              if (isSameAgentRow(current, row)) return "unchanged" as const;
              return yield* Effect.fail(
                new StateStoreError({
                  operation: "putAgent",
                  detail: `agent "${agent.id}" is already stored with different content; the registry is append-only, so an edit or a retirement appends a NEW revision whose supersedes names "${agent.id}"`,
                }),
              );
            }
            // EVERY append that names a `supersedes` is checked against the revision
            // it names — not just a retiring one. Two rules live there, on
            // overlapping but different sets of appends: a RETIRED revision may not
            // be superseded AT ALL (a successor either double-stamps the retirement
            // or, carrying no stamp of its own, silently un-retires the lineage —
            // which the port promises cannot happen), and a RETIRING revision must
            // additionally be lifecycle-only, repeating the superseded revision's
            // content verbatim so an edit and a retirement never fuse into one
            // indistinguishable append. Gating the first on the incoming revision's
            // own `retiredAt` — as this once did — enforced it for the append that
            // could not do the damage and skipped the one that could. The superseded
            // revision is ALWAYS there to be read — the `supersedes` FOREIGN KEY
            // guarantees it — so both rules are order-independent.
            if (agent.supersedes !== undefined) {
              yield* assertSupersedesIsWellFormed(agent.supersedes, row);
            }
            yield* sql`INSERT INTO agent ${sql.insert(row)}`.pipe(
              Effect.mapError(fail("putAgent")),
            );
            return "appended" as const;
          }),
        )
        .pipe(
          Effect.mapError(
            (error): StateStoreError =>
              SqlError.isSqlError(error) ? fail("putAgent")(error) : error,
          ),
        ),
    getAgent: (id) =>
      sql`SELECT * FROM agent WHERE id = ${id}`.pipe(
        Effect.mapError(fail("getAgent")),
        Effect.flatMap((rows) =>
          Option.match(Arr.head(rows), {
            onNone: () => Effect.succeedNone,
            onSome: (row) => Effect.asSome(hydrateAgent(row, "getAgent")),
          }),
        ),
      ),
    // `id` is a `TEXT` column with NO `COLLATE` clause, so `ORDER BY id` is SQLite's
    // default BINARY collation: a `memcmp` over the UTF-8 bytes. That BYTE order —
    // not a locale-aware alphabetical one — is what the port pins (`"agt-10"` sorts
    // before `"agt-2"`), and it is presentational: a lineage is read off
    // `supersedes`, never off this order.
    listAgents: sql`SELECT * FROM agent ORDER BY id`.pipe(
      Effect.mapError(fail("listAgents")),
      Effect.flatMap((rows) => Effect.forEach(rows, (row) => hydrateAgent(row, "listAgents"))),
    ),
  };

  // ── WorkGraphStore ──────────────────────────────────────────────────────

  const putWorkstream = SqlSchema.void({
    Request: WorkstreamRow,
    execute: (row) =>
      sql`INSERT INTO workstream ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`,
  });

  const putEpic = SqlSchema.void({
    Request: EpicRow,
    execute: (row) =>
      sql`INSERT INTO epic ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`,
  });

  const getWorkstreamQuery = SqlSchema.findOneOption({
    Request: WorkstreamId,
    Result: WorkstreamRow,
    execute: (id) => sql`SELECT * FROM workstream WHERE id = ${id}`,
  });

  const getEpicQuery = SqlSchema.findOneOption({
    Request: EpicId,
    Result: EpicRow,
    execute: (id) => sql`SELECT * FROM epic WHERE id = ${id}`,
  });

  const listEpicsQuery = SqlSchema.findAll({
    Request: WorkstreamId,
    Result: EpicRow,
    execute: (workstreamId) =>
      sql`SELECT * FROM epic WHERE "workstreamId" = ${workstreamId} ORDER BY id`,
  });

  /**
   * Reconstruct one {@link Issue} from its scalar row plus its ordered `dependsOn`
   * edges. `operation` is the calling store method (`getIssue` / `listIssues`) so a
   * failure reports the caller, not a fixed label.
   */
  const hydrateIssue = (row: unknown, operation: string): Effect.Effect<Issue, StateStoreError> =>
    Effect.gen(function* () {
      const base = yield* Schema.decodeUnknownEffect(IssueRow)(row);
      const edgeRows =
        yield* sql`SELECT "dependsOn" FROM issue_dependency WHERE "issueId" = ${base.id} ORDER BY seq`;
      const edges = yield* Schema.decodeUnknownEffect(Schema.Array(DependencyRow))(edgeRows);
      const issue: Issue = {
        id: base.id,
        epicId: base.epicId,
        number: base.number,
        title: base.title,
        status: base.status,
        dependsOn: edges.map((edge) => edge.dependsOn),
        ...(base.pr !== null ? { pr: base.pr } : {}),
      };
      return issue;
    }).pipe(Effect.mapError(fail(operation)));

  const putWorkGraph: WorkGraphStore = {
    putWorkstream: (workstream) =>
      putWorkstream(workstream).pipe(Effect.mapError(fail("putWorkstream"))),
    putEpic: (epic) => putEpic(epic).pipe(Effect.mapError(fail("putEpic"))),
    putIssue: (issue) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const pr = yield* Schema.encodeEffect(PrColumn)(issue.pr ?? null);
            const row = {
              id: issue.id,
              epicId: issue.epicId,
              number: issue.number,
              title: issue.title,
              status: issue.status,
              pr,
            };
            yield* sql`INSERT INTO issue ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`;
            yield* sql`DELETE FROM issue_dependency WHERE "issueId" = ${issue.id}`;
            if (issue.dependsOn.length > 0) {
              const edges = issue.dependsOn.map((dependsOn, seq) => ({
                issueId: issue.id,
                seq,
                dependsOn,
              }));
              yield* sql`INSERT INTO issue_dependency ${sql.insert(edges)}`;
            }
          }),
        )
        .pipe(Effect.mapError(fail("putIssue"))),
    getWorkstream: (id) => getWorkstreamQuery(id).pipe(Effect.mapError(fail("getWorkstream"))),
    getEpic: (id) => getEpicQuery(id).pipe(Effect.mapError(fail("getEpic"))),
    getIssue: (id) =>
      sql`SELECT * FROM issue WHERE id = ${id}`.pipe(
        Effect.mapError(fail("getIssue")),
        Effect.flatMap((rows) =>
          Option.match(Arr.head(rows), {
            onNone: () => Effect.succeedNone,
            onSome: (row) => Effect.asSome(hydrateIssue(row, "getIssue")),
          }),
        ),
      ),
    listWorkstreams: sql`SELECT * FROM workstream ORDER BY id`.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(WorkstreamRow))),
      Effect.mapError(fail("listWorkstreams")),
    ),
    listEpics: (workstreamId) =>
      listEpicsQuery(workstreamId).pipe(Effect.mapError(fail("listEpics"))),
    listIssues: (epicId) =>
      sql`SELECT * FROM issue WHERE "epicId" = ${epicId} ORDER BY number`.pipe(
        Effect.mapError(fail("listIssues")),
        Effect.flatMap((rows) => Effect.forEach(rows, (row) => hydrateIssue(row, "listIssues"))),
      ),
  };

  // ── JobStore ────────────────────────────────────────────────────────────

  /**
   * Reconstruct one {@link Job} from its row, dropping SQL `NULL`s to absent
   * optionals. `operation` is the calling store method (`getJob` /
   * `listJobsForIssue`) so a failure reports the caller, not a fixed label.
   */
  const hydrateJob = (row: unknown, operation: string): Effect.Effect<Job, StateStoreError> =>
    Schema.decodeUnknownEffect(JobRow)(row).pipe(
      Effect.map(
        (r): Job => ({
          id: r.id,
          issueId: r.issueId,
          kind: r.kind,
          status: r.status,
          ...(r.sessionId !== null ? { sessionId: r.sessionId } : {}),
          ...(r.transcriptRef !== null ? { transcriptRef: r.transcriptRef } : {}),
          ...(r.pr !== null ? { pr: r.pr } : {}),
        }),
      ),
      Effect.mapError(fail(operation)),
    );

  const putSession = SqlSchema.void({
    Request: Session,
    execute: (row) =>
      sql`INSERT INTO session ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`,
  });

  const getSessionQuery = SqlSchema.findOneOption({
    Request: SessionId,
    Result: SessionRow,
    execute: (id) => sql`SELECT * FROM session WHERE id = ${id}`,
  });

  const getSessionForJobQuery = SqlSchema.findOneOption({
    Request: JobId,
    Result: SessionRow,
    execute: (jobId) => sql`SELECT * FROM session WHERE "jobId" = ${jobId}`,
  });

  const jobs: JobStore = {
    putJob: (job) =>
      Effect.gen(function* () {
        const pr = yield* Schema.encodeEffect(PrColumn)(job.pr ?? null);
        const row = {
          id: job.id,
          issueId: job.issueId,
          kind: job.kind,
          status: job.status,
          sessionId: job.sessionId ?? null,
          transcriptRef: job.transcriptRef ?? null,
          pr,
        };
        yield* sql`INSERT INTO job ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`;
      }).pipe(Effect.mapError(fail("putJob"))),
    getJob: (id) =>
      sql`SELECT * FROM job WHERE id = ${id}`.pipe(
        Effect.mapError(fail("getJob")),
        Effect.flatMap((rows) =>
          Option.match(Arr.head(rows), {
            onNone: () => Effect.succeedNone,
            onSome: (row) => Effect.asSome(hydrateJob(row, "getJob")),
          }),
        ),
      ),
    listJobsForIssue: (issueId) =>
      sql`SELECT * FROM job WHERE "issueId" = ${issueId} ORDER BY id`.pipe(
        Effect.mapError(fail("listJobsForIssue")),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) => hydrateJob(row, "listJobsForIssue")),
        ),
      ),
    putSession: (session) => putSession(session).pipe(Effect.mapError(fail("putSession"))),
    getSession: (id) => getSessionQuery(id).pipe(Effect.mapError(fail("getSession"))),
    getSessionForJob: (jobId) =>
      getSessionForJobQuery(jobId).pipe(Effect.mapError(fail("getSessionForJob"))),
  };

  // ── EventLogStore ─────────────────────────────────────────────────────────

  const events: EventLogStore = {
    append: (event: AppendEvent) =>
      Effect.gen(function* () {
        const payload = yield* Schema.encodeEffect(Schema.UnknownFromJsonString)(event.payload);
        const rows =
          yield* sql`INSERT INTO event_log ${sql.insert({ kind: event.kind, payload })} RETURNING "offset"`;
        const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(OffsetRow))(rows);
        const persisted: PersistedEvent = {
          offset: decoded[0].offset,
          kind: event.kind,
          payload: event.payload,
        };
        return persisted;
      }).pipe(Effect.mapError(fail("append"))),
    read: sql`SELECT * FROM event_log ORDER BY "offset"`.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(EventRow))),
      Effect.mapError(fail("read")),
    ),
    tail: (offset) =>
      sql`SELECT * FROM event_log WHERE "offset" > ${offset} ORDER BY "offset"`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(EventRow))),
        Effect.mapError(fail("tail")),
      ),
    // `MAX("offset")` is answered by the backing from the PRIMARY KEY index — the
    // whole point of exposing it rather than reading the feed and taking the last
    // entry. `COALESCE` turns the SQL `NULL` an empty table yields into `0`, which is
    // below every durable offset (they are strictly `> 0`), so "empty" needs no
    // separate shape.
    maxOffset: sql`SELECT COALESCE(MAX("offset"), 0) AS n FROM event_log`.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.NonEmptyArray(CountRow))),
      Effect.map((rows) => rows[0].n),
      Effect.mapError(fail("maxOffset")),
    ),
  };

  // ── SessionLogStore ─────────────────────────────────────────────────────────

  const sessionLog: SessionLogStore = {
    append: (sessionId, event) =>
      Effect.gen(function* () {
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(SessionEvent))(event);
        const rows =
          yield* sql`INSERT INTO session_event_log ${sql.insert({ sessionId, event: encoded })} RETURNING "offset"`;
        const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(OffsetRow))(rows);
        const persisted: PersistedSessionEvent = { offset: decoded[0].offset, event };
        return persisted;
      }).pipe(Effect.mapError(fail("sessionLog.append"))),
    // The base store owns DURABILITY, not the live feed: an ephemeral delta is not persisted
    // here, so this is a total no-op. The daemon's journaling decorator overrides it to fan
    // the delta out on the `SessionEvents` feed offset-less.
    publishEphemeral: () => Effect.void,
    read: (sessionId) =>
      sql`SELECT "offset", event FROM session_event_log WHERE "sessionId" = ${sessionId} ORDER BY "offset"`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(SessionEventRow))),
        Effect.mapError(fail("sessionLog.read")),
      ),
    tail: (sessionId, offset) =>
      sql`SELECT "offset", event FROM session_event_log WHERE "sessionId" = ${sessionId} AND "offset" > ${offset} ORDER BY "offset"`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(SessionEventRow))),
        Effect.mapError(fail("sessionLog.tail")),
      ),
    // A `COUNT(*)` with a `json_extract` tag filter — the count is computed in SQLite and
    // never materializes or JSON/Schema-decodes the transcript rows (unlike `read`), so it
    // stays cheap on a long or many-times-retried session.
    countEntries: (sessionId) =>
      sql`SELECT COUNT(*) AS n FROM session_event_log WHERE "sessionId" = ${sessionId} AND json_extract(event, '$._tag') = 'EntryAppended'`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.NonEmptyArray(CountRow))),
        Effect.map((rows) => rows[0].n),
        Effect.mapError(fail("sessionLog.countEntries")),
      ),
    // The per-session extent, answered by the backing over the `session_event_log_session`
    // index — no row is materialized or JSON-decoded, which is the whole point: the
    // `sessionEvents` cursor guard runs BEFORE the transcript read, so a request that is
    // about to be refused never pays for one. `COALESCE` turns the SQL `NULL` an
    // entry-less session yields into `0`, below every durable offset (strictly `> 0`).
    maxOffset: (sessionId) =>
      sql`SELECT COALESCE(MAX("offset"), 0) AS n FROM session_event_log WHERE "sessionId" = ${sessionId}`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.NonEmptyArray(CountRow))),
        Effect.map((rows) => rows[0].n),
        Effect.mapError(fail("sessionLog.maxOffset")),
      ),
  };

  // ── transactions ──────────────────────────────────────────────────────────

  /**
   * Run `effect` in a single SQLite transaction on the ambient {@link SqlClient}:
   * all its writes commit together or roll back together, and a nested
   * `withTransaction` (e.g. `putIssue`'s own) folds into it as a savepoint. The
   * backing's own `SqlError` from BEGIN/COMMIT is mapped to the owned
   * {@link StateStoreError} at the boundary (INV-PORT); the caller's error channel
   * `E` passes through untouched.
   */
  const withTransaction = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | StateStoreError, R> =>
    sql.withTransaction(effect).pipe(
      // Only the backing's own BEGIN/COMMIT `SqlError` is mapped to the owned error
      // (INV-PORT); the caller's `E` (already-owned `StateStoreError`s from the inner
      // writes) passes through untouched.
      Effect.mapError((error): E | StateStoreError =>
        SqlError.isSqlError(error) ? fail("withTransaction")(error) : error,
      ),
    );

  return StateStore.of({
    agents,
    workGraph: putWorkGraph,
    jobs,
    events,
    sessionLog,
    generation,
    withTransaction,
  });
});

// ============================================================================
// Adapter layer
// ============================================================================

/** Configuration for the SQLite adapter. */
export interface StateStoreConfig {
  /**
   * The SQLite database file. Use `":memory:"` for an ephemeral in-memory
   * database (tests). A file path persists across restarts (AE5).
   */
  readonly filename: string;
  /**
   * Disable WAL journal mode. Defaults to disabled for `":memory:"` (WAL is
   * meaningless for an in-memory database) and enabled otherwise.
   */
  readonly disableWAL?: boolean;
}

/**
 * The SQLite adapter for {@link StateStore}. Opens the database, brings it to
 * {@link SCHEMA_VERSION} at construction (drop-and-recreate on a version mismatch —
 * never a migration, INV-FRESH), and provides the `StateStore` service — all behind
 * a `Layer<StateStore, StateStoreError>` that exposes no backing type (INV-PORT).
 */
/**
 * The adapter MINUS its backing connection: the construction-time guards
 * ({@link configureConnection}, {@link applySchema}) and {@link make}, over whatever
 * `SqlClient` is provided beneath.
 *
 * Foreign-key enforcement is turned on (and verified) BEFORE any schema work runs, on
 * the same connection: the pragma is per-connection, and `agent.supersedes` is only a
 * real constraint while it is on.
 *
 * NOT part of the package surface — `index.ts` re-exports only the backing-free
 * {@link layer} (INV-PORT). It is separated here so the construction guards can be
 * driven over a STUBBED `SqlClient` in tests: each of them refuses to build the store
 * at all, and a refusal that quietly became a success is only observable by feeding
 * the client an answer a real SQLite would not give.
 */
export const layerOverSqlClient: Layer.Layer<StateStore, StateStoreError, SqlClient.SqlClient> =
  Layer.effect(StateStore, make).pipe(
    Layer.provide(
      Layer.effectDiscard(
        configureConnection.pipe(Effect.andThen(applySchema), Effect.mapError(fail("applySchema"))),
      ),
    ),
  );

export const layer = (config: StateStoreConfig): Layer.Layer<StateStore, StateStoreError> => {
  const disableWAL = config.disableWAL ?? config.filename === ":memory:";
  const clientLayer = SqliteClient.layer({ filename: config.filename, disableWAL });
  return layerOverSqlClient.pipe(Layer.provide(clientLayer));
};

/** Convenience adapter for an ephemeral in-memory database (deterministic, offline — tests). */
export const layerMemory: Layer.Layer<StateStore, StateStoreError> = layer({
  filename: ":memory:",
});
