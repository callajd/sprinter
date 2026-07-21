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
 * - Optional links (`Issue.pr`, `Job.executionId` / `Job.transcriptRef` / `Job.pr`,
 *   `Execution.parent`) are nullable columns; `PullRequestRef` and the `Transcript`
 *   union are stored as JSON columns (a union is stored WHOLE, so its variant and its
 *   payload cannot disagree).
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
  BranchName,
  CommitSha,
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
  type Repository,
  RepositoryHost,
  RepositoryId,
  type RepositoryRef,
  RepositoryRefs,
  RepositorySegment,
  Execution,
  ExecutionEvent,
  ExecutionId,
  ExecutionMode,
  Transcript,
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
  type ExecutionLogStore,
  type JobStore,
  type PersistedEvent,
  type PersistedExecutionEvent,
  type RepositoryStore,
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

/**
 * A `repository` row — the entity's SCALAR columns only. Its observed refs live in
 * the `repository_ref` child table (D4) and are joined in separately, exactly as an
 * issue's `dependsOn` edges are: a JSON blob would make a duplicate branch name merely
 * unlikely, while the child table's composite PRIMARY KEY makes it unconstructible.
 */
const RepositoryScalarRow = Schema.Struct({
  id: RepositoryId,
  host: RepositoryHost,
  owner: RepositorySegment,
  name: RepositorySegment,
  observedAt: Timestamp,
});

/**
 * The `repository_ref` rows of ONE repository, read `ORDER BY name`.
 *
 * It is the domain's branded {@link RepositoryRefs}, not a bare `Schema.Array` of rows,
 * so the read is CHECKED against the same order rule a producer is: SQLite's default
 * BINARY collation is a `memcmp` over UTF-8, which is exactly the domain's
 * `compareBranchNames` (Unicode code point), and decoding through the brand is what
 * makes "the store reads them back in the required order" a verified property of every
 * read rather than a claim in a comment.
 */
const RepositoryRefRows = RepositoryRefs;

/**
 * One row of the LEFT-JOINED repository read: a repository's scalar columns plus at
 * most one of its refs.
 *
 * A joined read is how `listRepositories` avoids issuing one ref query per repository
 * (an N+1 that grows with the number of repositories, on a path the daemon runs for
 * every `snapshot()` and every resync). The join is LEFT because an EMPTY ref set is a
 * valid observation — "nothing seen yet" — and an inner join would silently drop those
 * repositories from the listing rather than returning them with no refs; the nullable
 * `refName`/`refSha` pair is exactly that case, and it is decoded as `NullOr` rather
 * than assumed away.
 */
const RepositoryJoinedRow = Schema.Struct({
  id: RepositoryId,
  host: RepositoryHost,
  owner: RepositorySegment,
  name: RepositorySegment,
  observedAt: Timestamp,
  refName: Schema.NullOr(BranchName),
  refSha: Schema.NullOr(CommitSha),
});

/**
 * Fold {@link RepositoryJoinedRow}s back into {@link Repository} records.
 *
 * The query orders by `(id, name)`, so every row of one repository is contiguous and
 * its refs arrive already in the branch-name order `Repository.refs` requires — the
 * fold appends, it never sorts. A row whose `refName` is `null` is the LEFT-join's
 * "this repository has no refs" marker and contributes nothing to the list.
 *
 * The accumulated list is DECODED through the branded {@link RepositoryRefs} rather
 * than assigned: the ordering claim above rests on `ORDER BY name` matching the
 * domain's `compareBranchNames`, and decoding is what turns that from an assertion in a
 * comment into a check the read cannot skip. A backing whose collation ever disagreed
 * would fail the read loudly instead of handing a mis-ordered record to every client.
 */
const groupRepositories = (
  rows: ReadonlyArray<(typeof RepositoryJoinedRow)["Type"]>,
): Effect.Effect<ReadonlyArray<Repository>, Schema.SchemaError> => {
  const refsById = new Map<string, Array<RepositoryRef>>();
  const firstRowPerRepository: Array<(typeof RepositoryJoinedRow)["Type"]> = [];
  for (const row of rows) {
    let refs = refsById.get(row.id);
    if (refs === undefined) {
      refs = [];
      refsById.set(row.id, refs);
      firstRowPerRepository.push(row);
    }
    if (row.refName !== null && row.refSha !== null) {
      refs.push({ name: row.refName, sha: row.refSha });
    }
  }
  return Effect.forEach(firstRowPerRepository, (row) =>
    Schema.decodeUnknownEffect(RepositoryRefs)(refsById.get(row.id) ?? []).pipe(
      Effect.map(
        (refs): Repository => ({
          id: row.id,
          host: row.host,
          owner: row.owner,
          name: row.name,
          refs,
          observedAt: row.observedAt,
        }),
      ),
    ),
  );
};

/** A workstream row: `epics` is a JSON-encoded child list. */
const WorkstreamRow = Schema.Struct({
  id: WorkstreamId,
  name: Schema.NonEmptyString,
  repositoryId: RepositoryId,
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

/**
 * An execution row. `parent` is a nullable self-reference (absent on a root
 * execution); `transcript` is a JSON column holding the owned {@link Transcript}
 * union, stored WHOLE so the variant and its payload can never disagree — a `_tag`
 * column beside a nullable `lastOffset` would admit a sealed transcript with no
 * extent and a live one carrying one (INV-SUM).
 */
const ExecutionRow = Schema.Struct({
  id: ExecutionId,
  jobId: JobId,
  agentId: AgentId,
  parent: Schema.NullOr(ExecutionId),
  mode: ExecutionMode,
  transcript: Schema.fromJsonString(Transcript),
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

/** A job row: `executionId` / `transcriptRef` are nullable; `pr` is a nullable JSON column. */
const JobRow = Schema.Struct({
  id: JobId,
  issueId: IssueId,
  kind: JobKind,
  status: JobStatus,
  executionId: Schema.NullOr(ExecutionId),
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
 * An execution-transcript-log row: `event` is a JSON column holding the owned
 * {@link ExecutionEvent}; `offset` is the auto-assigned rowid (monotonic per execution by
 * ascending read, globally unique across executions). The `executionId` scoping column is
 * a query predicate, not part of the reconstructed {@link PersistedExecutionEvent}.
 */
const ExecutionEventRow = Schema.Struct({
  offset: NonNegativeInt,
  event: Schema.fromJsonString(ExecutionEvent),
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
 * appending a successor before its predecessor. Version 5 lands the `── STATE ──`
 * layer's `Repository` entity (DE1.2): the `repository` table with its `UNIQUE
 * (host, owner, name)` natural key, the `repository_ref` child table keyed
 * `(repositoryId, name)` with an `ON DELETE CASCADE` foreign key, and
 * `workstream.repositoryId` as a real FOREIGN KEY onto it (replacing the bare
 * `repo` TEXT column).
 *
 * That last one has a RESET consequence worth stating, because it is what makes the
 * foreign key hold rather than merely be declared: a bump drops `repository`,
 * `repository_ref` AND `workstream` TOGETHER (the sweep is over the whole database),
 * so a reset can never leave a workstream behind pointing at a repository row that no
 * longer exists. There is no window in which the reference dangles — the referencing
 * table does not outlive the referenced one.
 *
 * Version 6 is the process-level RENAME `Session` → `Execution` (DE2.1): the
 * `session` table becomes `execution`, `session_event_log` becomes
 * `execution_event_log` with its scoping column `"sessionId"` → `"executionId"`
 * and its index `session_event_log_session` → `execution_event_log_execution`,
 * the `session_job` unique index becomes `execution_job`, and `job."sessionId"`
 * becomes `job."executionId"`. No column was added, removed or retyped — but a
 * renamed table IS a shape change, and there is no migration ladder, so it drops
 * and recreates like any other (INV-FRESH).
 *
 * Version 7 gives `Execution` its real shape (DE2.2): `execution` gains `"agentId"`,
 * `parent` and `mode` columns and a `transcript` JSON column, LOSES its `status`
 * column (liveness is the transcript variant — see `Transcript` in
 * `@sprinter/domain`), and gains THREE foreign keys — `"jobId"` → `job (id)`,
 * `"agentId"` → `agent (id)`, `parent` → `execution (id)`. Its `UNIQUE execution_job`
 * index becomes a PLAIN index: one job now has a TREE of executions, so uniqueness
 * would refuse the model rather than protect it. `execution_event_log."executionId"`
 * also becomes a real foreign key onto `execution (id)`, so a transcript cannot exist
 * without the run that produced it (1 Execution = 1 Transcript).
 *
 * The same RESET consequence version 5 recorded for `workstream.repositoryId` is what
 * makes `"agentId"` hold: a bump drops `agent`, `job` and `execution` TOGETHER, so a
 * reset cannot leave an execution behind naming an agent revision (or a job) that no
 * longer exists. There is no window in which those references dangle — the referencing
 * table does not outlive the referenced one — which is what closes DE1.1's recorded
 * `INV-FRESH`-vs-registry-durability tension for referential integrity (see
 * `registry.ts`).
 *
 * Version 8 makes the execution TREE well-defined rather than merely intended: a
 * partial `UNIQUE INDEX execution_root ON execution ("jobId") WHERE parent IS NULL`.
 * Version 7 dropped `UNIQUE execution_job` because a job owns many executions — correct,
 * but it left "the job's ROOT execution" ({@link StateStore}'s `getExecutionForJob`)
 * undefined whenever two PARENTLESS rows existed for one job, which was an ordinary
 * write. The read's `ORDER BY id LIMIT 1` then picked between them by id collation and
 * called it deterministic. The partial index refuses the second root outright, so a tree
 * has one root by construction and the read has exactly one row it can return.
 *
 * The bump is observable only across a daemon RESTART: {@link applySchema} runs at
 * LAYER CONSTRUCTION, so a running daemon holds the generation it opened with and
 * a bump reaches a client at its next reconnect, never mid-connection. That bounds
 * WHEN the stale-cursor hazard can occur; it does not reduce it, which is why the
 * generation identity below is on the wire. The bound is also SINGLE-PROCESS only —
 * see {@link applySchema} for what a second process at a different version can do.
 */
export const SCHEMA_VERSION = 8;

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
  // The `── STATE ──` layer's anchor: a repository as OBSERVED on a code host (DE1.2).
  // It carries `observedAt` because it is REFERENCED, not owned (INV-OBSERVED); the
  // owned nodes below carry none.
  //
  // `id` is the PRIMARY KEY because it is what other rows reference, but it is NOT
  // what identifies a repository — `(host, owner, name)` is. Two rows for one
  // repository is the domain's own "two records disagreeing about the same thing", and
  // it is made UNCONSTRUCTIBLE by the UNIQUE index below rather than by a resolve-time
  // read-then-check, which a concurrent or reordered write walks straight past.
  //
  // KNOWN CONSEQUENCE: a STALE row's natural key blocks a different repository from
  // being renamed INTO it. See the note on `putRepository` for the sequence; the cause
  // is that nothing TRIGGERS a refresh, not this index.
  yield* sql`CREATE TABLE repository (
      id TEXT PRIMARY KEY NOT NULL,
      host TEXT NOT NULL,
      owner TEXT NOT NULL,
      name TEXT NOT NULL,
      "observedAt" TEXT NOT NULL
    )`;
  yield* sql`CREATE UNIQUE INDEX repository_natural_key ON repository (host, owner, name)`;
  // The OBSERVED ref map `BranchName → CommitSha`, as a CHILD TABLE rather than a JSON
  // column on `repository` (D4). The difference is enforcement: the composite PRIMARY
  // KEY makes a repeated branch name within ONE repository unstorable, where a blob
  // would leave "at most one tip per branch" to whoever assembled the JSON. The
  // FOREIGN KEY makes a ref naming an ABSENT repository unstorable for the same
  // reason, and `ON DELETE CASCADE` means the refs cannot outlive the repository they
  // describe — which is also the ONLY delete anywhere on this path (the port exposes
  // none).
  yield* sql`CREATE TABLE repository_ref (
      "repositoryId" TEXT NOT NULL,
      name TEXT NOT NULL,
      sha TEXT NOT NULL,
      PRIMARY KEY ("repositoryId", name),
      FOREIGN KEY ("repositoryId") REFERENCES repository (id) ON DELETE CASCADE
    )`;
  // `repositoryId` is a REAL foreign key onto `repository`, not a bare column holding
  // an id (and not the `repo TEXT` string it replaced). A workstream anchored to a
  // repository that was never observed is REJECTED by the engine on the INSERT —
  // never stored and reconciled later. That is the `supersedes` lesson from DE1.1
  // applied one task on: while a dangling reference was storable, every rule decided
  // by READING the referent was bypassable by writing in the other order.
  yield* sql`CREATE TABLE workstream (
      id TEXT PRIMARY KEY NOT NULL,
      name TEXT NOT NULL,
      "repositoryId" TEXT NOT NULL,
      status TEXT NOT NULL,
      epics TEXT NOT NULL,
      FOREIGN KEY ("repositoryId") REFERENCES repository (id)
    )`;
  yield* sql`CREATE INDEX workstream_repository ON workstream ("repositoryId")`;
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
      "executionId" TEXT,
      "transcriptRef" TEXT,
      pr TEXT
    )`;
  yield* sql`CREATE INDEX job_issue ON job ("issueId")`;
  // One agent's continuous run (DE2.2). THREE foreign keys, none of them decorative:
  //
  // - `"jobId"` → `job (id)`: the work this run advances. It is the `sessionId` of the
  //   target model under its pre-DE2.4 name (D1) — a real key NOW, so referential
  //   integrity holds at every intermediate state rather than arriving with `Session`.
  // - `"agentId"` → `agent (id)`: the exact registry revision that ran. Since the
  //   registry never rewrites a revision, a historical execution always resolves to the
  //   agent that actually ran it — and the key is what makes that a guarantee instead of
  //   a hope (an execution naming an unregistered agent is refused by the engine, and a
  //   reset drops both tables together so the reference cannot outlive its referent).
  // - `parent` → `execution (id)`: executions form a TREE. Acyclicity takes THREE
  //   things, and the key alone is only the first of them:
  //   1. the KEY — a child cannot name a parent that is not already stored, so a
  //      DANGLING parent is unstorable;
  //   2. `putExecution`'s SELF-parent refusal — the single edge a foreign key accepts,
  //      since a row satisfies a key against itself (exactly as `putAgent` rejects a
  //      self-`supersedes`);
  //   3. `parent` being INSERT-ONLY in `putExecution`'s upsert. This is the one the key
  //      says NOTHING about: it constrains the REFERENCED row, not the re-pointing of an
  //      EXISTING one, so while `parent` sat in the `DO UPDATE SET` list a 2-cycle was
  //      constructible in three ordinary writes (insert `a` rootless, insert `b` under
  //      `a`, upsert `a` under `b`) — each satisfying the key at the moment it ran.
  //   With (3) in place, closing a cycle of length ≥ 2 genuinely requires naming a row
  //   that does not exist yet, which (1) refuses, and the relation is acyclic BY
  //   CONSTRUCTION for real. This is the `supersedes` lesson from issue #85 applied to
  //   the tree: three separate lineage rules were all defeated by a reordered write
  //   until a foreign key made the precondition unconstructible — and the registry's
  //   version holds only because `putAgent` is a bare INSERT, which is the same property
  //   (3) restores here.
  //
  // There is NO `status` column: liveness IS the transcript variant (`Transcript`,
  // `@sprinter/domain`), and a status enum beside it would be a second field that must
  // agree with the first (INV-SUM / INV-ENFORCE). `transcript` is stored as ONE JSON
  // column for the same reason — the variant and its payload move together.
  yield* sql`CREATE TABLE execution (
      id TEXT PRIMARY KEY NOT NULL,
      "jobId" TEXT NOT NULL,
      "agentId" TEXT NOT NULL,
      parent TEXT,
      mode TEXT NOT NULL,
      transcript TEXT NOT NULL,
      FOREIGN KEY ("jobId") REFERENCES job (id),
      FOREIGN KEY ("agentId") REFERENCES agent (id),
      FOREIGN KEY (parent) REFERENCES execution (id)
    )`;
  // PLAIN, not UNIQUE (DE2.2). It was `UNIQUE` while the model was 1 Job = 1 execution;
  // a job now owns a TREE of executions, so uniqueness would refuse the model rather
  // than protect it.
  yield* sql`CREATE INDEX execution_job ON execution ("jobId")`;
  // A job's executions are a TREE, and a tree has ONE root. Uniqueness on `"jobId"` is
  // wrong (that is the index above), but uniqueness on `"jobId"` AMONG THE ROOTLESS rows
  // is exactly the model: a job may own any number of executions and at most one of them
  // has no parent. Without it `getExecutionForJob`'s "the root" was not a definition at
  // all — two parentless rows for one job were an ordinary write, and the `ORDER BY id
  // LIMIT 1` picked between them by id COLLATION, which is arbitrary, not deterministic.
  // A partial UNIQUE index makes the second root unstorable instead (INV-ENFORCE), so
  // the read's answer is the only row that can match rather than the first of several.
  yield* sql`CREATE UNIQUE INDEX execution_root ON execution ("jobId") WHERE parent IS NULL`;
  // Keeps the tree walk (a parent's children) and the foreign key's own referencing
  // scan cheap; SQLite indexes the referenced side of a key, never the referencing one.
  yield* sql`CREATE INDEX execution_parent ON execution (parent)`;
  yield* sql`CREATE TABLE event_log (
      "offset" INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL,
      payload TEXT NOT NULL
    )`;
  // The durable per-execution transcript log: one append-only row per
  // durable, transcript-grade execution event, scoped by "executionId". The AUTOINCREMENT
  // rowid is the monotonic offset — globally unique, so a re-dispatch of the same execution
  // id APPENDS with fresh higher offsets rather than reusing or resetting the sequence
  // (never a duplicated/corrupted offset). The `execution_event_log_execution` index keeps a
  // per-execution ordered read/tail cheap.
  //
  // `"executionId"` is a real FOREIGN KEY onto `execution (id)` (DE2.2), not a bare
  // scoping column: `Execution 1:1 Transcript`, so a transcript that names no stored
  // run is not a transcript — it is an orphan whose owner nothing can resolve. With the
  // key, an entry for an unknown execution is refused by the engine at the append, so
  // "every durable entry belongs to an execution that exists" is a property of the
  // store rather than a claim about its writers.
  yield* sql`CREATE TABLE execution_event_log (
      "offset" INTEGER PRIMARY KEY AUTOINCREMENT,
      "executionId" TEXT NOT NULL,
      event TEXT NOT NULL,
      FOREIGN KEY ("executionId") REFERENCES execution (id)
    )`;
  yield* sql`CREATE INDEX execution_event_log_execution ON execution_event_log ("executionId")`;
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
        "SQLite refused `PRAGMA foreign_keys = ON`; the schema's foreign keys (the agent registry's `supersedes`, `repository_ref.repositoryId`, `workstream.repositoryId`, the execution's `jobId`/`agentId`/`parent`, and `execution_event_log.executionId`) would not be enforced, leaving dangling references storable",
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

  // ── RepositoryStore ─────────────────────────────────────────────────────
  //
  // Put + read only: there is no DELETE statement here, by design (see `store.ts`).
  // The one removal on this path is the `ON DELETE CASCADE` that clears a repository's
  // refs with the repository itself, which only a store reset performs.

  /**
   * Reconstruct one {@link Repository} from its scalar row plus its observed refs,
   * ordered by branch name. `operation` is the calling store method so a failure
   * reports the caller, not a fixed label.
   */
  const hydrateRepository = (
    row: unknown,
    operation: string,
  ): Effect.Effect<Repository, StateStoreError> =>
    Effect.gen(function* () {
      const base = yield* Schema.decodeUnknownEffect(RepositoryScalarRow)(row);
      const refRows =
        yield* sql`SELECT name, sha FROM repository_ref WHERE "repositoryId" = ${base.id} ORDER BY name`;
      const refs = yield* Schema.decodeUnknownEffect(RepositoryRefRows)(refRows);
      const repository: Repository = {
        id: base.id,
        host: base.host,
        owner: base.owner,
        name: base.name,
        refs,
        observedAt: base.observedAt,
      };
      return repository;
    }).pipe(Effect.mapError(fail(operation)));

  const repositories: RepositoryStore = {
    // A refresh REPLACES the record wholesale (D7) — scalars AND the whole ref set —
    // so a branch deleted upstream disappears rather than lingering from an older
    // read, and the stored record always describes ONE coherent moment. Both writes
    // run in ONE transaction, so a crash can never leave the new `observedAt`
    // alongside the previous observation's refs.
    //
    // There is no read-then-check for "is this natural key already taken by another
    // id": the `repository_natural_key` UNIQUE index answers it atomically, on the
    // INSERT, where a concurrent racing write cannot slip between the look and the
    // leap (INV-ENFORCE). A violation surfaces as an ordinary `StateStoreError`.
    //
    // `DO UPDATE SET` covers the NATURAL KEY columns too, and that is deliberate rather
    // than incidental: the id an adapter mints comes from the host's own stable
    // identifier, so a RENAMED repository refreshes under the SAME id with a DIFFERENT
    // `(host, owner, name)`. The conflict is on `id`, so this MOVES the existing row to
    // the new name — one row, every reference to it still valid — and the UNIQUE index
    // is satisfied because the old triple leaves the table in the same statement.
    // Deriving the id from the natural key instead would make that case an INSERT, and
    // the store would end up with two rows for one repository.
    //
    // KNOWN GAP (stated, not fixed here): a rename only moves the row when the CONFLICT
    // IS ON `id`. When it is not, a STALE row can permanently block a VALID repository:
    //
    //   1. Repository B (host id 2) is observed at `owner/X` — row stored, natural key `X`.
    //   2. B is renamed on the host to `owner/Y`. NOTHING refreshes it (there is no
    //      production refresh trigger), so our row still claims `X`.
    //   3. Repository A (host id 1) is renamed on the host into `owner/X`.
    //   4. Storing A inserts id 1 with natural key `X`. The ids differ, so `ON CONFLICT
    //      (id)` does NOT fire; it collides with the stale B row on the
    //      `repository_natural_key` UNIQUE index and surfaces as a `StateStoreError` —
    //      permanently, for a repository that is entirely valid on the host.
    //
    // The root cause is the ABSENT REFRESH TRIGGER, not the index: with a trigger, B's
    // row would move to `Y` and A would land. Evicting the stale row or resolving the
    // conflict HERE would invent policy DE1.2 has no basis to choose (which of the two
    // observations is the current one is exactly what only a refresh can answer), so the
    // behaviour is PINNED by a test in `store.test.ts` rather than changed, and the fix
    // is recorded against DE4.4 with the trigger it depends on.
    //
    // What the CALLER must not do with it is die. This failure is host-caused and
    // permanent, not a broken store, so the one user-facing caller
    // (`createWorkstreamFromPlan`) turns it into a `PlanRejected` naming the conflicting
    // key instead of an unmodelled defect — see `packages/daemon/src/rpc-handlers.ts`.
    putRepository: (repository) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const row = {
              id: repository.id,
              host: repository.host,
              owner: repository.owner,
              name: repository.name,
              observedAt: repository.observedAt,
            };
            yield* sql`INSERT INTO repository ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id"])}`;
            yield* sql`DELETE FROM repository_ref WHERE "repositoryId" = ${repository.id}`;
            if (repository.refs.length > 0) {
              const refs = repository.refs.map((ref) => ({
                repositoryId: repository.id,
                name: ref.name,
                sha: ref.sha,
              }));
              yield* sql`INSERT INTO repository_ref ${sql.insert(refs)}`;
            }
          }),
        )
        .pipe(Effect.mapError(fail("putRepository"))),
    getRepository: (id) =>
      sql`SELECT * FROM repository WHERE id = ${id}`.pipe(
        Effect.mapError(fail("getRepository")),
        Effect.flatMap((rows) =>
          Option.match(Arr.head(rows), {
            onNone: () => Effect.succeedNone,
            onSome: (row) => Effect.asSome(hydrateRepository(row, "getRepository")),
          }),
        ),
      ),
    // At most one row can match: the triple is UNIQUE in the backing, so taking the
    // head is deterministic by construction rather than "the first of whatever came
    // back". Staleness is NOT consulted (D7) — an old observation is still the
    // answer, and hiding it would delete the evidence DE4.4 renders from.
    findRepository: (key) =>
      sql`SELECT * FROM repository WHERE host = ${key.host} AND owner = ${key.owner} AND name = ${key.name}`.pipe(
        Effect.mapError(fail("findRepository")),
        Effect.flatMap((rows) =>
          Option.match(Arr.head(rows), {
            onNone: () => Effect.succeedNone,
            onSome: (row) => Effect.asSome(hydrateRepository(row, "findRepository")),
          }),
        ),
      ),
    // ONE joined read, not one query per repository. `hydrateRepository` issues a
    // `repository_ref` SELECT of its own, which is right for a single-record read and
    // wrong here: the daemon lists every repository for every `snapshot()` and every
    // resync, so a per-row ref query is an N+1 on the hottest read the store has.
    //
    // `ORDER BY r.id, f.name` does double duty — it groups each repository's rows
    // contiguously so the fold is a single pass, and it delivers the refs in the
    // branch-name order `Repository.refs` is checked for. That order is SQLite's default
    // BINARY collation (a `memcmp` over UTF-8), which is exactly the domain's
    // `compareBranchNames` (Unicode code point); the two are the same order by
    // construction, so a record's refs cannot be reordered by a store round-trip.
    //
    // That equivalence holds because the column is UTF-8 `TEXT` and every `BranchName`
    // is UTF-8-encodable: `BranchName` rejects an UNPAIRED SURROGATE, which is the one
    // JavaScript string a UTF-8 column cannot hold intact (it would be substituted or
    // refused, taking the round-trip claim with it). The rule lives in the domain, at
    // the boundary such a name would enter through, rather than here.
    //
    // The fold decodes through the branded `RepositoryRefs`, so if a backing's collation
    // ever DID disagree, the read would fail loudly rather than hand a mis-ordered record
    // to every client.
    listRepositories: sql`
      SELECT r.id, r.host, r.owner, r.name, r."observedAt",
             f.name AS "refName", f.sha AS "refSha"
      FROM repository r
      LEFT JOIN repository_ref f ON f."repositoryId" = r.id
      ORDER BY r.id, f.name
    `.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(RepositoryJoinedRow))),
      Effect.flatMap(groupRepositories),
      Effect.mapError(fail("listRepositories")),
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
          ...(r.executionId !== null ? { executionId: r.executionId } : {}),
          ...(r.transcriptRef !== null ? { transcriptRef: r.transcriptRef } : {}),
          ...(r.pr !== null ? { pr: r.pr } : {}),
        }),
      ),
      Effect.mapError(fail(operation)),
    );

  /**
   * Reconstruct one {@link Execution} from its row, dropping the SQL `NULL` parent to
   * an absent optional. `operation` names the calling store method so a decode failure
   * reports the caller.
   */
  const hydrateExecution = (
    row: unknown,
    operation: string,
  ): Effect.Effect<Execution, StateStoreError> =>
    Schema.decodeUnknownEffect(ExecutionRow)(row).pipe(
      Effect.map(
        (r): Execution => ({
          id: r.id,
          jobId: r.jobId,
          agentId: r.agentId,
          ...(r.parent !== null ? { parent: r.parent } : {}),
          mode: r.mode,
          transcript: r.transcript,
        }),
      ),
      Effect.mapError(fail(operation)),
    );

  /**
   * Read at most one execution row through {@link hydrateExecution}. Shared by the two
   * reads so the `NULL`-parent handling is written once.
   */
  const findExecution = (
    rows: ReadonlyArray<unknown>,
    operation: string,
  ): Effect.Effect<Option.Option<Execution>, StateStoreError> =>
    Option.match(Arr.head(rows), {
      onNone: () => Effect.succeedNone,
      onSome: (row) => Effect.asSome(hydrateExecution(row, operation)),
    });

  const getExecutionForJobQuery = (jobId: JobId) =>
    // The job's ROOT execution: `parent IS NULL`. A job owns a TREE now (DE2.2 dropped
    // `UNIQUE execution_job`), so "the execution for this job" needs a definition rather
    // than an assumption that only one row can match. The definition holds because the
    // `execution_root` partial UNIQUE index makes a SECOND rootless row for one job
    // unstorable — that, not the `LIMIT 1`, is what makes the answer deterministic. The
    // `ORDER BY id LIMIT 1` is kept as a plan hint only; with the index there is at most
    // one candidate to order. DE2.4 replaces this read entirely: a `Session` names its
    // `root` execution outright, so the root stops being something to look up.
    sql`SELECT * FROM execution WHERE "jobId" = ${jobId} AND parent IS NULL ORDER BY id LIMIT 1`;

  const jobs: JobStore = {
    putJob: (job) =>
      Effect.gen(function* () {
        const pr = yield* Schema.encodeEffect(PrColumn)(job.pr ?? null);
        const row = {
          id: job.id,
          issueId: job.issueId,
          kind: job.kind,
          status: job.status,
          executionId: job.executionId ?? null,
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
    putExecution: (execution) =>
      Effect.gen(function* () {
        // The ONE tree edge the `parent` foreign key does NOT reject: SQLite checks the
        // constraint against the table INCLUDING the row being inserted, so a row is its
        // own valid parent. Rejecting it here is what makes the whole relation acyclic —
        // every longer cycle needs an edge naming a row that does not exist yet, which
        // the key already refuses. (The same division of labour as `putAgent`'s
        // self-`supersedes` check.)
        if (execution.parent === execution.id) {
          return yield* Effect.fail(
            new StateStoreError({
              operation: "putExecution",
              detail: `execution "${execution.id}" names itself as its parent; an execution's parent must be a DIFFERENT execution`,
            }),
          );
        }
        const transcript = yield* Schema.encodeEffect(Schema.fromJsonString(Transcript))(
          execution.transcript,
        ).pipe(Effect.mapError(fail("putExecution")));
        const row = {
          id: execution.id,
          jobId: execution.jobId,
          agentId: execution.agentId,
          parent: execution.parent ?? null,
          mode: execution.mode,
          transcript,
        };
        // `parent` is INSERT-ONLY: it is absent from the `DO UPDATE SET` list, so an
        // upsert of an EXISTING id cannot re-point its edge. That omission is what makes
        // the foreign key mean what its docstring claims — the key only guarantees the
        // REFERENCED row exists, and while `parent` was in the updated set a 2-cycle was
        // constructible in three ordinary writes (insert `a` rootless, insert `b` under
        // `a`, upsert `a` under `b`), leaving the job with no `parent IS NULL` row at all.
        // Frozen at insert, closing a cycle genuinely requires naming a row that does not
        // exist yet, which the key refuses (INV-ENFORCE: unconstructible, not checked).
        //
        // The `WHERE … IS excluded.parent` makes the freeze LOUD rather than silent, and
        // the ENGINE decides it inside the one statement — no read-then-write window a
        // concurrent writer can slip through. A re-attach (the intended upsert) passes the
        // same `parent` and updates normally; a re-parent matches no row, updates nothing,
        // and `RETURNING` comes back empty, which is the failure below. `IS` is SQLite's
        // null-safe equality, so a rootless execution's `NULL` compares as a value.
        const written =
          yield* sql`INSERT INTO execution ${sql.insert(row)} ON CONFLICT (id) DO UPDATE SET ${sql.update(row, ["id", "parent"])} WHERE execution.parent IS excluded.parent RETURNING id`.pipe(
            Effect.mapError(fail("putExecution")),
          );
        if (written.length === 0) {
          return yield* Effect.fail(
            new StateStoreError({
              operation: "putExecution",
              detail: `execution "${execution.id}" is already stored with a DIFFERENT parent; an execution's parent is fixed at its first write — re-parenting is not an operation an execution has`,
            }),
          );
        }
      }),
    getExecution: (id) =>
      sql`SELECT * FROM execution WHERE id = ${id}`.pipe(
        Effect.mapError(fail("getExecution")),
        Effect.flatMap((rows) => findExecution(rows, "getExecution")),
      ),
    getExecutionForJob: (jobId) =>
      getExecutionForJobQuery(jobId).pipe(
        Effect.mapError(fail("getExecutionForJob")),
        Effect.flatMap((rows) => findExecution(rows, "getExecutionForJob")),
      ),
    listExecutionsForJob: (jobId) =>
      sql`SELECT * FROM execution WHERE "jobId" = ${jobId} ORDER BY id`.pipe(
        Effect.mapError(fail("listExecutionsForJob")),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) => hydrateExecution(row, "listExecutionsForJob")),
        ),
      ),
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

  // ── ExecutionLogStore ──────────────────────────────────────────────────────────────────────

  const executionLog: ExecutionLogStore = {
    append: (executionId, event) =>
      Effect.gen(function* () {
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ExecutionEvent))(event);
        const rows =
          yield* sql`INSERT INTO execution_event_log ${sql.insert({ executionId, event: encoded })} RETURNING "offset"`;
        const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(OffsetRow))(rows);
        const persisted: PersistedExecutionEvent = { offset: decoded[0].offset, event };
        return persisted;
      }).pipe(Effect.mapError(fail("executionLog.append"))),
    // The base store owns DURABILITY, not the live feed: an ephemeral delta is not persisted
    // here, so this is a total no-op. The daemon's journaling decorator overrides it to fan
    // the delta out on the `ExecutionEvents` feed offset-less.
    publishEphemeral: () => Effect.void,
    read: (executionId) =>
      sql`SELECT "offset", event FROM execution_event_log WHERE "executionId" = ${executionId} ORDER BY "offset"`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ExecutionEventRow))),
        Effect.mapError(fail("executionLog.read")),
      ),
    tail: (executionId, offset) =>
      sql`SELECT "offset", event FROM execution_event_log WHERE "executionId" = ${executionId} AND "offset" > ${offset} ORDER BY "offset"`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ExecutionEventRow))),
        Effect.mapError(fail("executionLog.tail")),
      ),
    // A `COUNT(*)` with a `json_extract` tag filter — the count is computed in SQLite and
    // never materializes or JSON/Schema-decodes the transcript rows (unlike `read`), so it
    // stays cheap on a long or many-times-retried execution.
    countEntries: (executionId) =>
      sql`SELECT COUNT(*) AS n FROM execution_event_log WHERE "executionId" = ${executionId} AND json_extract(event, '$._tag') = 'EntryAppended'`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.NonEmptyArray(CountRow))),
        Effect.map((rows) => rows[0].n),
        Effect.mapError(fail("executionLog.countEntries")),
      ),
    // The per-execution extent, answered by the backing over the `execution_event_log_execution`
    // index — no row is materialized or JSON-decoded, which is the whole point: the
    // `executionEvents` cursor guard runs BEFORE the transcript read, so a request that is
    // about to be refused never pays for one. `COALESCE` turns the SQL `NULL` an
    // entry-less execution yields into `0`, below every durable offset (strictly `> 0`).
    maxOffset: (executionId) =>
      sql`SELECT COALESCE(MAX("offset"), 0) AS n FROM execution_event_log WHERE "executionId" = ${executionId}`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.NonEmptyArray(CountRow))),
        Effect.map((rows) => rows[0].n),
        Effect.mapError(fail("executionLog.maxOffset")),
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
    repositories,
    workGraph: putWorkGraph,
    jobs,
    events,
    executionLog,
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
