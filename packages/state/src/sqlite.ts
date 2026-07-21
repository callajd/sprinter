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
 * hazard, not bookkeeping, and it is why the `events` RPC carries an explicit
 * `ResyncRequired` contract error rather than leaving a stale cursor to be inferred
 * from offsets (see `packages/contract/src/rpc.ts`).
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
 * Version 1 is the greenfield reset that REPLACED the previous incremental
 * migration ladder (`1_initial` / `2_session_event_log`). A database left by that
 * ladder has `user_version = 0`, so it mismatches and is reset like any other
 * stale store.
 */
export const SCHEMA_VERSION = 1;

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
 */
const createSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // The append-only Agent REGISTRY: owned, global, scoped to NO repository — hence
  // no repositoryId/workstreamId column and no per-repo join table ("agents used in
  // this repo" is a fold over that repo's executions, INV-DERIVED). `retiredAt` is a
  // nullable stamp, not a status column (INV-SUM). Nothing DELETEs from this table.
  yield* sql`CREATE TABLE agent (
      id TEXT PRIMARY KEY NOT NULL,
      name TEXT NOT NULL,
      model TEXT NOT NULL,
      version TEXT NOT NULL,
      tools TEXT NOT NULL,
      supersedes TEXT,
      "retiredAt" TEXT
    )`;
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
 */
const applySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`PRAGMA user_version`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(UserVersionRow))(rows);
  if (decoded[0].user_version === SCHEMA_VERSION) return;
  yield* sql.withTransaction(
    Effect.gen(function* () {
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
   * Enforce the LIFECYCLE-ONLY retirement rule: a revision that carries both
   * `supersedes` and `retiredAt` must repeat the superseded revision's
   * non-lifecycle columns verbatim.
   *
   * Compared on the STORED BYTES (`tools` stays the encoded JSON string, as
   * {@link AgentColumns} models it), so "same content" is decided exactly as the
   * column stores it rather than by a looser re-decoded comparison — the same basis
   * {@link isSameAgentRow} uses for the idempotent re-append.
   *
   * A superseded revision that is NOT stored is not a failure here: the rule is
   * unverifiable without it, and a dangling `supersedes` is already the writer's
   * obligation under `registry.ts`'s acyclicity precondition. The check runs inside
   * the caller's transaction, so the row it reads is the one the insert commits
   * against.
   */
  const assertRetirementPreservesContent = (
    supersedes: AgentId,
    row: AgentColumns,
  ): Effect.Effect<void, StateStoreError> =>
    sql`SELECT * FROM agent WHERE id = ${supersedes}`.pipe(
      Effect.mapError(fail("putAgent")),
      Effect.flatMap((rows) =>
        Option.match(Arr.head(rows), {
          onNone: () => Effect.void,
          onSome: (found) =>
            Schema.decodeUnknownEffect(AgentColumns)(found).pipe(
              Effect.mapError(fail("putAgent")),
              Effect.flatMap((head) =>
                isSameAgentContent(head, row)
                  ? Effect.void
                  : Effect.fail(
                      new StateStoreError({
                        operation: "putAgent",
                        detail: `agent "${row.id}" retires "${supersedes}" but changes its content; a retirement sets retiredAt ONLY — append the edit as its own revision first, then retire that revision`,
                      }),
                    ),
              ),
            ),
        }),
      ),
    );

  const agents: AgentStore = {
    putAgent: (agent) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            // A revision that supersedes ITSELF is the one `supersedes` cycle a
            // single append can create, and it would make a consumer's backwards
            // walk of the chain non-terminating — so it is rejected at the port
            // (`registry.ts` states the full acyclicity precondition).
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
            // A RETIREMENT is LIFECYCLE-ONLY: it may set `retiredAt` and nothing
            // else. Fusing a content edit into it would collapse two distinct
            // operations into one indistinguishable append — a reader walking back
            // from the retiring revision could no longer tell whether the lineage
            // was edited, retired, or both, and the retired head's content would
            // appear to have changed at the moment it went out of service. So a
            // revision carrying BOTH `supersedes` and `retiredAt` must match the
            // revision it supersedes on every non-lifecycle column. Checkable only
            // when that revision is actually stored; a dangling `supersedes` is the
            // same writer obligation the acyclicity precondition already carries.
            if (agent.retiredAt !== undefined && agent.supersedes !== undefined) {
              yield* assertRetirementPreservesContent(agent.supersedes, row);
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
export const layer = (config: StateStoreConfig): Layer.Layer<StateStore, StateStoreError> => {
  const disableWAL = config.disableWAL ?? config.filename === ":memory:";
  const clientLayer = SqliteClient.layer({ filename: config.filename, disableWAL });
  const schemaLayer = Layer.effectDiscard(applySchema.pipe(Effect.mapError(fail("applySchema"))));
  return Layer.effect(StateStore, make).pipe(
    Layer.provide(schemaLayer),
    Layer.provide(clientLayer),
  );
};

/** Convenience adapter for an ephemeral in-memory database (deterministic, offline — tests). */
export const layerMemory: Layer.Layer<StateStore, StateStoreError> = layer({
  filename: ":memory:",
});
