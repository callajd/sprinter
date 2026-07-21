/**
 * `INV-FRESH` — the schema-version mechanism that REPLACED the incremental
 * migration ladder (DE1.1). The store is greenfield: it never migrates. There is
 * one constant, {@link SCHEMA_VERSION}, and on any `PRAGMA user_version` mismatch
 * the adapter DROPS every table and recreates the schema.
 *
 * These tests drive a real FILE-backed database (a temp file, removed afterwards)
 * because the property under test is what happens when an EXISTING store is
 * reopened — an in-memory database cannot express that. Each case uses the public
 * `@sprinter/state` surface only (INV-PORT); the one raw `bun:sqlite` handle is the
 * TEST's own probe of the on-disk state, not production code.
 */
import { it } from "@effect/vitest";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Context, Effect, Option, Schema } from "effect";
import { expect } from "vitest";
import { Agent, Repository, Workstream } from "@sprinter/domain";
import { layer, SCHEMA_VERSION, StateStore } from "./index.ts";

/** A fresh temp database path, torn down with the surrounding scope. */
const dbFile = Effect.acquireRelease(
  Effect.sync(() => mkdtempSync(join(tmpdir(), "sprinter-state-"))),
  (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true })),
).pipe(Effect.map((dir) => join(dir, "state.db")));

const repository = Schema.decodeUnknownEffect(Repository)({
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
}).pipe(Effect.orDie);

/** A workstream ANCHORED to {@link repository} — the FK-bearing shape a reset must clear. */
const anchoredWorkstream = Schema.decodeUnknownEffect(Workstream)({
  id: "ws-a",
  name: "Track A",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: [],
}).pipe(Effect.orDie);

const agent = Schema.decodeUnknownEffect(Agent)({
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read"],
}).pipe(Effect.orDie);

/** Open the store on `filename`, run `use`, and close it before returning. */
const withStore = <A>(
  filename: string,
  use: (store: Context.Service.Shape<typeof StateStore>) => Effect.Effect<A, never>,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    return yield* use(store);
  }).pipe(Effect.provide(layer({ filename, disableWAL: true })), Effect.orDie);

/**
 * The LEDGER that binds every emitted schema to the version it belongs to: a
 * `version → SHA-256 of the DDL SQLite actually stores` table, appended to (never
 * edited) as {@link SCHEMA_VERSION} advances.
 *
 * A ledger rather than a single `{ version, sha256 }` pair, because the pair could
 * be defeated by the exact mistake this guard exists to catch: edit `createSchema`,
 * leave the version at 1, and update the ONE digest in place — both fields still
 * agree, the test passes, and an existing store at `user_version = 1` is silently
 * left holding the OLD shape (INV-FRESH's whole failure mode). A version that has
 * already shipped a digest cannot be re-used for a different one here: rewriting an
 * existing row is the failure, and the only clean way forward is a NEW row under a
 * NEW version — which is exactly the bump.
 *
 * When the schema changes: bump {@link SCHEMA_VERSION} and ADD a row. Do not edit an
 * existing row.
 *
 * **HISTORICAL ROWS ARE DOCUMENTATION, NOT VERIFIED GUARDS.** Only the row for the
 * CURRENT {@link SCHEMA_VERSION} is re-derived and compared below; every earlier row
 * pins DDL that no longer exists anywhere in the tree, so its digest can never be
 * recomputed from source and nothing here can detect a typo'd, corrupted, or invented
 * historical value. What the older rows genuinely enforce is DISTINCTNESS — a version
 * may not silently re-claim a shipped digest — which is what makes the pin
 * un-defeatable going forward. Read them as a changelog of shapes that once shipped,
 * not as a checked assertion about them. (Nothing stronger is available: INV-FRESH
 * never migrates, so no past schema survives to be reconstructed and re-hashed.)
 */
const SCHEMA_LEDGER: Readonly<Record<number, string>> = {
  1: "12cfac61b40228489b1fbd68c13b9660799e48089d12ee0b30043e7668d604f0",
  2: "4fe63e6cce2cfabc2382c99d73347aec6a08fdbf58e42791be6cfeb93f960eac",
  3: "e005c3b880ba0bb3f0cb1645504d09d31ed3dd91ab9174b7ea1e03764fa84f7b",
  4: "ac07e2636d90f3fa43cf7265cc16747c29ba0af4b822461873b1f1ec24fd9572",
  5: "45ac3c1188e9d0fed3d26ce55a38b838389bc137da0974c3a2100ece0c9f599f",
  6: "a9753b45791103db9e9384ad15948e8a9ee58a1060370410c0b5a6b521c5b860",
};

/** The DDL of every object in the database, canonically ordered — the pinned text. */
const emittedSchema = (filename: string): string => {
  const probe = new Database(filename);
  const rows = probe
    .query(
      `SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`,
    )
    .all();
  probe.close();
  return JSON.stringify(rows, null, 2);
};

it.effect("pins the emitted schema to SCHEMA_VERSION — a shape change must bump it", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    // Build the schema through the adapter itself, then read back what SQLite
    // actually stored: every table, every column, every index.
    yield* withStore(filename, () => Effect.void);
    const ddl = emittedSchema(filename);
    const digest = createHash("sha256").update(ddl).digest("hex");

    const fix =
      "The persisted schema changed. A bump touches EXACTLY TWO FILES — both, or " +
      "neither:\n" +
      "  1. packages/state/src/sqlite.ts — increment `SCHEMA_VERSION` (existing " +
      "stores must drop and recreate; INV-FRESH never migrates).\n" +
      "  2. packages/state/src/schema-version.test.ts (THIS FILE) — ADD a row to " +
      "`SCHEMA_LEDGER` mapping the NEW version to the digest below.\n" +
      "Do NOT rewrite an existing ledger row: re-using a version for a different " +
      `shape is the exact failure this guards. Digest of the emitted schema: ${digest}` +
      `\nEmitted schema was:\n${ddl}`;

    // The current version must be LEDGERED, and its ledgered digest must be the one
    // the adapter just emitted. Editing `createSchema` without bumping therefore fails
    // on the digest of an ALREADY-CLAIMED version — and the only way to make it pass is
    // to add a new row under a new version, i.e. to bump.
    expect(Object.keys(SCHEMA_LEDGER).map(Number), fix).toContain(SCHEMA_VERSION);
    expect(SCHEMA_LEDGER[SCHEMA_VERSION], fix).toBe(digest);
  }).pipe(Effect.scoped),
);

it("ledgers each schema version exactly once, under a distinct digest", () => {
  // The ledger's own well-formedness — what makes the pin above un-defeatable. A
  // JS object cannot hold a duplicate key, so uniqueness of the VERSION is
  // structural; what has to be asserted is the other direction: no two versions may
  // claim the same DDL (that would mean a bump with no shape change, i.e. a pin
  // rewritten rather than appended), and the current version may never exceed the
  // highest ledgered one (a bump with no row added).
  const versions = Object.keys(SCHEMA_LEDGER).map(Number);
  const digests = Object.values(SCHEMA_LEDGER);
  expect(new Set(digests).size).toBe(digests.length);
  expect(Math.max(...versions)).toBe(SCHEMA_VERSION);
});

it.effect("stamps the schema version on a fresh store and preserves data across reopen", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    const value = yield* agent;

    yield* withStore(filename, (store) => store.agents.putAgent(value).pipe(Effect.orDie));

    // The version constant is stamped into the database itself — that stamp is what
    // the next open compares against.
    const probe = new Database(filename);
    const stamped = probe.query("PRAGMA user_version").get();
    probe.close();
    expect(stamped).toStrictEqual({ user_version: SCHEMA_VERSION });

    // Reopening at the SAME version is a no-op: no drop, so the data survives.
    const reread = yield* withStore(filename, (store) =>
      store.agents.getAgent(value.id).pipe(Effect.orDie),
    );
    expect(Option.getOrThrow(reread)).toStrictEqual(value);
  }).pipe(Effect.scoped),
);

it.effect("drops and recreates the store on a version mismatch — it never migrates", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    const value = yield* agent;
    yield* withStore(filename, (store) => store.agents.putAgent(value).pipe(Effect.orDie));

    // Simulate a store written by a DIFFERENT schema version — exactly what bumping
    // SCHEMA_VERSION looks like to an already-deployed database. Also plant the
    // bookkeeping table the retired migration ladder used, so the reset is proven to
    // clear the old mechanism's ghost too.
    const stale = new Database(filename);
    stale.run(`PRAGMA user_version = ${SCHEMA_VERSION + 1}`);
    stale.run(`CREATE TABLE IF NOT EXISTS effect_sql_migrations (id INTEGER PRIMARY KEY)`);
    // A table the CURRENT schema knows nothing about — a table a future version
    // renames or drops looks exactly like this to the reset. The drop list is read
    // from `sqlite_master`, not hardcoded, so it is swept with no special case.
    stale.run(`CREATE TABLE IF NOT EXISTS retired_from_a_future_version (id TEXT PRIMARY KEY)`);
    // And the OTHER droppable kinds `sqlite_master` can hold. A table-only sweep
    // would leave these behind while claiming to clear whatever the database holds:
    // the standalone view survives, and the trigger keeps FIRING on the recreated
    // `agent` table — a stale object silently mutating the new schema's writes.
    stale.run(`CREATE VIEW IF NOT EXISTS stale_view AS SELECT id FROM agent`);
    stale.run(
      `CREATE TRIGGER IF NOT EXISTS stale_trigger AFTER INSERT ON agent BEGIN INSERT INTO retired_from_a_future_version (id) VALUES (NEW.id); END`,
    );
    stale.close();

    const after = yield* withStore(filename, (store) => store.agents.listAgents.pipe(Effect.orDie));
    // No data preserved and no migration written: the store came back EMPTY.
    expect(after).toStrictEqual([]);

    const probe = new Database(filename);
    expect(probe.query("PRAGMA user_version").get()).toStrictEqual({
      user_version: SCHEMA_VERSION,
    });
    // Nothing the previous version left behind survives, of ANY droppable kind —
    // table, view, or trigger.
    const ghosts = probe
      .query(
        `SELECT name FROM sqlite_master WHERE name IN ('effect_sql_migrations', 'retired_from_a_future_version', 'stale_view', 'stale_trigger')`,
      )
      .all();
    probe.close();
    expect(ghosts).toStrictEqual([]);
  }).pipe(Effect.scoped),
);

it.effect("mints a FRESH generation on every drop-and-recreate, and only then", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;

    // The identity is minted by `createSchema`, so it is born with the schema and dies
    // with it. Two properties have to hold together, and neither alone is enough:
    const first = yield* withStore(filename, (store) => Effect.succeed(store.generation));

    // (1) STABLE across a reopen at the SAME version. A store that re-minted on every
    // open would refuse every legitimate reconnect — the cursor guard would be a
    // permanent "resync", i.e. exactly the snapshot-on-connect regression it exists to
    // avoid — and it would be indistinguishable from a real reset.
    const reopened = yield* withStore(filename, (store) => Effect.succeed(store.generation));
    expect(reopened).toBe(first);

    // (2) DIFFERENT after a version mismatch drops and recreates the database. This is
    // the whole mechanism: the offsets restart at 1, so the new coordinate space MUST
    // be distinguishable from the old one, or a client's stale cursor stays silently
    // resumable the moment the new log outgrows it.
    const stale = new Database(filename);
    stale.run(`PRAGMA user_version = ${SCHEMA_VERSION + 1}`);
    stale.close();
    const afterReset = yield* withStore(filename, (store) => Effect.succeed(store.generation));
    expect(afterReset).not.toBe(first);
  }).pipe(Effect.scoped),
);

it.effect(
  "enforces foreign keys on EVERY connection, including a reopen that skips the reset",
  () =>
    Effect.gen(function* () {
      const filename = yield* dbFile;
      const value = yield* agent;

      // SQLite defaults `PRAGMA foreign_keys` OFF and the setting is PER-CONNECTION, not
      // a property of the file — declaring the key in the DDL enforces nothing on its
      // own. So the case that matters is the SECOND open: `applySchema` sees a matching
      // `user_version` and returns EARLY, running no DDL at all, and a connection that
      // only turned the pragma on as part of building the schema would silently serve
      // writes with `agent.supersedes` unenforced — which is exactly the state in which
      // every lineage rule becomes bypassable by reversing the write order.
      yield* withStore(filename, (store) => store.agents.putAgent(value).pipe(Effect.orDie));

      const rejected = yield* withStore(filename, (store) =>
        Effect.gen(function* () {
          const dangling = yield* Schema.decodeUnknownEffect(Agent)({
            id: "agt-2",
            name: "implementer",
            model: "claude-opus-4-8",
            version: "1.0.0",
            tools: ["read"],
            supersedes: "agt-absent",
          }).pipe(Effect.orDie);
          // `flip` makes the REJECTION the success; a write that succeeded would flip
          // into the error channel and `orDie` would fail the test rather than pass it.
          return yield* store.agents.putAgent(dangling).pipe(Effect.flip, Effect.orDie);
        }),
      );
      expect(rejected.operation).toBe("putAgent");

      // And nothing landed: the reopened store still holds exactly the original revision.
      const after = yield* withStore(filename, (store) =>
        store.agents.listAgents.pipe(Effect.orDie),
      );
      expect(after).toStrictEqual([value]);
    }).pipe(Effect.scoped),
);

it.effect("a version bump drops repository, repository_ref AND workstream TOGETHER", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;

    // Seed a fully-anchored graph: a repository, its observed refs, and a workstream
    // REFERENCING it. This is the shape the `workstream.repositoryId` FOREIGN KEY
    // protects, and the shape a reset has to be able to clear COHERENTLY.
    yield* withStore(filename, (store) =>
      Effect.gen(function* () {
        yield* store.repositories.putRepository(yield* repository);
        yield* store.workGraph.putWorkstream(yield* anchoredWorkstream);
      }).pipe(Effect.orDie),
    );

    // Simulate the store having been written by a DIFFERENT schema version — exactly
    // what bumping SCHEMA_VERSION looks like to an already-deployed database.
    const stale = new Database(filename);
    stale.run(`PRAGMA user_version = ${SCHEMA_VERSION + 1}`);
    stale.close();

    const after = yield* withStore(filename, (store) =>
      Effect.gen(function* () {
        const repositories = yield* store.repositories.listRepositories;
        const workstreams = yield* store.workGraph.listWorkstreams;
        return { repositories, workstreams };
      }).pipe(Effect.orDie),
    );

    // The store came back EMPTY and SELF-CONSISTENT. The referencing table cannot
    // outlive the referenced one, because the reset drops the WHOLE database rather
    // than a chosen subset of it — so there is no window in which a workstream
    // survives pointing at a repository row that no longer exists. That is what makes
    // the FOREIGN KEY hold ACROSS a reset and not only within one generation.
    expect(after.repositories).toStrictEqual([]);
    expect(after.workstreams).toStrictEqual([]);

    // And the invariant is live again immediately: the recreated schema still refuses
    // a workstream naming a repository that is not there — the reset rebuilt the
    // constraint, it did not merely empty the tables.
    const rejected = yield* withStore(filename, (store) =>
      Effect.gen(function* () {
        return yield* store.workGraph
          .putWorkstream(yield* anchoredWorkstream)
          .pipe(Effect.flip, Effect.orDie);
      }),
    );
    expect(rejected.operation).toBe("putWorkstream");
  }).pipe(Effect.scoped),
);

it.effect("resets a database left by the retired migration ladder (user_version 0)", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    // A pre-INV-FRESH database: the ladder tracked versions in its own table and left
    // `PRAGMA user_version` at 0, so it mismatches and takes the same reset path — no
    // special case, and no migration.
    const legacy = new Database(filename);
    legacy.run(`CREATE TABLE agent (id TEXT PRIMARY KEY NOT NULL, stale TEXT)`);
    legacy.run(`INSERT INTO agent VALUES ('agt-legacy', 'from the old ladder')`);
    legacy.close();

    const value = yield* agent;
    const after = yield* withStore(filename, (store) =>
      store.agents.putAgent(value).pipe(Effect.andThen(store.agents.listAgents), Effect.orDie),
    );
    // The legacy row is gone and the recreated table has the CURRENT columns (a
    // migration would have had to reshape it; the reset simply rebuilt it).
    expect(after).toStrictEqual([value]);
  }).pipe(Effect.scoped),
);
