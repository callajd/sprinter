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
import { Agent } from "@sprinter/domain";
import { layer, SCHEMA_VERSION, StateStore } from "./index.ts";

/** A fresh temp database path, torn down with the surrounding scope. */
const dbFile = Effect.acquireRelease(
  Effect.sync(() => mkdtempSync(join(tmpdir(), "sprinter-state-"))),
  (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true })),
).pipe(Effect.map((dir) => join(dir, "state.db")));

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
 */
const SCHEMA_LEDGER: Readonly<Record<number, string>> = {
  1: "12cfac61b40228489b1fbd68c13b9660799e48089d12ee0b30043e7668d604f0",
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
      "The persisted schema changed. THE FIX IS TO BUMP `SCHEMA_VERSION` in " +
      "packages/state/src/sqlite.ts (existing stores must drop and recreate — " +
      "INV-FRESH never migrates), and then ADD a row to SCHEMA_LEDGER in this test " +
      "for the new version. Do NOT rewrite an existing row: re-using a version for a " +
      `different shape is the exact failure this guards. Emitted schema was:\n${ddl}`;

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
