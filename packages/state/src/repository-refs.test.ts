/**
 * `INV-ENFORCE` for the `repository_ref` child table (DE1.2 D4) — the constraints
 * that make a malformed ref set UNCONSTRUCTIBLE rather than merely unlikely.
 *
 * These are WHITE-BOX tests, and they have to be: the `StateStore` port cannot
 * express the states under test. `putRepository` takes a whole `Repository`, whose
 * schema already refuses a repeated branch name, and the port exposes no way to write
 * a ref against an absent repository or to delete a repository at all. The point of a
 * schema-level constraint, though, is that it holds against ANY writer — including one
 * that never goes through the port — so the only way to demonstrate it is to write
 * directly to the backing and be refused.
 *
 * That is the same licence `schema-version.test.ts` takes, and the same limit applies:
 * the raw `bun:sqlite` handle is the TEST's probe of the on-disk schema, never
 * production code, and it lives beside the adapter it probes (no CONSUMER may do this
 * — INV-PORT).
 *
 * Every probe turns `PRAGMA foreign_keys = ON` on for its own connection, because the
 * setting is PER-CONNECTION: a probe that skipped it would find every foreign key
 * silently unenforced and would pass by not testing anything.
 */
import { it } from "@effect/vitest";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import { Repository } from "@sprinter/domain";
import { layer, StateStore } from "./index.ts";

/** A fresh temp database path, torn down with the surrounding scope. */
const dbFile = Effect.acquireRelease(
  Effect.sync(() => mkdtempSync(join(tmpdir(), "sprinter-repo-refs-"))),
  (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true })),
).pipe(Effect.map((dir) => join(dir, "state.db")));

const SHA_MAIN = "0123456789abcdef0123456789abcdef01234567";
const SHA_FEAT = "89abcdef0123456789abcdef0123456789abcdef";

const repository = Schema.decodeUnknownEffect(Repository)({
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [
    { name: "feat/x-1", sha: SHA_FEAT },
    { name: "main", sha: SHA_MAIN },
  ],
  observedAt: "2026-07-20T12:00:00.000Z",
}).pipe(Effect.orDie);

/** Build the schema on `filename` through the ADAPTER and seed one repository. */
const seed = (filename: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.repositories.putRepository(yield* repository);
  }).pipe(Effect.provide(layer({ filename, disableWAL: true })), Effect.orDie);

/**
 * Open a raw probe connection WITH foreign-key enforcement on, run `use`, and close
 * it. The pragma is per-connection, so a probe that omitted it would observe no
 * constraint at all.
 */
const probe = <A>(filename: string, use: (database: Database) => A): A => {
  const database = new Database(filename);
  database.run("PRAGMA foreign_keys = ON");
  try {
    return use(database);
  } finally {
    database.close();
  }
};

it.effect("REJECTS two refs with the same (repositoryId, name) — the composite PRIMARY KEY", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    yield* seed(filename);
    // `main` is already stored for this repository. A second row under the same
    // (repositoryId, name) is not "an update that happens to conflict" — it is the
    // state D4 exists to make unstorable, because it would give one branch two tips
    // in one observation with no rule for which is current.
    const rejected = probe(filename, (database) => {
      try {
        database.run(`INSERT INTO repository_ref ("repositoryId", name, sha) VALUES (?, ?, ?)`, [
          "repo:github:1296269",
          "main",
          SHA_FEAT,
        ]);
        return undefined;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    });
    expect(rejected, "a duplicate (repositoryId, name) must be REJECTED").toBeDefined();
  }).pipe(Effect.scoped),
);

it.effect("REJECTS a ref naming a repository that is not stored — the FOREIGN KEY", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    yield* seed(filename);
    const rejected = probe(filename, (database) => {
      try {
        database.run(`INSERT INTO repository_ref ("repositoryId", name, sha) VALUES (?, ?, ?)`, [
          "repo:github:9000001",
          "main",
          SHA_MAIN,
        ]);
        return undefined;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    });
    expect(rejected, "a ref naming an ABSENT repository must be REJECTED").toBeDefined();
  }).pipe(Effect.scoped),
);

it.effect("CASCADES — deleting a repository removes its observed refs", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    yield* seed(filename);
    const remaining = probe(filename, (database) => {
      const before = database.query(`SELECT COUNT(*) AS n FROM repository_ref`).get();
      database.run(`DELETE FROM repository WHERE id = ?`, ["repo:github:1296269"]);
      const after = database.query(`SELECT COUNT(*) AS n FROM repository_ref`).get();
      return { before, after };
    });
    // The refs existed, and they went WITH the repository — they describe an
    // observation of it and cannot outlive it. This cascade is the ONLY delete
    // anywhere on this path; the port exposes none.
    expect(remaining.before).toStrictEqual({ n: 2 });
    expect(remaining.after).toStrictEqual({ n: 0 });
  }).pipe(Effect.scoped),
);

it.effect("REJECTS a workstream row naming an absent repository, at the BACKING", () =>
  Effect.gen(function* () {
    const filename = yield* dbFile;
    yield* seed(filename);
    // The port-level version of this lives in `store.test.ts`; this asserts the
    // constraint holds against a writer that never went through the port at all,
    // which is what makes it a MECHANISM rather than a convention.
    const rejected = probe(filename, (database) => {
      try {
        database.run(
          `INSERT INTO workstream (id, name, "repositoryId", status, epics) VALUES (?, ?, ?, ?, ?)`,
          ["ws-orphan", "Orphan", "repo:github:9000001", "pending", "[]"],
        );
        return undefined;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    });
    expect(rejected, "a workstream naming an ABSENT repository must be REJECTED").toBeDefined();
  }).pipe(Effect.scoped),
);
