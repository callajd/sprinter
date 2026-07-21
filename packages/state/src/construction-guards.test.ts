/**
 * The two FAIL-FAST guards the SQLite adapter runs at CONSTRUCTION — the refusals
 * that keep a half-configured store from ever being handed out:
 *
 * 1. `PRAGMA foreign_keys` is set AND read back. A connection that still reports
 *    enforcement OFF fails the build rather than serving writes under rules it is
 *    not enforcing (a dangling `agent.supersedes` becomes storable again, and every
 *    lineage rule reverts to being bypassable by write ordering).
 * 2. The store GENERATION row must be present. It is minted by `createSchema` and
 *    cannot be reconstructed, so a missing row fails the build rather than some
 *    later resume request.
 *
 * Neither is reachable through a real SQLite — that is the point of them — so each
 * runs the adapter over a STUBBED `SqlClient` whose connection answers the
 * construction statements directly. The stub is a real `SqlClient` built from a real
 * compiler over a fake `Connection` (no casts, INV-NOCAST); only the ROWS are
 * fabricated.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";
import { Reactivity } from "effect/unstable/reactivity";
import { SqlClient, SqlConnection, SqlError, Statement } from "effect/unstable/sql";
import { expect } from "vitest";
import { layerOverSqlClient, SCHEMA_VERSION } from "./sqlite.ts";
import { StateStore, StateStoreError } from "./store.ts";

/** The rows a stubbed connection answers a given statement with. */
type Respond = (sql: string) => ReadonlyArray<Record<string, unknown>>;

/**
 * A real `SqlClient` over a fake `Connection`: statements compile normally and the
 * rows come from `respond`. The construction path only ever runs row-returning
 * statements, so the value/stream methods are defects — reaching one means the path
 * under test changed shape, which should fail loudly rather than silently pass.
 */
const stubClient = (respond: Respond): Layer.Layer<SqlClient.SqlClient> => {
  const unreached = (method: string): Effect.Effect<never, SqlError.SqlError> =>
    Effect.die(new Error(`the construction guards never call Connection.${method}`));
  const connection: SqlConnection.Connection = {
    execute: (sql) => Effect.succeed(respond(sql)),
    executeRaw: (sql) => Effect.succeed(respond(sql)),
    executeUnprepared: (sql) => Effect.succeed(respond(sql)),
    executeValues: () => unreached("executeValues"),
    executeValuesUnprepared: () => unreached("executeValuesUnprepared"),
    executeStream: () => Stream.fromEffect(unreached("executeStream")),
  };
  return Layer.effect(
    SqlClient.SqlClient,
    SqlClient.make({
      acquirer: Effect.succeed(connection),
      compiler: Statement.makeCompilerSqlite(),
      spanAttributes: [],
    }),
  ).pipe(Layer.provide(Reactivity.layer));
};

/** Build the adapter over a stubbed client and return its CONSTRUCTION failure. */
const buildFailure = (respond: Respond): Effect.Effect<StateStoreError> =>
  StateStore.pipe(
    Effect.provide(layerOverSqlClient.pipe(Layer.provide(stubClient(respond)))),
    Effect.flip,
    Effect.scoped,
    Effect.orDie,
  );

it.effect("REFUSES to build when the foreign_keys pragma does not take", () =>
  Effect.gen(function* () {
    // SQLite answers a `PRAGMA foreign_keys = ON` it cannot honour by doing NOTHING at
    // all, with no error — so the only signal is the read-back. This stub is that
    // silent case: the SET succeeds, the read-back still says off.
    const failure = yield* buildFailure((sql) =>
      sql.includes("PRAGMA foreign_keys") ? [{ foreign_keys: 0 }] : [],
    );
    expect(failure).toBeInstanceOf(StateStoreError);
    expect(failure.operation).toBe("applySchema");
    expect(failure.detail).toContain("foreign_keys");
  }),
);

it.effect("REFUSES to build when store_meta carries no generation row", () =>
  Effect.gen(function* () {
    // Enforcement is on and the schema is already at the current version — so
    // `applySchema` returns EARLY, running no DDL, and nothing re-mints the generation.
    // A store_meta with no generation row is therefore a real reachable shape (a
    // truncated or hand-edited database), and it must fail construction rather than
    // surface as an undefined identity on a later resume.
    const failure = yield* buildFailure((sql) => {
      if (sql.includes("PRAGMA foreign_keys")) return [{ foreign_keys: 1 }];
      if (sql.includes("PRAGMA user_version")) return [{ user_version: SCHEMA_VERSION }];
      return [];
    });
    expect(failure).toBeInstanceOf(StateStoreError);
    expect(failure.operation).toBe("generation");
    expect(failure.detail).toContain("store_meta");
  }),
);
