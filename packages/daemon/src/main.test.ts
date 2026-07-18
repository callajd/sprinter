/**
 * Daemon composition-root coverage (CE1.2) — the `./main.ts` layer graph proven to
 * COMPOSE and SERVE the frozen `SprinterRpc` contract end-to-end, exercised through
 * an in-memory `RpcTest` client (no real socket) against the REAL adapters with only
 * the substitutable LEAVES faked (INV-EFFECT-DI: fake-vs-real is a `Layer`
 * substitution):
 *
 *   - `StateStore` — the REAL file-backed SQLite adapter on a tmpfile (NOT
 *     `:memory:`), behind the real journaling + publishing decorators;
 *   - `ExecutionRunner` — the REAL `LocalPi` adapter over the REAL worktree router,
 *     with only the `ChildProcessSpawner` faked (never driven here — dispatch is
 *     covered by `local-pi-runner.test.ts` / `job-runner.test.ts`);
 *   - `Repository` — a canned fake (no HTTP);
 *   - `FileSystem`/`Path` — real Bun services.
 *
 * The suite proves the CE1.2 Done criteria on the SERVED surface: the graph builds,
 * `snapshot`/commands work, the `events` feed does DURABLE offset-based resync
 * (replays journaled history to a late-attaching client), and state is FILE-backed
 * (survives a rebuild on the same file). Deterministic and offline (INV-GATE-A).
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import { FileSystem } from "effect/FileSystem";
import { ChildProcessSpawner } from "effect/unstable/process";
import { RpcTest } from "effect/unstable/rpc";
import { BunFileSystem, BunPath } from "@effect/platform-bun";
import { expect } from "vitest";
import { SprinterRpc } from "@sprinter/contract";
import { Repository, RepositoryError, RepositoryIssue } from "@sprinter/repository";
import { StateStore } from "@sprinter/state";
import { appLayer, bootLayer, configFromEnv, type DaemonConfig, mainLayer } from "./main.ts";
import { StartupReconcile } from "./startup-reconcile.ts";

// ── substitutable leaves (fakes) ──────────────────────────────────────────────

/** A fake `Repository`: canned, no HTTP. Never driven here (reconcile is not run). */
const fakeRepository: Layer.Layer<Repository> = Layer.succeed(
  Repository,
  Repository.of({
    code: { defaultBranch: Effect.succeed("main"), branchExists: () => Effect.succeed(false) },
    issues: {
      getIssue: (number) =>
        Effect.succeed(
          Schema.decodeUnknownSync(RepositoryIssue)({ number, title: "fake", state: "open" }),
        ),
    },
    pullRequests: {
      closingPullRequest: () => Effect.succeed(Option.none()),
      getPullRequest: (number) =>
        Effect.die(
          new RepositoryError({ operation: "getPullRequest", detail: `unused #${number}` }),
        ),
    },
  }),
);

/**
 * A fake `ChildProcessSpawner`: the composition test never spawns (no dispatch is
 * driven), so the spawn factory is a defect if ever invoked — its presence only lets
 * the real `LocalPi` adapter BUILD, proving the leaf is a `Layer` substitution.
 */
const fakeSpawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner> = Layer.succeed(
  ChildProcessSpawner.ChildProcessSpawner,
  ChildProcessSpawner.make(() => Effect.die("spawn not exercised in composition test")),
);

const testConfig = (dir: string): DaemonConfig => ({
  databasePath: `${dir}/state.db`,
  socketPath: `${dir}/daemon.sock`,
  workspaceRoot: `${dir}/worktrees`,
  repository: { owner: "callajd", repo: "sprinter" },
});

/** The full graph with the leaves substituted by fakes/real-Bun services. */
const servedLeaves = Layer.mergeAll(
  fakeRepository,
  fakeSpawner,
  BunFileSystem.layer,
  BunPath.layer,
);

const served = (config: DaemonConfig) => appLayer(config).pipe(Layer.provide(servedLeaves));

// ── configFromEnv ─────────────────────────────────────────────────────────────

it("configFromEnv maps the environment with sensible defaults and an optional token", () => {
  const withToken = configFromEnv({
    SPRINTER_DB: "/var/sprinter.db",
    SPRINTER_SOCKET: "/run/sprinter.sock",
    SPRINTER_WORKSPACE: "/srv/worktrees",
    SPRINTER_REPO_OWNER: "acme",
    SPRINTER_REPO_NAME: "widgets",
    GITHUB_TOKEN: "ghp_secret",
  });
  expect(withToken).toEqual({
    databasePath: "/var/sprinter.db",
    socketPath: "/run/sprinter.sock",
    workspaceRoot: "/srv/worktrees",
    repository: { owner: "acme", repo: "widgets", token: "ghp_secret" },
  });

  const defaults = configFromEnv({});
  expect(defaults.databasePath).toBe("./sprinter.db");
  expect(defaults.socketPath).toBe("./sprinter.sock");
  expect(defaults.workspaceRoot).toBe("./worktrees");
  expect(defaults.repository).toEqual({ owner: "callajd", repo: "sprinter" });
  // The token is OMITTED (not `undefined`) when absent (exactOptionalPropertyTypes).
  expect("token" in defaults.repository).toBe(false);
});

// ── the served graph composes ─────────────────────────────────────────────────

it.effect("the graph serves snapshot + commands end-to-end over the frozen contract", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-daemon-" });

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(SprinterRpc);

      // A fresh daemon: empty snapshot.
      const empty = yield* client.snapshot();
      expect(empty.workstreams).toEqual([]);

      // A command materializes + persists a workstream through the real file-backed store.
      const id = yield* client.createWorkstreamFromPlan({
        plan: { name: "Convergence", repo: "callajd/sprinter", spec: "wire the daemon" },
      });
      const hydrated = yield* client.snapshot();
      expect(hydrated.workstreams.map((w) => w.id)).toEqual([id]);
    }).pipe(Effect.scoped, Effect.provide(served(testConfig(dir))));
  }).pipe(Effect.provide(BunFileSystem.layer)),
);

it.effect("the events feed does DURABLE offset-based resync for a late-attaching client", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-daemon-" });

    yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(SprinterRpc);

      // A mutation BEFORE the client subscribes to `events` — snapshot-on-connect
      // alone would miss it; only the durable resync catches it up.
      yield* client.createWorkstreamFromPlan({
        plan: { name: "Convergence", repo: "callajd/sprinter", spec: "wire the daemon" },
      });

      // Attaching now REPLAYS the journaled history: the pre-subscribe delta arrives.
      const first = Option.getOrThrow(yield* client.events().pipe(Stream.take(1), Stream.runHead));
      expect(first._tag).toBe("WorkstreamChanged");
    }).pipe(Effect.scoped, Effect.provide(served(testConfig(dir))));
  }).pipe(Effect.provide(BunFileSystem.layer)),
);

it.effect("state is FILE-backed: a rebuild on the same database file sees prior state", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-daemon-" });
    const config = testConfig(dir);

    // Build #1 (the daemon): create a workstream, then TEAR DOWN the graph.
    const id = yield* Effect.gen(function* () {
      const client = yield* RpcTest.makeClient(SprinterRpc);
      return yield* client.createWorkstreamFromPlan({
        plan: { name: "Convergence", repo: "callajd/sprinter", spec: "wire the daemon" },
      });
    }).pipe(Effect.scoped, Effect.provide(served(config)));

    // Build #2 (a "restart") on the SAME file: the workstream is still there.
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      const persisted = yield* store.workGraph.getWorkstream(id);
      expect(Option.isSome(persisted)).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(served(config)));
  }).pipe(Effect.provide(BunFileSystem.layer)),
);

// ── boot composes StartupReconcile ────────────────────────────────────────────

it.effect("bootLayer runs StartupReconcile once at startup (restart resync + re-dispatch)", () =>
  Effect.gen(function* () {
    const ran = yield* Ref.make(0);
    const fakeStartup = Layer.succeed(
      StartupReconcile,
      StartupReconcile.of({
        run: Ref.updateAndGet(ran, (n) => n + 1).pipe(
          Effect.as({ reconciledWorkstreams: 0, resumed: [], skipped: [] }),
        ),
      }),
    );

    // Building the boot layer must invoke `StartupReconcile.run` exactly once — this
    // is the "startup composes StartupReconcile" wiring (AE5), independent of the
    // concrete reconcile logic (covered by `startup-reconcile.test.ts`).
    yield* Layer.build(bootLayer).pipe(Effect.provide(fakeStartup), Effect.scoped);
    expect(yield* Ref.get(ran)).toBe(1);
  }),
);

// ── the full production graph constructs (transport + boot included) ──────────

it("mainLayer assembles the full served graph (transport + boot) without error", () => {
  // Constructing the layer is pure (no socket bound until it is BUILT), so this
  // proves the production graph — RpcServer over the socket transport, the real
  // Repository, boot reconcile, Bun services — type-checks and assembles as ONE
  // Effect layer graph (INV-EFFECT-DI). Its launch is the documented smoke step.
  const layer = mainLayer(configFromEnv({ SPRINTER_SOCKET: "/tmp/sprinter-test.sock" }));
  expect(layer).toBeDefined();
});
