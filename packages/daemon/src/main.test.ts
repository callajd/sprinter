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
import {
  appLayer,
  bootLayer,
  configFromEnv,
  type DaemonConfig,
  DaemonSocketInUseError,
  mainLayer,
  probeSocket,
  unlinkStaleSocket,
} from "./main.ts";
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
  repository: { owner: "callajd", repo: "sprinter", token: "test-token" },
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

it("configFromEnv maps the environment with sensible defaults and the required token", () => {
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

  // Only GITHUB_TOKEN supplied: everything else falls back to the local defaults.
  const defaults = configFromEnv({ GITHUB_TOKEN: "ghp_secret" });
  expect(defaults.databasePath).toBe("./sprinter.db");
  expect(defaults.socketPath).toBe("./sprinter.sock");
  expect(defaults.workspaceRoot).toBe("./worktrees");
  expect(defaults.repository).toEqual({ owner: "callajd", repo: "sprinter", token: "ghp_secret" });
});

it("configFromEnv FAILS FAST when GITHUB_TOKEN is absent or empty (B1)", () => {
  // Token-less is not a supported mode: GitHub's GraphQL API 401s every
  // unauthenticated request, so a token-less daemon would silently never observe an
  // Issue as landed. Boot must refuse loudly with an actionable message.
  expect(() => configFromEnv({})).toThrow(/GITHUB_TOKEN is required/);
  expect(() => configFromEnv({ GITHUB_TOKEN: "" })).toThrow(/GITHUB_TOKEN is required/);
  expect(() => configFromEnv({ GITHUB_TOKEN: "   " })).toThrow(/GITHUB_TOKEN is required/);
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
      const first = Option.getOrThrow(
        yield* client.events({}).pipe(Stream.take(1), Stream.runHead),
      );
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
  // Effect layer graph (INV-EFFECT-DI). The BUILD is exercised below.
  const layer = mainLayer(
    configFromEnv({ SPRINTER_SOCKET: "/tmp/sprinter-test.sock", GITHUB_TOKEN: "ghp_secret" }),
  );
  expect(layer).toBeDefined();
});

it.effect("mainLayer BUILDS the full served graph (real transport bind + boot) in a scope", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-daemon-main-" });

    // Actually BUILD the production graph — not just construct the VALUE. This binds
    // a REAL Unix-domain socket on a temp path (via the un-widened `mainLayer`, whose
    // now-visible error/requirement channels a build error would surface), runs the
    // boot reconcile (a fresh/empty graph → offline, no host calls), and wires
    // RpcServer over the socket transport, the real Repository, and Bun services. A
    // wiring/build failure (or a failed bind) fails the test — the assurance the
    // construct-only assertion above cannot give. Building on a temp path also
    // exercises the stale-socket unlink-before-bind (a prior bind on the same path is
    // cleared), and the scope tears the socket server down deterministically.
    yield* Layer.build(mainLayer(testConfig(dir))).pipe(Effect.scoped);
  }).pipe(Effect.provide(BunFileSystem.layer)),
);

// ── conditional stale-socket unlink (FIX 2: live peer fails fast) ─────────────

/** A scoped, real Bun Unix-domain listener bound to `path` — a "live daemon". */
const liveListener = (path: string) =>
  Effect.acquireRelease(
    Effect.sync(() => Bun.listen({ unix: path, socket: { data() {} } })),
    (listener) => Effect.sync(() => listener.stop(true)),
  );

it.effect(
  "probeSocket reports `live` for a bound listener and `stale` for a refused connection",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-sock-" });

      // A bound listener accepts the probe connection → live.
      const livePath = `${dir}/live.sock`;
      yield* liveListener(livePath);
      expect(yield* probeSocket(livePath)).toBe("live");

      // A path with no listener refuses the probe connection → stale.
      expect(yield* probeSocket(`${dir}/absent.sock`)).toBe("stale");
    }).pipe(Effect.scoped, Effect.provide(BunFileSystem.layer)),
);

it.effect(
  "unlinkStaleSocket FAILS FAST on a LIVE peer without unlinking (double-run protection)",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-sock-" });
      const path = `${dir}/live.sock`;

      // A real daemon is already listening on the path.
      yield* liveListener(path);

      // A second daemon must NOT unlink the live socket (that would split-brain two
      // daemons on one socket) — it fails fast instead.
      const error = yield* unlinkStaleSocket(path).pipe(Effect.flip);
      expect(error instanceof DaemonSocketInUseError).toBe(true);
      // The live socket is untouched — no unlink of a live peer.
      expect(yield* fs.exists(path)).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(BunFileSystem.layer)),
);

it.effect(
  "unlinkStaleSocket binds fresh (absent), rebinds after a crashed daemon (stale), but refuses a non-socket path",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-sock-" });

      // Absent: a fresh start — nothing to unlink, succeeds.
      yield* unlinkStaleSocket(`${dir}/fresh.sock`);

      // Non-socket path: never force-delete an arbitrary file — fail fast, file kept.
      const filePath = `${dir}/not-a-socket`;
      yield* fs.writeFileString(filePath, "data");
      const err = yield* unlinkStaleSocket(filePath).pipe(Effect.flip);
      expect(err instanceof DaemonSocketInUseError).toBe(true);
      expect(yield* fs.exists(filePath)).toBe(true);

      // Stale: a crashed daemon leaves a socket with no live listener — the rebind
      // path must succeed and clear the way (the socket is gone afterwards).
      const stalePath = `${dir}/stale.sock`;
      const listener = yield* Effect.sync(() =>
        Bun.listen({ unix: stalePath, socket: { data() {} } }),
      );
      yield* Effect.sync(() => listener.stop(true));
      yield* unlinkStaleSocket(stalePath);
      expect(yield* fs.exists(stalePath)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(BunFileSystem.layer)),
);
