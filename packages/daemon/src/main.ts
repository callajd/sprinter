/**
 * `main` — the daemon composition root (Track A CVG, task CE1.2). This is the ONE
 * place the real daemon is assembled: a SINGLE Effect layer graph
 * (`Layer.provide`/`Layer.mergeAll`) wiring the concrete adapters behind the ports
 * every other module depends on, and serving the FROZEN `SprinterRpc` contract over
 * a concrete transport the app dials (INV-EFFECT-DI / INV-PORT / INV-CONTRACT).
 *
 * The graph, bottom-up:
 *
 * - {@link StateStore} — the SQLite adapter opened on a real FILE (durable across
 *   restarts, AE5), decorated with durable journaling (`./event-journal.ts`) then
 *   live publishing (`./store-publishing.ts`), so every mutation is persisted,
 *   journaled to the offset log, AND fanned out to the reactive feed.
 * - {@link ExecutionRunner} — CE1.1's real `LocalPi` adapter (`@sprinter/job`
 *   `layerLocalPi`) over a per-Job worktree router (`layerWorktreeRouter`, CE1.1-F2),
 *   spawning `pi` through the Bun `ChildProcessSpawner`.
 * - {@link JobRunner}, {@link Repository}, {@link SessionRegistry},
 *   {@link WorkGraphEvents} — the remaining ports, each an Effect `Layer`.
 * - {@link handlers} — the `SprinterRpc` server handlers, served by
 *   `RpcServer.layer` over a `SocketServer` transport (NDJSON framing) — the ONLY
 *   new edges the app dials (INV-PORT).
 * - {@link StartupReconcile} — run once at boot so a restart resyncs the durable
 *   graph and re-dispatches persisted in-flight work (AE5, now file-backed).
 *
 * Selecting real-vs-fake (a fake `Repository`/`ChildProcessSpawner` under test) or
 * one transport-vs-another is a `Layer` substitution and nothing else: NOTHING here
 * is `new`-ed or hand-wired outside DI (INV-EFFECT-DI). The runnable process
 * entrypoint (env → config → `runMain`) lives in the sibling `./run.ts`; this module
 * is the pure, tested graph.
 */
import { Effect, Layer } from "effect";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { BunServices, BunSocketServer } from "@effect/platform-bun";
import { SprinterRpc } from "@sprinter/contract";
import { layer as layerJobRunner, layerLocalPi, layerWorktreeRouter } from "@sprinter/job";
import { layerFetch as layerRepository, type RepositoryConfig } from "@sprinter/repository";
import { layer as layerStateSqlite } from "@sprinter/state";
import { layerJournaling } from "./event-journal.ts";
import { handlers } from "./rpc-handlers.ts";
import { layer as layerSessionRegistry } from "./session-registry.ts";
import { layer as layerStartupReconcile, StartupReconcile } from "./startup-reconcile.ts";
import { layerPublishing } from "./store-publishing.ts";
import { layer as layerWorkGraphEvents } from "./work-graph-events.ts";

// ── configuration ─────────────────────────────────────────────────────────────

/**
 * The daemon's runtime configuration — every location-variant the composition root
 * needs, resolved once at boot. All are provisioning inputs (paths, the bound
 * repository); the graph itself is fixed.
 */
export interface DaemonConfig {
  /** The SQLite database FILE (never `:memory:` — durability is the point, AE5). */
  readonly databasePath: string;
  /** The Unix-domain socket path the app dials (the served transport's wire). */
  readonly socketPath: string;
  /** The base directory under which each Job gets its own `<root>/<job.id>` worktree. */
  readonly workspaceRoot: string;
  /** The single bound repository the daemon reconciles against (repo-scoped, D14). */
  readonly repository: RepositoryConfig;
}

/**
 * Resolve a {@link DaemonConfig} from an environment record — pure, with sensible
 * defaults for a local daemon, so the entrypoint is a thin `process.env` read. The
 * GitHub `token` is included only when present (`exactOptionalPropertyTypes`).
 */
export const configFromEnv = (env: Readonly<Record<string, string | undefined>>): DaemonConfig => {
  const token = env["GITHUB_TOKEN"];
  return {
    databasePath: env["SPRINTER_DB"] ?? "./sprinter.db",
    socketPath: env["SPRINTER_SOCKET"] ?? "./sprinter.sock",
    workspaceRoot: env["SPRINTER_WORKSPACE"] ?? "./worktrees",
    repository: {
      owner: env["SPRINTER_REPO_OWNER"] ?? "callajd",
      repo: env["SPRINTER_REPO_NAME"] ?? "sprinter",
      ...(token !== undefined ? { token } : {}),
    },
  };
};

// ── port sub-graphs ───────────────────────────────────────────────────────────

/**
 * The durable {@link StateStore}: the SQLite adapter on the configured FILE,
 * decorated with durable journaling (beneath) then live publishing (above). The
 * order matters — a mutation is journaled to the offset log BEFORE it is fanned out
 * live, so the offset-based resync (`./event-journal.ts`) is gap-free.
 */
export const stateStoreLayer = (config: DaemonConfig) =>
  layerPublishing(layerJournaling(layerStateSqlite({ filename: config.databasePath })));

/**
 * The real {@link ExecutionRunner}: CE1.1's `LocalPi` adapter over a per-Job
 * worktree router (CE1.1-F2). Requires only the `ChildProcessSpawner` and the
 * `FileSystem`/`Path` the router uses — the daemon edge (`BunServices`) provides
 * them; a test substitutes a fake spawner (INV-EFFECT-DI).
 */
export const executionRunnerLayer = (config: DaemonConfig) =>
  layerLocalPi.pipe(Layer.provide(layerWorktreeRouter(config.workspaceRoot)));

/**
 * The full port sub-graph MINUS the leaf adapters (`Repository`, the process
 * spawner, the filesystem) that a test substitutes: the `StateStore`,
 * `ExecutionRunner`, `SessionRegistry`, `WorkGraphEvents`, `JobRunner`, and
 * `StartupReconcile` services, cross-wired. Requires `Repository` +
 * `ChildProcessSpawner`/`FileSystem`/`Path`.
 */
const portsLayer = (config: DaemonConfig) =>
  Layer.mergeAll(stateStoreLayer(config), executionRunnerLayer(config), layerSessionRegistry).pipe(
    Layer.provideMerge(layerWorkGraphEvents),
  );

/**
 * The app services: the ports plus the `JobRunner` and `StartupReconcile` built on
 * them. `StartupReconcile` requires the `JobRunner`, so it is provided beneath.
 */
const servicesLayer = (config: DaemonConfig) =>
  layerStartupReconcile.pipe(
    Layer.provideMerge(layerJobRunner),
    Layer.provideMerge(portsLayer(config)),
  );

/**
 * The handlers + all services graph, requiring only the substitutable leaves
 * (`Repository`, `ChildProcessSpawner`, `FileSystem`, `Path`). This is the surface a
 * test drives via `RpcTest` with fake leaves (INV-EFFECT-DI); production provides
 * the real leaves and the transport in {@link mainLayer}.
 */
export const appLayer = (config: DaemonConfig) =>
  handlers.pipe(Layer.provideMerge(servicesLayer(config)));

// ── boot + transport ──────────────────────────────────────────────────────────

/**
 * Run {@link StartupReconcile} once at boot: reconcile the durable graph against the
 * host and re-dispatch persisted in-flight work (AE5). A background resume fibers
 * onto the daemon scope, so `run` returns promptly and boot never blocks on a
 * session. Requires only the `StartupReconcile` port.
 */
export const bootLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;
    yield* Effect.logInfo("sprinter-daemon: startup reconcile complete", summary);
  }),
);

/**
 * The concrete served transport: `RpcServer` over a `SocketServer` bound to the
 * configured Unix-domain socket, with NDJSON framing — the wire the app dials. This
 * (and {@link mainLayer}) are the ONLY transport-aware edges (INV-PORT); swapping to
 * stdio/TCP is a `Layer` substitution here.
 */
export const socketProtocolLayer = (config: DaemonConfig) =>
  RpcServer.layerProtocolSocketServer.pipe(
    Layer.provide(RpcSerialization.layerNdjson),
    Layer.provide(BunSocketServer.layer({ path: config.socketPath })),
  );

/**
 * The complete daemon graph: the `SprinterRpc` server + boot, provided the app
 * services, the socket transport, the real `Repository`, and the Bun platform
 * services — a single self-contained `Layer` whose launch IS the running daemon.
 * Building it opens the socket, runs the boot reconcile, and starts serving; closing
 * its scope tears everything down.
 */
export const mainLayer = (config: DaemonConfig): Layer.Layer<never, unknown> =>
  Layer.mergeAll(RpcServer.layer(SprinterRpc), bootLayer).pipe(
    Layer.provide(appLayer(config)),
    Layer.provide(socketProtocolLayer(config)),
    Layer.provide(layerRepository(config.repository)),
    Layer.provide(BunServices.layer),
  );
