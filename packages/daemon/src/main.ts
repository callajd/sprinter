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
 *   restarts, AE5), decorated with durable journaling (`./event-journal.ts`), so
 *   every mutation is persisted, journaled to the offset log, AND fanned out to the
 *   reactive feed stamped with its durable offset (one decorator, one coordinate).
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
import { Duration, Effect, Layer, Schema } from "effect";
import { FileSystem } from "effect/FileSystem";
import type { PlatformError } from "effect/PlatformError";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { BunServices, BunSocketServer } from "@effect/platform-bun";
import { SprinterRpc } from "@sprinter/contract";
import { layer as layerJobRunner, layerLocalPi, layerWorktreeRouter } from "@sprinter/job";
import { layerFetch as layerRepository, type RepositoryConfig } from "@sprinter/repository";
import { layer as layerStateSqlite } from "@sprinter/state";
import { layerJournaling } from "./event-journal.ts";
import { handlers } from "./rpc-handlers.ts";
import { layer as layerSessionEvents } from "./session-events.ts";
import { layerWith as layerSessionRegistry, SESSION_RESOLVE_TIMEOUT } from "./session-registry.ts";
import { layerRegisterSessions } from "./session-runner.ts";
import { layer as layerStartupReconcile, StartupReconcile } from "./startup-reconcile.ts";
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
  /**
   * The hard bound the session-channel handlers wait for a genuinely mid-dispatch
   * session's handle to register before failing `SessionNotFound` (the
   * register-after-dispatch window — a `pi` spawn + RPC handshake). Defaults to
   * {@link SESSION_RESOLVE_TIMEOUT} (5s); the operational knob to RAISE if a real `pi`
   * cold-start exceeds it (a spurious `SessionNotFound` with no other lever otherwise).
   */
  readonly sessionResolveTimeout: Duration.Duration;
}

/**
 * Resolve a {@link DaemonConfig} from an environment record — pure, with sensible
 * defaults for a local daemon, so the entrypoint is a thin `process.env` read.
 *
 * `GITHUB_TOKEN` is REQUIRED (B1, CE1.3): the daemon drives authenticated Issue→PR
 * work and GitHub's GraphQL closing-PR signal 401s every unauthenticated request, so
 * a token-less daemon would silently never observe any Issue as landed. This FAILS
 * FAST AND LOUDLY at boot — throwing a clear, actionable error before the graph is
 * built — rather than constructing a token-less config that dies quietly per-call.
 */
export const configFromEnv = (env: Readonly<Record<string, string | undefined>>): DaemonConfig => {
  const token = env["GITHUB_TOKEN"];
  if (token === undefined || token.trim() === "") {
    throw new Error(
      "GITHUB_TOKEN is required: the daemon needs an authenticated GitHub token (GitHub's GraphQL API rejects unauthenticated requests with 401). Set GITHUB_TOKEN and restart.",
    );
  }
  return {
    databasePath: env["SPRINTER_DB"] ?? "./sprinter.db",
    socketPath: env["SPRINTER_SOCKET"] ?? "./sprinter.sock",
    workspaceRoot: env["SPRINTER_WORKSPACE"] ?? "./worktrees",
    repository: {
      owner: env["SPRINTER_REPO_OWNER"] ?? "callajd",
      repo: env["SPRINTER_REPO_NAME"] ?? "sprinter",
      token,
    },
    sessionResolveTimeout: sessionResolveTimeoutFrom(env["SPRINTER_SESSION_RESOLVE_TIMEOUT_MS"]),
  };
};

/**
 * Resolve the session-resolve bound from an optional millisecond env override,
 * defaulting to {@link SESSION_RESOLVE_TIMEOUT}. A missing, blank, non-numeric, or
 * non-positive value falls back to the default rather than a nonsensical bound — the
 * knob only ever RAISES a sane cold-start allowance, never breaks it (INV-NOCAST: the
 * parse is a total `Number` check, no assertion).
 */
const sessionResolveTimeoutFrom = (raw: string | undefined): Duration.Duration => {
  if (raw === undefined || raw.trim() === "") return SESSION_RESOLVE_TIMEOUT;
  const ms = Number(raw);
  return Number.isInteger(ms) && ms > 0 ? Duration.millis(ms) : SESSION_RESOLVE_TIMEOUT;
};

// ── port sub-graphs ───────────────────────────────────────────────────────────

/**
 * The durable {@link StateStore}: the SQLite adapter on the configured FILE,
 * decorated with durable journaling (`./event-journal.ts`). One decorator both
 * journals each mutation to the offset log AND fans it out live on the
 * {@link WorkGraphEvents} feed stamped with the durable offset it committed at — so
 * the live tail and the durable replay share one coordinate space and the
 * offset-based resync is gap-free. The SAME decorator fans each durable session-transcript
 * append out on the {@link SessionEvents} feed stamped with its per-session offset.
 * Requires `WorkGraphEvents` + `SessionEvents` (both provided by {@link portsLayer}).
 */
export const stateStoreLayer = (config: DaemonConfig) =>
  layerJournaling(layerStateSqlite({ filename: config.databasePath }));

/**
 * The real {@link ExecutionRunner}: CE1.1's `LocalPi` adapter over a per-Job
 * worktree router (CE1.1-F2), DECORATED so every started session is registered in
 * the {@link SessionRegistry} (CE4.1, `./session-runner.ts`) — the wire that makes a
 * dispatched session reachable over the contract's session channel. Requires the
 * `ChildProcessSpawner`, the `FileSystem`/`Path` the router uses (the daemon edge
 * `BunServices` provides them; a test substitutes a fake spawner), and the
 * `SessionRegistry` ({@link portsLayer} provides it). Selecting real-vs-fake is a
 * `Layer` substitution (INV-EFFECT-DI).
 */
export const executionRunnerLayer = (config: DaemonConfig) =>
  layerRegisterSessions(
    layerLocalPi.pipe(Layer.provide(layerWorktreeRouter(config.workspaceRoot))),
  );

/**
 * The full port sub-graph MINUS the leaf adapters (`Repository`, the process
 * spawner, the filesystem) that a test substitutes: the `StateStore`,
 * `ExecutionRunner`, `SessionRegistry`, `WorkGraphEvents`, `JobRunner`, and
 * `StartupReconcile` services, cross-wired. Requires `Repository` +
 * `ChildProcessSpawner`/`FileSystem`/`Path`.
 *
 * The {@link SessionRegistry} is provided BENEATH (`provideMerge`) rather than merged
 * as a sibling, because the registering {@link executionRunnerLayer} decorator (CE4.1)
 * now DEPENDS on it — one registry instance feeds both the `ExecutionRunner` (which
 * registers dispatched handles) and the handlers (which resolve them), so the session
 * a command dispatches is the very session the session channel drives.
 */
const portsLayer = (config: DaemonConfig) =>
  Layer.mergeAll(stateStoreLayer(config), executionRunnerLayer(config)).pipe(
    Layer.provideMerge(layerSessionRegistry(config.sessionResolveTimeout)),
    Layer.provideMerge(layerWorkGraphEvents),
    Layer.provideMerge(layerSessionEvents),
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
 * The fail-fast signal that a second daemon cannot bind the socket: either a LIVE
 * daemon is already listening on the path (double-run protection — unlinking it would
 * split-brain two daemons on one `SPRINTER_SOCKET`), or the path is occupied by a
 * non-socket file (which must never be force-deleted). Raised at boot so the socket
 * bind fails fast with a clear cause instead of silently clobbering a live peer.
 */
export class DaemonSocketInUseError extends Schema.TaggedErrorClass<DaemonSocketInUseError>()(
  "DaemonSocketInUseError",
  {
    /** The Unix-domain socket path that could not be safely bound. */
    path: Schema.String,
    /** A neutral, human-readable description of why the bind was refused. */
    detail: Schema.String,
  },
) {}

/**
 * Probe whether a LIVE listener is accepting connections on a Unix-domain socket
 * path: connect and, if it SUCCEEDS, a daemon is listening (`"live"`); if the
 * connection is refused (`ECONNREFUSED` — the socket file exists but its owner is
 * dead), it is a stale leftover (`"stale"`). This runs only against a path already
 * confirmed to be a socket, so ANY connect failure means there is no live owner —
 * safe to unlink. Bun-native (`Bun.connect`), never `node:*` (matching the codebase's
 * Bun-native substrate). The probe connection is closed immediately.
 */
export const probeSocket = (path: string): Effect.Effect<"live" | "stale"> =>
  Effect.tryPromise(() => Bun.connect({ unix: path, socket: { data() {} } })).pipe(
    Effect.flatMap((socket) =>
      Effect.sync(() => {
        socket.end();
      }).pipe(Effect.as("live" as const)),
    ),
    // A refused/failed connect on a confirmed-socket path means the owner is gone.
    Effect.catch(() => Effect.succeed("stale" as const)),
  );

/**
 * CONDITIONALLY unlink a stale Unix-domain socket before a bind (INV-RESTART), while
 * preserving double-run protection. A daemon that crashed leaves its `sprinter.sock`
 * on disk and `bind(2)` on an existing socket path fails with `EADDRINUSE`, so a
 * crashed daemon must be able to rebind. But `BunSocketServer.layer` (→
 * `@effect/platform-node-shared` `NodeSocketServer`, `Net.createServer` +
 * `server.listen`) does NOT unlink first — and unconditionally removing the path
 * would let a SECOND daemon silently unlink a LIVE peer's socket and bind a
 * split-brain listener.
 *
 * So the decision is conditional on the socket actually being STALE:
 *
 * - Path absent → fresh start; nothing to unlink (bind directly).
 * - Path present but NOT a socket → fail fast; never force-delete an arbitrary path.
 * - Path present and a socket → PROBE it: a LIVE listener → fail fast (a daemon is
 *   already running; do NOT unlink); a refused connection → stale (dead owner) →
 *   unlink so the bind can succeed.
 *
 * Only an actual dead unix socket is ever removed.
 */
export const unlinkStaleSocket = (
  path: string,
): Effect.Effect<void, PlatformError | DaemonSocketInUseError, FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    // Fresh start: nothing bound at the path — bind directly (no unlink needed).
    if (!(yield* fs.exists(path))) return;
    // The path exists but is NOT a socket — never force-delete an arbitrary path.
    const info = yield* fs.stat(path);
    if (info.type !== "Socket") {
      return yield* Effect.fail(
        new DaemonSocketInUseError({
          path,
          detail: `socket path exists and is not a socket (type: ${info.type})`,
        }),
      );
    }
    // A socket file is present: probe for a live owner.
    const state = yield* probeSocket(path);
    if (state === "live") {
      return yield* Effect.fail(
        new DaemonSocketInUseError({ path, detail: "a daemon is already running on this socket" }),
      );
    }
    // Stale socket (owner dead, connection refused) — safe to unlink and rebind.
    yield* fs.remove(path);
  });

/**
 * The bound socket transport: unlink any stale socket FILE, THEN bind. `Layer.unwrap`
 * runs the unlink effect to COMPLETION before it yields (and builds) the binding
 * layer, so the two are strictly ordered — the stale file is always gone before the
 * `listen` that would otherwise `EADDRINUSE`. Requires the `FileSystem` (the daemon
 * edge `BunServices` provides it).
 */
const socketServerLayer = (config: DaemonConfig) =>
  Layer.unwrap(
    unlinkStaleSocket(config.socketPath).pipe(
      Effect.as(BunSocketServer.layer({ path: config.socketPath })),
    ),
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
    Layer.provide(socketServerLayer(config)),
  );

/**
 * The complete daemon graph: the `SprinterRpc` server + boot, provided the app
 * services, the socket transport, the real `Repository`, and the Bun platform
 * services — a single self-contained `Layer` whose launch IS the running daemon.
 * Building it opens the socket, runs the boot reconcile, and starts serving; closing
 * its scope tears everything down.
 */
export const mainLayer = (config: DaemonConfig) =>
  Layer.mergeAll(RpcServer.layer(SprinterRpc), bootLayer).pipe(
    Layer.provide(appLayer(config)),
    Layer.provide(socketProtocolLayer(config)),
    Layer.provide(layerRepository(config.repository)),
    Layer.provide(BunServices.layer),
  );
