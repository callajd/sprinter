/**
 * CE4.2 — Restart-safe cutover: a deterministic **build-write-RESTART-read** pass over
 * the real wire, the FINAL convergence proof that the Issue→PR loop survives a daemon
 * restart mid-flight without losing or duplicating work.
 *
 * It extends the CE4.1 acceptance harness (real `./main.ts` daemon graph — file-backed
 * SQLite `StateStore` + journaling, `RpcServer` over a REAL Unix-domain socket with NDJSON
 * — driven from the app side by a REAL `effect/unstable/rpc` client over that socket, with
 * only the two non-deterministic leaves substituted as pure Layers, INV-EFFECT-DI: a
 * CONTROLLABLE scripted `pi` at the `ChildProcessSpawner` seam and a SANDBOXED in-process
 * `CodeHost`). CE4.2 adds the RESTART: the daemon is brought up, driven to a mid-flight
 * `running` Job persisted to the file, then its whole layer scope is TORN DOWN — the
 * process death: the socket unbinds and SQLite closes with the Job still durably `running`,
 * NOT settled — and a FRESH daemon graph is brought up on the SAME database file + socket
 * path (the restart). We then assert, over the real wire, that:
 *
 *   1. the daemon **re-dispatches** the persisted in-flight Job on boot
 *      (`StartupReconcile` → the file-backed durability of CE1.2), driving it to a terminal
 *      settle — the work CONTINUES across the restart rather than being lost;
 *   2. **1 Job = 1 execution re-attached by id** — the resume re-attaches the SAME persisted
 *      execution id (`UNIQUE(execution.jobId)`), never a new execution, so the work is not
 *      DUPLICATED;
 *   3. the app **resyncs** on reconnect — a fresh client (the app re-dialing the restarted
 *      daemon) resumes the `events` feed from its last-applied durable **offset** (the
 *      resume cursor) and observes the post-restart deltas STRICTLY AFTER it: no delta re-delivered
 *      (no duplication) and the terminal `succeeded` delta delivered (no loss).
 *
 * Every wait is HARD-timeout-bounded so a hung socket/reconnect/event fails fast rather
 * than blocking forever (CRITICAL — this is a real process-kill/restart test); the whole
 * graph (socket + StateStore + tempdir) is `Scope`-torn-down in teardown — nothing leaked.
 *
 * The genuinely-real restart (real `pi`, real GitHub, a real SIGKILL) is the operator
 * RUNBOOK's job (`docs/runbook/ce4.2-restart-cutover.md`), deliberately out of this
 * deterministic test's scope.
 */
import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Layer, Option, Queue, Schema, Sink, Stream } from "effect";
import { FileSystem } from "effect/FileSystem";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcessSpawner } from "effect/unstable/process";
import { RpcClient, RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { BunFileSystem, BunServices, BunSocket } from "@effect/platform-bun";
import { expect } from "vitest";
import { SprinterRpc } from "@sprinter/contract";
import { Agent, Epic, Issue, Job, Execution, ExecutionId, Workstream } from "@sprinter/domain";
import { Repository as DomainRepository } from "@sprinter/domain";
import { CodeHost, RepositoryIssue } from "@sprinter/repository";
import { layer as layerStateSqlite, StateStore } from "@sprinter/state";
import { appLayer, bootLayer, type DaemonConfig, socketProtocolLayer } from "./main.ts";
import { EXECUTION_RESOLVE_TIMEOUT } from "./execution-registry.ts";

// ── hard timeout so a hung socket/daemon/reconnect/event fails fast, never blocks ──
const HARD_TIMEOUT = "15 seconds";

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

/**
 * The stand-in for the NUMERIC identifier a code host assigns a repository — a pure
 * FNV-1a hash of the natural key, so it is deterministic and independent of test order.
 *
 * It stands in for the host's OWN id rather than being the key in the id's clothing: a
 * real adapter mints a `RepositoryId` from an identifier a RENAME does not change, and
 * `RepositoryId` now CHECKS that shape (`repo:<host>:<host-id>`, host-id from the
 * URL-unreserved set), so a key-shaped `repo:github:owner/name` no longer decodes at
 * all. Spelling the fake's id like the real one keeps this harness from agreeing with a
 * broken adapter.
 */
const fakeRepositoryId = (owner: string, name: string): string => {
  let hash = 0x811c9dc5;
  for (const character of `${owner}/${name}`) {
    hash = Math.imul(hash ^ (character.codePointAt(0) ?? 0), 0x01000193) >>> 0;
  }
  return `repo:github:${hash}`;
};

// ── the sandboxed code host: the Issue stays open (not landed) across the restart, so
//    StartupReconcile RESUMES the in-flight Job rather than settling it as landed. ─────
const fakeRepository: Layer.Layer<CodeHost> = Layer.succeed(
  CodeHost,
  CodeHost.of({
    repositories: {
      // Resolves ANY key to a canned observation: these suites exercise Issue/PR
      // reconciliation, not repository resolution (which is tested on its own).
      resolve: (key) =>
        Effect.succeed(
          Option.some(
            decode(DomainRepository, {
              id: fakeRepositoryId(key.owner, key.name),
              host: key.host,
              owner: key.owner,
              name: key.name,
              refs: [{ name: "main", sha: "a".repeat(40) }],
              observedAt: "2026-07-20T12:00:00.000Z",
            }),
          ),
        ),
    },
    code: { defaultBranch: Effect.succeed("main"), branchExists: () => Effect.succeed(true) },
    issues: {
      getIssue: (number) =>
        Effect.succeed(
          decode(RepositoryIssue, { number, title: "Restart-safe cutover", state: "open" }),
        ),
    },
    pullRequests: {
      // No closing PR and the Issue is open → `isIssueLanded` is false → resume, not settle.
      closingPullRequest: () => Effect.succeed(Option.none()),
      getPullRequest: () => Effect.die(new Error("unused: the Issue never lands in this test")),
    },
  }),
);

// ── the controllable `pi` stand-in (the process-spawner seam) ─────────────────
const AckCommand = Schema.Struct({ type: Schema.String, id: Schema.optionalKey(Schema.String) });
const RESPONDS_TO = new Set(["prompt", "steer", "follow_up", "abort", "get_state"]);

interface ScriptedPi {
  readonly layer: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>;
  /** Resolves once the daemon has spawned the (single) execution process. */
  readonly awaitSpawn: Effect.Effect<void>;
  /** Push one raw NDJSON server message onto the stand-in's stdout. */
  readonly emit: (message: unknown) => Effect.Effect<void>;
}

/**
 * A scripted `pi` at the `ChildProcessSpawner` seam: ACKs the correlated stdin commands so
 * the runner's `send`/`interrupt` resolve, and emits a test-driven queue of NDJSON server
 * messages on stdout. A fresh instance backs EACH daemon build — phase-1's process is
 * killed with the daemon; the restart's `StartupReconcile` resume spawns a fresh one.
 */
const makeScriptedPi: Effect.Effect<ScriptedPi> = Effect.gen(function* () {
  const stdout = yield* Queue.make<unknown, Cause.Done>();
  const spawned = yield* Deferred.make<void>();

  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const stdinBytes = yield* Queue.make<Uint8Array, Cause.Done>();
      const respond = Stream.fromQueue(stdinBytes).pipe(
        Stream.pipeThroughChannel(Ndjson.decodeSchema(AckCommand)({ ignoreEmptyLines: true })),
        Stream.runForEach((command) =>
          command.id !== undefined && RESPONDS_TO.has(command.type)
            ? Queue.offer(stdout, {
                type: "response",
                command: command.type,
                success: true,
                id: command.id,
              })
            : Effect.void,
        ),
        Effect.orDie,
      );
      yield* Effect.forkScoped(respond);
      yield* Deferred.succeed(spawned, undefined);

      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(4321),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        stdin: Sink.forEach<Uint8Array, boolean, never, never>((chunk) =>
          Queue.offer(stdinBytes, chunk),
        ),
        stdout: Stream.fromQueue(stdout).pipe(
          Stream.pipeThroughChannel(Ndjson.encode()),
          Stream.orDie,
        ),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      });
    }),
  );

  return {
    layer: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    awaitSpawn: Deferred.await(spawned),
    emit: (message) => Queue.offer(stdout, message).pipe(Effect.asVoid),
  };
});

// ── canned transcript fragments (raw Pi wire NDJSON) ──────────────────────────
const turnStart = { type: "turn_start" };
const settled = { type: "agent_settled" };
const entryAppended = (id: string, text: string) => ({
  type: "entry_appended",
  entry: {
    type: "message",
    id,
    parentId: null,
    timestamp: "2026-07-19T00:00:00.000Z",
    message: { role: "user", content: text, timestamp: 0 },
  },
});

// ── the seeded materialized plan (an active workstream with one queued Job) ────
/**
 * The repository the seeded workstream is anchored to — `repositoryId` is a real
 * FOREIGN KEY, so it has to be written before the workstream that references it.
 */
const seedRepository = decode(DomainRepository, {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
});

const seedWorkstream = decode(Workstream, {
  id: "ws-seed",
  name: "Restart Seed",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["epic-seed"],
});
const seedEpic = decode(Epic, {
  id: "epic-seed",
  workstreamId: "ws-seed",
  name: "CE4.2 restart",
  status: "active",
  issues: ["issue-seed"],
});
const seedIssue = decode(Issue, {
  id: "issue-seed",
  epicId: "epic-seed",
  number: 69,
  title: "Restart-safe cutover",
  status: "in_progress",
  dependsOn: [],
});
const seedJob = decode(Job, {
  id: "job-seed",
  issueId: "issue-seed",
  kind: "implement",
  status: "queued",
});
const EXECUTION_ID = decode(ExecutionId, "execution-job-seed");

// ── the harness daemon graph: the REAL main.ts graph, fake leaves substituted ──
const harnessDaemon = (config: DaemonConfig, pi: ScriptedPi) =>
  Layer.mergeAll(RpcServer.layer(SprinterRpc), bootLayer).pipe(
    Layer.provide(appLayer(config)),
    Layer.provide(socketProtocolLayer(config)),
    Layer.provide(Layer.mergeAll(fakeRepository, pi.layer)),
    Layer.provide(BunServices.layer),
  );

const testConfig = (dir: string): DaemonConfig => ({
  databasePath: `${dir}/state.db`,
  socketPath: `${dir}/daemon.sock`,
  workspaceRoot: `${dir}/worktrees`,
  repository: { owner: "callajd", repo: "sprinter", token: "unused-in-sandbox" },
  executionResolveTimeout: EXECUTION_RESOLVE_TIMEOUT,
});

const clientLayer = (socketPath: string) =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(BunSocket.layerNet({ path: socketPath })),
    Layer.provide(RpcSerialization.layerNdjson),
  );

it.effect(
  "survives a daemon restart mid-flight: re-dispatches the in-flight Job, re-attaches its execution, and the app resyncs by offset (no work lost or duplicated)",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-ce42-" });
      const config = testConfig(dir);

      // Seed the materialized plan into the durable FILE before the daemon boots (a
      // short-lived non-journaling store whose scope closes before the daemon opens it).
      yield* Effect.gen(function* () {
        const store = yield* StateStore;
        yield* store.repositories.putRepository(seedRepository);
        yield* store.workGraph.putWorkstream(seedWorkstream);
        yield* store.workGraph.putEpic(seedEpic);
        yield* store.workGraph.putIssue(seedIssue);
        yield* store.jobs.putJob(seedJob);
      }).pipe(Effect.provide(layerStateSqlite({ filename: config.databasePath })), Effect.orDie);

      // ── PHASE 1 — build + write: dispatch the Job to a mid-flight `running`, held LIVE,
      //    persisted to the file; then TEAR DOWN the daemon (the process kill). ─────────
      const piA = yield* makeScriptedPi;
      const resumePoint = yield* Effect.gen(function* () {
        return yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(SprinterRpc);

          // The app hydrates FIRST, and what it retains is the pair: the state and the
          // STORE GENERATION it was read in. The generation is the context every later
          // cursor is interpreted in, so a resume hands back both.
          const generation = (yield* client.snapshot()).generation;
          yield* client.control({ workstreamId: seedWorkstream.id, action: "start" });
          // The scripted pi spawns; drive a live turn + a transcript entry, held open (no
          // `agent_settled`), so the Job is durably `running` when the daemon is killed.
          yield* piA.awaitSpawn.pipe(Effect.timeout(HARD_TIMEOUT), Effect.orDie);
          yield* piA.emit(turnStart);
          yield* piA.emit(entryAppended("entry-1", "Working on #69"));

          const running = yield* client.events({}).pipe(
            Stream.filter(
              (oe) =>
                oe.event._tag === "JobChanged" &&
                oe.event.job.id === "job-seed" &&
                oe.event.job.status === "running",
            ),
            Stream.runHead,
            Effect.timeout(HARD_TIMEOUT),
            Effect.map(Option.getOrThrow),
            Effect.orDie,
          );
          return { offset: running.offset, generation };
        }).pipe(Effect.provide(clientLayer(config.socketPath)), Effect.scoped);
      }).pipe(Effect.provide(harnessDaemon(config, piA)), Effect.scoped);

      // The daemon (phase-1 scope) is now torn down: socket unbound, SQLite closed with the
      // Job durably `running` (never settled — the execution was held live at the kill).

      // ── PHASE 2 — restart + read: a FRESH daemon on the SAME file + socket. Its boot
      //    `StartupReconcile` re-dispatches the persisted `running` Job; a fresh client (the
      //    app re-dialing) resumes the events feed from the retained resume point (offset +
      //    generation) and observes the Job
      //    settle — strictly AFTER the cursor (no dup) and reaching `succeeded` (no loss). ──
      const piB = yield* makeScriptedPi;
      yield* Effect.gen(function* () {
        // The restart's resume spawns a fresh pi; ack the re-driven prompt and SETTLE it.
        yield* piB.awaitSpawn.pipe(Effect.timeout(HARD_TIMEOUT), Effect.orDie);
        yield* piB.emit(turnStart);
        yield* piB.emit(entryAppended("entry-2", "Resumed after restart"));
        yield* piB.emit(settled);

        yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(SprinterRpc);

          // The app resyncs: snapshot hydrates, and the offset-resumed events feed carries
          // the post-restart deltas. Collect every delta strictly after the cursor up to
          // the terminal `succeeded` — asserting monotonic, all past the cursor (no dup).
          // The resume carries the generation retained in PHASE 1. A restart on the SAME
          // database at the SAME schema version does NOT start a new generation (the
          // identity is minted by `createSchema`, and no reset happened), so this cursor
          // is genuinely live and the daemon must resume it incrementally — the guard
          // must refuse dead cursors WITHOUT refusing this one.
          const resumed = yield* client
            .events({
              resume: { sinceOffset: resumePoint.offset, generation: resumePoint.generation },
            })
            .pipe(
              Stream.takeUntil(
                (offsetEvent) =>
                  offsetEvent.event._tag === "JobChanged" &&
                  offsetEvent.event.job.id === "job-seed" &&
                  offsetEvent.event.job.status === "succeeded",
              ),
              Stream.runCollect,
              Effect.timeout(HARD_TIMEOUT),
              Effect.orDie,
            );

          // No duplication: every replayed delta is STRICTLY AFTER the resume cursor…
          expect(resumed.length).toBeGreaterThan(0);
          for (const offsetEvent of resumed)
            expect(offsetEvent.offset).toBeGreaterThan(resumePoint.offset);
          // …and monotonically increasing (a gap-free contiguous resume, no reordering loss).
          const offsets = resumed.map((oe) => oe.offset);
          expect([...offsets]).toStrictEqual([...offsets].sort((a, b) => a - b));
          // No loss: the terminal `succeeded` delta arrived over the resumed feed.
          const last = resumed[resumed.length - 1];
          expect(last?.event._tag).toBe("JobChanged");

          // The durable end state: the Job COMPLETED (work continued across the restart)…
          const snap = yield* client.snapshot();
          const finalJob = snap.jobs.find((j) => j.id === "job-seed");
          expect(finalJob?.status).toBe("succeeded");
          expect(finalJob?.transcriptRef).toBeDefined();
          // …with the SAME persisted execution re-attached, not a forked second one: the
          // resume upserted the same id. (A job MAY own several executions now — DE2.2 —
          // so this asserts what the resume did, which the store no longer constrains.)
          const jobExecutions = snap.executions.filter((s) => s.jobId === "job-seed");
          expect(jobExecutions.length).toBe(1);
          expect(jobExecutions[0]?.id).toBe(EXECUTION_ID);
          // The run ENDED: its transcript is sealed, which is the whole of that statement.
          expect(jobExecutions[0]?.transcript._tag).toBe("SealedTranscript");
        }).pipe(Effect.provide(clientLayer(config.socketPath)), Effect.scoped);
      }).pipe(Effect.provide(harnessDaemon(config, piB)), Effect.scoped);
    }).pipe(Effect.provide(BunFileSystem.layer)),
  { timeout: 60_000 },
);

// ── watch-item CE1.2-RR-Q2 — cross-connection visibility of a committed mid-write ──
//
// What this deterministic test ACTUALLY proves: a committed write is visible to a FRESH
// `StateStore` connection opened on the SAME file. The daemon runs SQLite in WAL mode, so a
// second connection opened while the writer is STILL OPEN (no checkpoint/close has run)
// reads the committed `running` Job back intact — cross-connection durability of a committed
// write. This is the in-process stand-in a deterministic test can assert.
//
// What it does NOT prove: survival of a real process crash. It does not SIGKILL a writer
// mid-write and then reopen the on-disk file, so it exercises neither fsync durability nor
// on-disk WAL replay after an ungraceful kill. That genuinely-real crash-recovery
// (SIGKILL mid-write → reboot → WAL replay) is exercised by the runbook, not this test.
it.effect(
  "a committed row is visible to a fresh StateStore connection on the same file (cross-connection durability)",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-ce42-wal-" });
      const filename = `${dir}/state.db`;

      const runningJob = decode(Job, {
        id: "job-wal",
        issueId: "issue-seed",
        kind: "implement",
        status: "running",
        executionId: "execution-job-wal",
      });
      const runningAgent = decode(Agent, {
        id: "agt-wal",
        name: "implementer",
        model: "claude-opus-4-8",
        version: "1.0.0",
        tools: ["read"],
      });
      const startingExecution = decode(Execution, {
        id: "execution-job-wal",
        jobId: "job-wal",
        agentId: "agt-wal",
        mode: "autonomous",
        transcript: { _tag: "LiveTranscript" },
      });

      // Writer store A stays OPEN (its scope is the outer `Effect.gen`) — no close, no
      // checkpoint. It commits the in-flight rows…
      yield* Effect.gen(function* () {
        const writer = yield* StateStore;
        yield* writer.repositories.putRepository(seedRepository);
        yield* writer.workGraph.putWorkstream(seedWorkstream);
        yield* writer.workGraph.putEpic(seedEpic);
        yield* writer.workGraph.putIssue(seedIssue);
        // Agent + job before the execution that names them (both are foreign keys).
        yield* writer.agents.putAgent(runningAgent);
        yield* writer.jobs.putJob(runningJob);
        yield* writer.jobs.putExecution(startingExecution);

        // …and a SEPARATE reader store B, opened on the SAME file while A is still open,
        // replays the WAL and reads the committed mid-write back intact.
        yield* Effect.gen(function* () {
          const reader = yield* StateStore;
          const reloaded = Option.getOrThrow(yield* reader.jobs.getJob(runningJob.id));
          expect(reloaded.status).toBe("running");
          expect(reloaded.executionId).toBe("execution-job-wal");
          const execution = Option.getOrThrow(yield* reader.jobs.getExecutionForJob(runningJob.id));
          expect(execution.id).toBe(startingExecution.id);
        }).pipe(Effect.provide(layerStateSqlite({ filename })), Effect.orDie);
      }).pipe(Effect.provide(layerStateSqlite({ filename })), Effect.orDie);
    }).pipe(Effect.provide(BunFileSystem.layer)),
);
