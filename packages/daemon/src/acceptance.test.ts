/**
 * CE4.1 — Real Issue → PR through the app (deterministic end-to-end acceptance).
 *
 * The ACCEPTANCE test that proves the WHOLE loop COMPOSES: the REAL daemon layer
 * graph (`./main.ts` — file-backed SQLite `StateStore` + journaling, `RpcServer`
 * over a REAL Unix-domain socket with NDJSON framing) driven from the APP SIDE by a
 * REAL `effect/unstable/rpc` client over that REAL socket — byte-for-byte the wire
 * the Swift `UnixSocketTransport`/`RpcBackend` dial. Everything real EXCEPT the two
 * leaves a deterministic test must not touch, substituted as pure `Layer`s
 * (INV-EFFECT-DI):
 *
 *   - a CONTROLLABLE `pi` stand-in at the `ChildProcessSpawner` seam — a scripted
 *     process that acks the runner's commands and emits a canned NDJSON transcript
 *     reaching a terminal settle (NOT the real `pi` binary), and
 *   - a SANDBOXED in-process `CodeHost` that deterministically yields a PR-open
 *     (NOT real GitHub — no `GITHUB_TOKEN`, no network).
 *
 * ## Harness choice (justification)
 *
 * The issue allows either a Swift-spawns-bun integration test or an in-process TS
 * harness speaking the same wire. This is the TS harness, chosen for DETERMINISM:
 * the test drives the scripted pi's stdout timing DIRECTLY, so it can hold a session
 * LIVE across `sessionSend` + `interrupt` before settling it — a control a spawned,
 * opaque process cannot give — with no cross-language toolchain (bun on the Swift CI
 * runner) and no wall-clock races. It stays FAITHFUL to the real wire: the daemon is
 * its real `main.ts` graph bound to a real Unix socket, and the client is a real
 * `RpcClient` over `BunSocket.layerNet` + `RpcSerialization.layerNdjson` — exactly
 * the Swift client's transport. Every wait is bounded by a HARD timeout so a failure
 * fails fast rather than hanging, and the whole graph (socket + StateStore + tempdir)
 * is `Scope`-torn-down in teardown — no leaked process/socket/file.
 *
 * ## What it proves — and, precisely, what it does NOT
 *
 * This is a DETERMINISTIC COMPOSITION test. It proves the pieces are wired and speak
 * the real wire end-to-end; it does NOT drive a real agent or discover a real PR.
 * Concretely, from the app side, over the real socket, against the real daemon:
 *   1. **materialize a plan → workstream** — `createWorkstreamFromPlan`, observed as
 *      a live `WorkstreamChanged` on the `events` feed;
 *   2. **dispatch a Sprinter Issue** — `control(start)` dispatches the issue's Job;
 *      the scripted pi drives a session to a terminal settle, observed as live
 *      `JobChanged` (running → succeeded) + `SessionChanged` deltas — the **Mission
 *      Control** board updating live; the boot reconcile survives against the fake
 *      code host;
 *   3. **session channel resolves** — the **Interactive session** channel:
 *      `sessionEvents` streams the live transcript, `sessionSend` drives input, and
 *      `interrupt` aborts the turn — all resolving the SAME live session with NO
 *      client retry (the registration wire + the server-side bounded-wait CE4.1 close);
 *   4. **a SEEDED PR pairs through the real read model** — the **Inspector** pairing:
 *      the settled Job carries a `transcriptRef` and resolves, session → job → issue,
 *      to a PR. That PR-open is SEED DATA — injected into the materialized plan
 *      (`seedIssue.pr`) and the fake `CodeHost`, NOT produced by the loop and NOT
 *      discovered by the daemon. So this proves the read-model pairing WIRE carries a
 *      PR through to the app; it does NOT prove the daemon can discover an
 *      agent-opened PR. In fact the production daemon has no such path — reconcile
 *      pairs a PR only once it is MERGED (`reconcile.ts`/`github.ts`; see the runbook's
 *      "Known limitation"). A real agent OPENING a PR and it being paired is the
 *      RUNBOOK's real-cutover job, deliberately out of this deterministic test's scope.
 */
import { it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Layer,
  Option,
  Queue,
  Ref,
  Schema,
  Sink,
  Stream,
} from "effect";
import { FileSystem } from "effect/FileSystem";
import { Ndjson } from "effect/unstable/encoding";
import { ChildProcessSpawner } from "effect/unstable/process";
import { RpcClient, RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { BunFileSystem, BunServices, BunSocket } from "@effect/platform-bun";
import { expect } from "vitest";
import { type Snapshot, SprinterRpc, type WorkGraphEvent } from "@sprinter/contract";
import {
  Epic,
  Issue,
  Job,
  PositiveInt,
  PullRequestRef,
  RepositoryKey,
  SessionId,
  Workstream,
} from "@sprinter/domain";
import { Repository as DomainRepository } from "@sprinter/domain";
import { CodeHost, CodeHostError, RepositoryIssue } from "@sprinter/repository";
import { layer as layerStateSqlite, StateStore } from "@sprinter/state";
import { appLayer, bootLayer, type DaemonConfig, socketProtocolLayer } from "./main.ts";
import { SESSION_RESOLVE_TIMEOUT } from "./session-registry.ts";

// ── hard timeout so a hung socket/daemon/event fails fast, never blocks ────────
const HARD_TIMEOUT = "15 seconds";

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

/**
 * A natural key, DECODED — `RepositorySegment` is branded, so a plain object literal is
 * not a `RepositoryKey`.
 */
const repositoryKey = (owner: string, name: string): RepositoryKey =>
  decode(RepositoryKey, { host: "github", owner, name });

// ── the sandboxed GitHub truth: one deterministic OPEN pull request ───────────
//
// The seeded work graph AND the fake `CodeHost` both reflect THIS single fact —
// the PR the agent opened for issue #68, open (unmerged) and awaiting review. It is
// the sandbox's stand-in for GitHub; nothing here reads real GitHub or a token.
const PR_NUMBER = 4210;
const openPr: (typeof PullRequestRef)["Encoded"] = {
  number: PR_NUMBER,
  url: `https://github.com/callajd/sprinter/pull/${PR_NUMBER}`,
  merged: false,
};

/**
 * The sandbox's stand-in for the NUMERIC identifier a code host assigns a repository —
 * a pure hash of the natural key, so it is deterministic and order-independent.
 *
 * It stands in for the host's own id rather than being derived from the key in the
 * shape the id takes: a real adapter mints `RepositoryId` from an identifier a RENAME
 * does not change, never from the mutable `(host, owner, name)` triple. Spelling the
 * fake's id like the real one keeps this harness from agreeing with a broken adapter.
 */
const sandboxRepositoryId = (owner: string, name: string): string => {
  let hash = 0x811c9dc5;
  for (const character of `${owner}/${name}`) {
    hash = Math.imul(hash ^ (character.codePointAt(0) ?? 0), 0x01000193) >>> 0;
  }
  return `repo:github:${hash}`;
};

/**
 * The sandboxed in-process `CodeHost` (NOT real GitHub): every read is a canned,
 * deterministic value consistent with {@link openPr} — the issue is still open, its
 * closing PR is #4210, and that PR is open (unmerged). Wired into the real daemon
 * graph exactly where the GitHub adapter would be (a `Layer` substitution).
 */
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
              id: sandboxRepositoryId(key.owner, key.name),
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
          decode(RepositoryIssue, {
            number,
            title: "Real Issue to PR through the app",
            state: "open",
          }),
        ),
    },
    pullRequests: {
      closingPullRequest: () => Effect.succeed(Option.some(decode(PositiveInt, PR_NUMBER))),
      getPullRequest: (number) =>
        number === PR_NUMBER
          ? Effect.succeed(decode(PullRequestRef, openPr))
          : Effect.die(
              new CodeHostError({ operation: "getPullRequest", detail: `unused #${number}` }),
            ),
    },
  }),
);

// ── the controllable `pi` stand-in (the process-spawner seam) ─────────────────

/** Minimal boundary schema to read a stdin command's `type` + correlation `id`. */
const AckCommand = Schema.Struct({ type: Schema.String, id: Schema.optionalKey(Schema.String) });

/** The correlated commands the stand-in must ACK so `transport.request` resolves. */
const RESPONDS_TO = new Set(["prompt", "steer", "follow_up", "abort", "get_state"]);

interface ScriptedPi {
  /** The `ChildProcessSpawner` `Layer` substituting the real spawner (INV-EFFECT-DI). */
  readonly layer: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>;
  /** Resolves once the daemon has spawned the (single) session process. */
  readonly awaitSpawn: Effect.Effect<void>;
  /** Push one raw NDJSON server message onto the stand-in's stdout. */
  readonly emit: (message: unknown) => Effect.Effect<void>;
}

/**
 * A scripted `pi` process at the `ChildProcessSpawner` seam: it decodes the
 * runner's stdin commands and ACKs the correlated ones (so `handle.send` /
 * `handle.interrupt` — hence the `sessionSend` / `interrupt` RPCs — resolve), and
 * its stdout is a test-driven queue of raw NDJSON server messages. The test emits a
 * canned transcript (`turn_start` → `entry_appended` → … → `agent_settled`) on its
 * own schedule, so a session can be held LIVE across the session-channel drive and
 * only THEN settled — fully deterministic, no `pi` binary, no wall-clock.
 */
const makeScriptedPi: Effect.Effect<ScriptedPi> = Effect.gen(function* () {
  const stdout = yield* Queue.make<unknown, Cause.Done>();
  const spawned = yield* Deferred.make<void>();

  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const stdinBytes = yield* Queue.make<Uint8Array, Cause.Done>();

      // The auto-responder: decode each stdin command and ACK the correlated ones
      // by echoing its id, so the transport's pending-request deferred completes.
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
    timestamp: "2026-07-18T00:00:00.000Z",
    message: { role: "user", content: text, timestamp: 0 },
  },
});

// ── the seeded "materialized plan" (post-planning, agent-opened-PR state) ─────
/**
 * The repository the seeded workstream is anchored to — `repositoryId` is a real
 * FOREIGN KEY, so it has to be written before the workstream that references it.
 */
const SEED_REPOSITORY_ID = sandboxRepositoryId("callajd", "sprinter");

const seedRepository = decode(DomainRepository, {
  id: SEED_REPOSITORY_ID,
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
});

const seedWorkstream = decode(Workstream, {
  id: "ws-seed",
  name: "Convergence Seed",
  repositoryId: SEED_REPOSITORY_ID,
  status: "active",
  epics: ["epic-seed"],
});
const seedEpic = decode(Epic, {
  id: "epic-seed",
  workstreamId: "ws-seed",
  name: "CE4 acceptance",
  status: "active",
  issues: ["issue-seed"],
});
const seedIssue = decode(Issue, {
  id: "issue-seed",
  epicId: "epic-seed",
  number: 68,
  title: "Real Issue to PR through the app",
  status: "in_review",
  dependsOn: [],
  pr: openPr,
});
const seedJob = decode(Job, {
  id: "job-seed",
  issueId: "issue-seed",
  kind: "implement",
  status: "queued",
});
const SESSION_ID = decode(SessionId, "session-job-seed");

/**
 * A repository segment carrying the domain's BRAND but NONE of its checks.
 *
 * This is not a way around `RepositorySegment` — it is the THREAT MODEL, made
 * constructible. The segment is client-supplied, so the guard's whole job is to hold
 * against a client that never ran our schema; a test that could only build well-formed
 * segments could not put a malformed one on the wire and so could not test the guard at
 * all. It is a real decode of a real schema (no cast, INV-NOCAST) — just a deliberately
 * permissive one, confined to this file.
 */
const AttackerSegment = Schema.String.pipe(Schema.brand("RepositorySegment"));

/**
 * A `CodeHost` that RECORDS every natural key its `resolve` is handed, then answers as
 * the sandbox does. The recording is the assertion surface for "no adapter ever sees a
 * rejected value".
 */
const recordingRepository = (asked: Ref.Ref<ReadonlyArray<string>>): Layer.Layer<CodeHost> =>
  Layer.succeed(
    CodeHost,
    CodeHost.of({
      repositories: {
        resolve: (key) =>
          Ref.update(asked, (seen) => [...seen, `${key.owner}/${key.name}`]).pipe(
            Effect.andThen(
              Effect.succeed(
                Option.some(
                  decode(DomainRepository, {
                    id: sandboxRepositoryId(key.owner, key.name),
                    host: key.host,
                    owner: key.owner,
                    name: key.name,
                    refs: [{ name: "main", sha: "a".repeat(40) }],
                    observedAt: "2026-07-20T12:00:00.000Z",
                  }),
                ),
              ),
            ),
          ),
      },
      code: { defaultBranch: Effect.succeed("main"), branchExists: () => Effect.succeed(true) },
      issues: {
        getIssue: (number) =>
          Effect.succeed(decode(RepositoryIssue, { number, title: "unused", state: "open" })),
      },
      pullRequests: {
        closingPullRequest: () => Effect.succeed(Option.none()),
        getPullRequest: (number) =>
          Effect.die(
            new CodeHostError({ operation: "getPullRequest", detail: `unused #${number}` }),
          ),
      },
    }),
  );

// ── the harness daemon graph: the REAL main.ts graph, fake leaves substituted ──
const harnessDaemon = (
  config: DaemonConfig,
  pi: ScriptedPi,
  // The CodeHost is a PARAMETER so a test can observe what the port was asked, without
  // a second copy of the whole graph.
  codeHost: Layer.Layer<CodeHost> = fakeRepository,
) =>
  Layer.mergeAll(RpcServer.layer(SprinterRpc), bootLayer).pipe(
    Layer.provide(appLayer(config)),
    Layer.provide(socketProtocolLayer(config)),
    // Fake leaves win (provided nearest): the sandboxed CodeHost + the scripted pi
    // spawner. Real Bun services satisfy the rest (FileSystem/Path/socket platform).
    Layer.provide(Layer.mergeAll(codeHost, pi.layer)),
    Layer.provide(BunServices.layer),
  );

const testConfig = (dir: string): DaemonConfig => ({
  databasePath: `${dir}/state.db`,
  socketPath: `${dir}/daemon.sock`,
  workspaceRoot: `${dir}/worktrees`,
  repository: { owner: "callajd", repo: "sprinter", token: "unused-in-sandbox" },
  sessionResolveTimeout: SESSION_RESOLVE_TIMEOUT,
});

// ── the real socket client (the app's transport) ──────────────────────────────
const clientLayer = (socketPath: string) =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(BunSocket.layerNet({ path: socketPath })),
    Layer.provide(RpcSerialization.layerNdjson),
  );

it.effect(
  "composes the Issue → PR loop from the app side over the real socket (seeded PR pairs through the read model)",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-ce41-" });
      const config = testConfig(dir);
      const pi = yield* makeScriptedPi;

      // Seed the "materialized plan" into the durable FILE BEFORE the daemon boots,
      // so the boot reconcile runs against the sandboxed CodeHost (which reports the
      // issue still open → conservatively no-op, keeping the seeded open PR) and the
      // app hydrates the plan on connect. A separate short-lived StateStore instance
      // on the same file; its scope closes (the "process exit") before the daemon
      // opens the file.
      yield* Effect.gen(function* () {
        const store = yield* StateStore;
        yield* store.repositories.putRepository(seedRepository);
        yield* store.workGraph.putWorkstream(seedWorkstream);
        yield* store.workGraph.putEpic(seedEpic);
        yield* store.workGraph.putIssue(seedIssue);
        yield* store.jobs.putJob(seedJob);
      }).pipe(Effect.provide(layerStateSqlite({ filename: config.databasePath })), Effect.orDie);

      // Bring up the REAL daemon (real socket bind, file StateStore, boot reconcile)
      // then drive it from the app side; the scope tears it all down at the end.
      yield* Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(SprinterRpc);

          const waitForDelta = (
            predicate: (event: WorkGraphEvent) => boolean,
          ): Effect.Effect<WorkGraphEvent> =>
            client.events({}).pipe(
              Stream.map((offsetEvent) => offsetEvent.event),
              Stream.filter(predicate),
              Stream.runHead,
              Effect.timeout(HARD_TIMEOUT),
              Effect.map(Option.getOrThrow),
              Effect.orDie,
            );

          // ── baseline: the seeded plan hydrates, PR-open already present ──────
          const base: Snapshot = yield* client.snapshot();
          expect(base.workstreams.map((w) => w.id)).toContain("ws-seed");
          const baseIssue = base.issues.find((i) => i.id === "issue-seed");
          expect(baseIssue?.status).toBe("in_review");
          expect(baseIssue?.pr?.number).toBe(PR_NUMBER);
          expect(base.jobs.find((j) => j.id === "job-seed")?.status).toBe("queued");

          // ── (1) materialize a plan → workstream (live board update) ─────────
          const newId = yield* client.createWorkstreamFromPlan({
            plan: {
              name: "CE4 New Plan",
              repository: repositoryKey("callajd", "sprinter"),
              spec: "drive the loop",
            },
          });
          const materialized = yield* waitForDelta(
            (e) => e._tag === "WorkstreamChanged" && e.workstream.id === newId,
          );
          expect(materialized._tag).toBe("WorkstreamChanged");

          // ── (2) dispatch the Issue's Job (Mission Control board updates live) ─
          yield* client.control({ workstreamId: seedWorkstream.id, action: "start" });

          // The scripted pi spawns; drive a live turn + a transcript entry, held open.
          yield* pi.awaitSpawn.pipe(Effect.timeout(HARD_TIMEOUT), Effect.orDie);
          yield* pi.emit(turnStart);
          yield* pi.emit(entryAppended("entry-1", "Working on #68: opening a PR"));

          const running = yield* waitForDelta(
            (e) => e._tag === "JobChanged" && e.job.id === "job-seed" && e.job.status === "running",
          );
          expect(running._tag).toBe("JobChanged");
          yield* waitForDelta(
            (e) => e._tag === "SessionChanged" && e.session.id === "session-job-seed",
          );

          // ── (3) session drives + is interruptible (Interactive session) ──────
          // `sessionEvents` resolves the SAME live session (the registration wire
          // CE4.1 closes) and streams the transcript. NO client retry: opening the
          // channel immediately after the `running` delta must resolve, because the
          // daemon's `SessionRegistry.resolve` bounded-WAITS out the
          // register-after-dispatch window server-side (the real Swift app carries no
          // retry — that is exactly what this proves). Bounded hard so a genuine hang
          // still fails fast.
          // Each streamed item is an `OffsetSessionEvent`: unwrap `.event` to
          // the durable transcript entry the daemon journaled as the live session ran.
          const transcript = yield* client.sessionEvents({ sessionId: SESSION_ID }).pipe(
            Stream.filter((item) => item.event._tag === "EntryAppended"),
            Stream.runHead,
            Effect.timeout(HARD_TIMEOUT),
            Effect.map(Option.getOrThrow),
            Effect.orDie,
          );
          expect(transcript.event._tag).toBe("EntryAppended");

          // Drive input and interrupt the turn — both must resolve the live session
          // (never `SessionNotFound`), proving the channel is wired end-to-end.
          yield* client
            .sessionSend({ sessionId: SESSION_ID, input: { text: "keep going", mode: "steer" } })
            .pipe(Effect.timeout(HARD_TIMEOUT), Effect.orDie);
          yield* client
            .interrupt({ sessionId: SESSION_ID })
            .pipe(Effect.timeout(HARD_TIMEOUT), Effect.orDie);

          // ── settle the session → the Job completes (live board update) ───────
          yield* pi.emit(settled);
          const succeeded = yield* waitForDelta(
            (e) =>
              e._tag === "JobChanged" && e.job.id === "job-seed" && e.job.status === "succeeded",
          );
          expect(succeeded._tag).toBe("JobChanged");

          // ── (4) transcript paired with the PR (Inspector pairing) ───────────
          const finalSnap: Snapshot = yield* client.snapshot();
          const finalJob = finalSnap.jobs.find((j) => j.id === "job-seed");
          expect(finalJob?.status).toBe("succeeded");
          // The Job carries a durable transcript reference (the "transcript" side)…
          expect(finalJob?.transcriptRef).toBeDefined();
          // …and resolves — session → job → issue — to the PR (the "PR" side), exactly
          // the Inspector's session↔PR pairing. NB: this open PR is SEED DATA
          // (`seedIssue.pr` + the fake `CodeHost`), carried through the real read
          // model — it is NOT discovered by the daemon (production reconcile pairs a PR
          // only once MERGED; see the runbook's "Known limitation"). This asserts the
          // pairing WIRE, not open-PR discovery.
          const session = finalSnap.sessions.find((s) => s.id === "session-job-seed");
          expect(session?.jobId).toBe("job-seed");
          const pairedIssue = finalSnap.issues.find((i) => i.id === finalJob?.issueId);
          expect(pairedIssue?.pr?.number).toBe(PR_NUMBER);
          expect(pairedIssue?.pr?.merged).toBe(false);
        }).pipe(Effect.provide(clientLayer(config.socketPath)), Effect.scoped);
      }).pipe(Effect.provide(harnessDaemon(config, pi)), Effect.scoped);
    }).pipe(Effect.provide(BunFileSystem.layer)),
  { timeout: 60_000 },
);

// ── B1 (round 2, N1) — the traversal guard is enforced at the RPC DECODE ───────
//
// `RepositorySegment` rejects `.` and `..` in the SCHEMA, and the load-bearing half of
// that claim is WHERE the rejection happens: on DECODE, at the RPC boundary, before any
// adapter is handed the value. Asserting it against the domain schema alone proves only
// that a decode rejects it — not that the daemon actually decodes what a client sent.
//
// So this drives a poisoned key over the REAL socket with REAL NDJSON serialization
// (the only harness in the suite where the server genuinely decodes the payload — the
// in-memory `RpcTest` client is a NO-SERIALIZATION transport and would prove nothing
// here), and asserts the `CodeHost` was NEVER ASKED. A guard that merely returned an
// error after the adapter had already issued the authenticated request would fail this.
it.effect(
  "a traversal segment is refused at the RPC decode — the CodeHost is never asked",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-b1-" });
      const config = testConfig(dir);
      const pi = yield* makeScriptedPi;
      // Every `resolve` the daemon performs lands here. It must stay empty.
      const asked = yield* Ref.make<ReadonlyArray<string>>([]);

      yield* Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const client = yield* RpcClient.make(SprinterRpc);
          // A client that did NOT run the domain's schema — which is the entire threat
          // model, since the segment is client-supplied. `AttackerSegment` carries the
          // brand WITHOUT the checks, so the poisoned key can be built here (no cast,
          // INV-NOCAST) exactly as a hostile client would put it on the wire.
          const poisoned: RepositoryKey = {
            host: "github",
            owner: decode(AttackerSegment, ".."),
            name: decode(AttackerSegment, "user"),
          };
          const outcome = yield* client
            .createWorkstreamFromPlan({
              plan: { name: "Traversal", repository: poisoned, spec: "real spec" },
            })
            .pipe(Effect.exit);
          // The call did not succeed…
          expect(Exit.isFailure(outcome)).toBe(true);
          // …and, the point of the test: no adapter was ever handed the value, so no
          // authenticated request was steered anywhere.
          expect(yield* Ref.get(asked)).toStrictEqual([]);

          // POSITIVE CONTROL, in the same test: a WELL-FORMED key over the same wire
          // does reach the host — so the assertion above is the guard working, not the
          // recorder being wired to nothing.
          yield* client.createWorkstreamFromPlan({
            plan: {
              name: "Legitimate",
              repository: repositoryKey("callajd", "sprinter"),
              spec: "real spec",
            },
          });
          expect(yield* Ref.get(asked)).toStrictEqual(["callajd/sprinter"]);
        }).pipe(Effect.provide(clientLayer(config.socketPath)), Effect.scoped);
      }).pipe(Effect.provide(harnessDaemon(config, pi, recordingRepository(asked))), Effect.scoped);
    }).pipe(Effect.provide(BunFileSystem.layer)),
  { timeout: 60_000 },
);
