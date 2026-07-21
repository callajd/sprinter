/**
 * `StartupReconcile` coverage (AE5.1) — the restart-safety service exercised
 * against the in-memory {@link StateStore} (`layerMemory`), a FAKE {@link CodeHost}
 * (a canned host, no HTTP), and a FAKE {@link JobRunner} (records dispatches, no
 * `pi` process). Deterministic and OFFLINE (INV-GATE / INV-PORT): the service
 * depends only on the three ports.
 *
 * The suite proves the AE5.1 Done criteria:
 *
 * - **reconcile roll-up with one host error isolated** — a single Issue's host
 *   failure (a 404/403/429) does not abort the whole roll-up; the remaining Issues
 *   still land (carried AE3.2 / #27 F4);
 * - **running-Job resume onto the SAME execution id** — a Job persisted `running` is
 *   re-dispatched carrying its persisted `executionId`, never a new one;
 * - **no double-run** — an already-terminal Job is not re-dispatched, and a Job
 *   whose Issue reconciled to landed is not re-run;
 * - **control state respected** — no Job of a `done`/`blocked` Workstream is
 *   re-dispatched (carried AE4.1 / #30 N1);
 * - **resume-failure isolation** — one Job whose dispatch fails does not abort the
 *   startup; the other in-flight Jobs still resume.
 *
 * The durable-persistence restart premise (build-write-rebuild-read on a real
 * tmpfile) is proven separately in {@link ./restart-durability.test.ts}.
 */
import { it } from "@effect/vitest";
import { type Context, Effect, Layer, Option, Queue, Schema } from "effect";
import { expect } from "vitest";
import {
  Agent,
  Epic,
  Execution,
  ExecutionEvent,
  isExecutionLive,
  Issue,
  Job,
  type JobResult,
  PositiveInt,
  PullRequestRef,
  Workstream,
} from "@sprinter/domain";
import { Repository as DomainRepository } from "@sprinter/domain";
import { ExecutionRunnerError, JobRunner } from "@sprinter/job";
import { CodeHost, CodeHostError, RepositoryIssue } from "@sprinter/repository";
import { layerMemory, StateStore, StateStoreError } from "@sprinter/state";
import { layer as startupLayer, StartupReconcile } from "./startup-reconcile.ts";

// ============================================================================
// Fixtures — decoded through the owned schemas (no casts, INV-NOCAST)
// ============================================================================

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

const posInt = (n: number): PositiveInt => decode(PositiveInt, n);

/**
 * The repository the {@link workstream} fixtures are anchored to — `repositoryId` is a
 * real FOREIGN KEY, so it has to be stored before anything references it.
 */
const repository = decode(DomainRepository, {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
});

const workstream = (over: Partial<(typeof Workstream)["Encoded"]> = {}) =>
  decode(Workstream, {
    id: "ws-a",
    name: "Track A",
    repositoryId: "repo:github:1296269",
    status: "active",
    epics: ["epic-1"],
    ...over,
  });

const epic = (over: Partial<(typeof Epic)["Encoded"]> = {}) =>
  decode(Epic, {
    id: "epic-1",
    workstreamId: "ws-a",
    name: "Epic",
    status: "active",
    issues: ["issue-1"],
    ...over,
  });

const issue = (number: number, over: Partial<(typeof Issue)["Encoded"]> = {}) =>
  decode(Issue, {
    id: `issue-${number}`,
    epicId: "epic-1",
    number,
    title: `Issue ${number}`,
    status: "in_progress",
    dependsOn: [],
    ...over,
  });

const job = (over: Partial<(typeof Job)["Encoded"]> = {}) =>
  decode(Job, {
    id: "job-1",
    issueId: "issue-1",
    kind: "implement",
    status: "running",
    executionId: "execution-job-1",
    ...over,
  });

const execution = (over: Partial<(typeof Execution)["Encoded"]> = {}) =>
  decode(Execution, {
    id: "execution-job-1",
    jobId: "job-1",
    agentId: "agt-1",
    mode: "autonomous",
    transcript: { _tag: "LiveTranscript" },
    ...over,
  });

/** The registry revision every {@link execution} fixture is attributed to. */
const agent = decode(Agent, {
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read"],
});

/**
 * Store a job and its execution, in the order the FOREIGN KEYs require: the agent
 * revision and the job row must exist before an execution can name them (DE2.2).
 */
const seedRun = (
  store: Context.Service.Shape<typeof StateStore>,
  jobRow: Job,
  executionRow: Execution,
) =>
  Effect.gen(function* () {
    yield* store.agents.putAgent(agent);
    yield* store.jobs.putJob(jobRow);
    yield* store.jobs.putExecution(executionRow);
  });

const pullRef = (number: number, merged: boolean): PullRequestRef =>
  decode(PullRequestRef, {
    number,
    url: `https://github.com/callajd/sprinter/pull/${number}`,
    merged,
  });

// ============================================================================
// Fakes — a canned host and a recording runner (no HTTP, no `pi`)
// ============================================================================

interface HostState {
  /** Issue number → host state; absent ⇒ still open. */
  readonly issues: ReadonlyMap<number, "open" | "closed">;
  /** Issue number → the PR number that closes it; absent ⇒ no closing PR. */
  readonly closing: ReadonlyMap<number, number>;
  /** PR number → whether it merged; absent ⇒ not merged. */
  readonly pulls: ReadonlyMap<number, boolean>;
  /** Issue numbers whose `getIssue` fails with a host error (a 404/403/429). */
  readonly failing: ReadonlySet<number>;
}

const host = (over: Partial<HostState> = {}): HostState => ({
  issues: new Map(),
  closing: new Map(),
  pulls: new Map(),
  failing: new Set(),
  ...over,
});

const repoIssue = (number: number, state: "open" | "closed"): RepositoryIssue =>
  decode(RepositoryIssue, { number, title: `Issue ${number}`, state });

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

const fakeRepository = (state: HostState): Layer.Layer<CodeHost> =>
  Layer.succeed(
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
      code: {
        defaultBranch: Effect.succeed("main"),
        branchExists: () => Effect.succeed(true),
      },
      issues: {
        getIssue: (number) =>
          state.failing.has(number)
            ? Effect.fail(
                new CodeHostError({
                  operation: "getIssue",
                  kind: "unreachable",
                  detail: `host 404 #${number}`,
                }),
              )
            : Effect.succeed(repoIssue(number, state.issues.get(number) ?? "open")),
      },
      pullRequests: {
        closingPullRequest: (issueNumber) => {
          const pr = state.closing.get(issueNumber);
          return Effect.succeed(pr === undefined ? Option.none() : Option.some(posInt(pr)));
        },
        getPullRequest: (number) =>
          Effect.succeed(pullRef(number, state.pulls.get(number) ?? false)),
      },
    }),
  );

const okResult: JobResult = { status: "succeeded" };

/**
 * A recording {@link JobRunner}: offers every dispatched {@link Job} to `log` (a
 * Queue, so the BACKGROUND resume fibers are observed deterministically — a
 * `Queue.take` blocks until the forked fiber offers), then succeeds.
 */
const recordingRunner = (log: Queue.Enqueue<Job>): Layer.Layer<JobRunner> =>
  Layer.succeed(
    JobRunner,
    JobRunner.of({
      dispatch: (dispatchedJob) => Queue.offer(log, dispatchedJob).pipe(Effect.as(okResult)),
    }),
  );

/** A {@link JobRunner} whose `dispatch` fails for one job id, else records + succeeds. */
const flakyRunner = (log: Queue.Enqueue<Job>, failFor: string): Layer.Layer<JobRunner> =>
  Layer.succeed(
    JobRunner,
    JobRunner.of({
      dispatch: (dispatchedJob) =>
        dispatchedJob.id === failFor
          ? Effect.fail(new ExecutionRunnerError({ operation: "run", detail: "spawn refused" }))
          : Queue.offer(log, dispatchedJob).pipe(Effect.as(okResult)),
    }),
  );

/**
 * Compose the service-under-test over one SHARED base (`layerMemory` + fakes) via
 * `provideMerge`, so the `StateStore` the test seeds is the exact instance
 * `StartupReconcile` reads (a plain `Layer.provide` would build a second store).
 */
const testLayer = (
  state: HostState,
  runner: Layer.Layer<JobRunner>,
  store: Layer.Layer<StateStore, StateStoreError> = layerMemory,
) => startupLayer.pipe(Layer.provideMerge(Layer.mergeAll(store, fakeRepository(state), runner)));

/** A {@link StateStore} whose `executionLog.maxOffset` ALWAYS fails; everything else delegates. */
const failMaxOffset: Layer.Layer<StateStore, StateStoreError> = Layer.effect(
  StateStore,
  Effect.gen(function* () {
    const base = yield* StateStore;
    return StateStore.of({
      ...base,
      executionLog: {
        ...base.executionLog,
        maxOffset: () =>
          Effect.fail(new StateStoreError({ operation: "maxOffset", detail: "transient" })),
      },
    });
  }),
).pipe(Layer.provide(layerMemory));

// ============================================================================
// reconcile roll-up with one host error isolated (F4)
// ============================================================================

it.effect("isolates one Issue's host error and still lands the rest of the roll-up", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream({ epics: ["epic-1"] }));
      yield* store.workGraph.putEpic(epic({ issues: ["issue-1", "issue-2"] }));
      // issue-1's host read fails (a deleted-issue 404); issue-2 is closed + merged.
      yield* store.workGraph.putIssue(issue(1));
      yield* store.workGraph.putIssue(issue(2, { id: "issue-2" }));

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;

      // The run did NOT abort on the host error, and reconciled the one workstream.
      expect(summary.reconciledWorkstreams).toBe(1);

      // issue-2 landed despite issue-1's host failure; issue-1 is left unchanged.
      const i1 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(1).id));
      const i2 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(2, { id: "issue-2" }).id));
      expect(i1.status).toBe("in_progress");
      expect(i2.status).toBe("done");
      expect(i2.pr?.merged).toBe(true);
    }).pipe(
      Effect.provide(
        testLayer(
          host({
            issues: new Map([[2, "closed"]]),
            closing: new Map([[2, 20]]),
            pulls: new Map([[20, true]]),
            failing: new Set([1]),
          }),
          recordingRunner(dispatched),
        ),
      ),
    );
  }),
);

// ============================================================================
// running-Job resume onto the SAME persisted execution id
// ============================================================================

it.effect("resumes a running Job onto its persisted execution id, never a new one", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream());
      yield* store.workGraph.putEpic(epic());
      yield* store.workGraph.putIssue(issue(1));
      yield* seedRun(store, job(), execution());

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;
      expect(summary.resumed).toStrictEqual(["job-1"]);
      expect(summary.skipped).toStrictEqual([]);

      // The in-flight job was re-dispatched (in the background), carrying its
      // PERSISTED execution id — `Queue.take` awaits the forked resume fiber.
      const redispatched = yield* Queue.take(dispatched);
      expect(redispatched.id).toBe("job-1");
      expect(redispatched.executionId).toBe("execution-job-1");
    }).pipe(
      Effect.provide(
        testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
      ),
    );
  }),
);

// ============================================================================
// no double-run — terminal Job, and a Job whose Issue reconciled to landed
// ============================================================================

it.effect("does not re-dispatch an already-terminal Job", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream());
      yield* store.workGraph.putEpic(epic());
      yield* store.workGraph.putIssue(issue(1));
      yield* seedRun(
        store,
        job({ status: "succeeded" }),
        execution({ transcript: { _tag: "SealedTranscript", lastOffset: 3 } }),
      );

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;

      // A terminal job is not a resume candidate at all — nothing resumed, nothing
      // settled, nothing dispatched.
      expect(summary.resumed).toStrictEqual([]);
      expect(summary.skipped).toStrictEqual([]);
      expect(yield* Queue.size(dispatched)).toBe(0);
    }).pipe(
      Effect.provide(
        testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
      ),
    );
  }),
);

it.effect(
  "does not re-run a running Job whose Issue reconciled to landed; settles it succeeded",
  () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<Job>();
      yield* Effect.gen(function* () {
        const store = yield* StateStore;
        yield* store.repositories.putRepository(repository);
        yield* store.workGraph.putWorkstream(workstream());
        yield* store.workGraph.putEpic(epic());
        yield* store.workGraph.putIssue(issue(1));
        yield* seedRun(store, job(), execution());

        const startup = yield* StartupReconcile;
        const summary = yield* startup.run;

        // Reconcile lands issue-1 (closed + merged), so its running job is held back and
        // settled to `succeeded` (its work landed) — no re-dispatch, no durable limbo.
        const i1 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(1).id));
        expect(i1.status).toBe("done");
        expect(summary.resumed).toStrictEqual([]);
        expect(summary.skipped).toStrictEqual(["job-1"]);
        expect(yield* Queue.size(dispatched)).toBe(0);
        const j1 = Option.getOrThrow(yield* store.jobs.getJob(job().id));
        expect(j1.status).toBe("succeeded");
        // ROOT FIX (CE4.1-R4): a landed Job's EXECUTION has its transcript SEALED, so
        // nothing is left looking live to the execution-resolve gate.
        const s1 = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job().id));
        expect(isExecutionLive(s1)).toBe(false);
      }).pipe(
        Effect.provide(
          testLayer(
            host({
              issues: new Map([[1, "closed"]]),
              closing: new Map([[1, 10]]),
              pulls: new Map([[10, true]]),
            }),
            recordingRunner(dispatched),
          ),
        ),
      );
    }),
);

// ============================================================================
// control state — a paused/cancelled (blocked) or done Workstream is skipped
// ============================================================================

it.effect("does not re-dispatch Jobs of a blocked Workstream; re-queues them for resume", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream({ status: "blocked" }));
      yield* store.workGraph.putEpic(epic({ status: "blocked" }));
      yield* store.workGraph.putIssue(issue(1));
      yield* seedRun(store, job(), execution());

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;

      // The blocked (paused) workstream stays blocked; its running job is not resumed
      // now, but is re-queued so a later `control resume` re-dispatches it (no limbo).
      const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream().id));
      expect(ws.status).toBe("blocked");
      expect(summary.resumed).toStrictEqual([]);
      expect(summary.skipped).toStrictEqual(["job-1"]);
      expect(yield* Queue.size(dispatched)).toBe(0);
      const j1 = Option.getOrThrow(yield* store.jobs.getJob(job().id));
      expect(j1.status).toBe("queued");
      // ROOT FIX (CE4.1-R4): the EXECUTION's transcript is SEALED alongside the
      // re-queued Job, so no LIVE execution orphan survives to stall the
      // execution-resolve gate. A later resume re-attaches this same id and re-opens it.
      const s1 = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job().id));
      expect(isExecutionLive(s1)).toBe(false);
    }).pipe(
      Effect.provide(
        testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
      ),
    );
  }),
);

it.effect("seals EVERY execution a settled Job owns, not just the tree's ROOT", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream({ status: "blocked" }));
      yield* store.workGraph.putEpic(epic({ status: "blocked" }));
      yield* store.workGraph.putIssue(issue(1));
      // A job owning a TREE: the root and a CHILD (a subagent), both LIVE. A settle that
      // read `getExecutionForJob` saw only the root — so the child kept its
      // `LiveTranscript` forever, `isExecutionLive` stayed true on it, and `resolveLive`
      // bounded-WAITED on it every time the (now `queued`, i.e. still mid-dispatch) job
      // was resolved. That is the CE4.1-R4 stall the seal exists to prevent, merely moved
      // off the root and onto a sibling.
      yield* seedRun(store, job(), execution());
      yield* store.jobs.putExecution(
        execution({ id: "execution-child", parent: "execution-job-1" }),
      );
      // A THIRD execution that is ALREADY SEALED. The settle must SKIP it rather than
      // re-seal it: a sealed range is settled and immutable, and re-writing its
      // `lastOffset` from a fresh read would move a range a client may already have
      // cached (the whole cacheability claim).
      yield* store.jobs.putExecution(
        execution({
          id: "execution-done",
          parent: "execution-job-1",
          transcript: { _tag: "SealedTranscript", lastOffset: 99 },
        }),
      );
      // Distinct per-execution extents, so a seal that reused ONE offset for the whole
      // tree would be visible rather than coincidentally right.
      const entry = decode(ExecutionEvent, { _tag: "ExecutionIdle" });
      yield* store.executionLog.append(execution().id, entry);
      yield* store.executionLog.append(execution({ id: "execution-child" }).id, entry);
      yield* store.executionLog.append(execution({ id: "execution-child" }).id, entry);

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;
      expect(summary.skipped).toStrictEqual(["job-1"]);
      expect(yield* Queue.size(dispatched)).toBe(0);

      // EVERY execution is sealed — the whole tree, not the root alone.
      const all = yield* store.jobs.listExecutionsForJob(job().id);
      expect(all.map((e) => e.id)).toStrictEqual([
        "execution-child",
        "execution-done",
        "execution-job-1",
      ]);
      expect(all.every((e) => !isExecutionLive(e))).toBe(true);
      // The ALREADY-SEALED one was SKIPPED, not re-sealed: its settled range is exactly
      // what it was, even though its own log is empty (a re-seal would have moved it to 0).
      expect(all.find((e) => e.id === "execution-done")?.transcript).toStrictEqual({
        _tag: "SealedTranscript",
        lastOffset: 99,
      });
      // …and each at ITS OWN log's extent, not a shared one.
      const sealedAt = new Map(
        all.map((e) => [
          e.id,
          e.transcript._tag === "SealedTranscript" ? e.transcript.lastOffset : -1,
        ]),
      );
      expect(sealedAt.get(execution().id)).toBe(
        yield* store.executionLog.maxOffset(execution().id),
      );
      expect(sealedAt.get(execution({ id: "execution-child" }).id)).toBe(
        yield* store.executionLog.maxOffset(execution({ id: "execution-child" }).id),
      );
      expect(sealedAt.get(execution().id)).not.toBe(
        sealedAt.get(execution({ id: "execution-child" }).id),
      );
    }).pipe(
      Effect.provide(
        testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
      ),
    );
  }),
);

// The settle's `maxOffset` fallback, pinned. It is the LOAD-BEARING premise of the whole
// lower-bound disposition of `Transcript.lastOffset`: sealing at `0` rather than refusing to
// seal is what guarantees a settle can never leave an execution LIVE, and an execution left live
// because an extent read hiccupped is the CE4.1-R4 stall — strictly worse than an understated
// extent. Unlike the dispatch path there is NO local high-water mark to prefer here: the run
// whose appends it would have counted belongs to a process that is already gone, which is why
// this settle runs at all. So `0` is the honest answer, and liveness must still clear.
it.effect("seals at the LOWER BOUND 0 when the settle's `maxOffset` read fails", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream({ status: "blocked" }));
      yield* store.workGraph.putEpic(epic({ status: "blocked" }));
      yield* store.workGraph.putIssue(issue(1));
      yield* seedRun(store, job(), execution());
      // Entries DO exist — so `0` is demonstrably an understatement rather than a
      // coincidentally-correct empty log, which is exactly the lower-bound claim.
      const entry = decode(ExecutionEvent, { _tag: "ExecutionIdle" });
      yield* store.executionLog.append(execution().id, entry);
      yield* store.executionLog.append(execution().id, entry);

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;
      expect(summary.skipped).toStrictEqual(["job-1"]);

      const sealed = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job().id));
      expect(sealed.transcript).toStrictEqual({ _tag: "SealedTranscript", lastOffset: 0 });
      // The whole point of the fallback: LIVENESS STILL CLEARS, so no gate sees a
      // forever-running execution, and the settled Job is durably settled.
      expect(isExecutionLive(sealed)).toBe(false);
      expect(Option.getOrThrow(yield* store.jobs.getJob(job().id)).status).toBe("queued");
    }).pipe(
      Effect.provide(
        testLayer(
          host({ issues: new Map([[1, "open"]]) }),
          recordingRunner(dispatched),
          failMaxOffset,
        ),
      ),
    );
  }),
);

it.effect("settles a running Job of a cancelled Workstream to cancelled, not resumed (CE5.1)", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream({ status: "cancelled" }));
      yield* store.workGraph.putEpic(epic({ status: "cancelled" }));
      yield* store.workGraph.putIssue(issue(1));
      yield* seedRun(store, job(), execution());

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;

      // The cancelled workstream stays cancelled (the roll-up never resurrects it);
      // its running job is settled to `cancelled`, never re-dispatched (no limbo).
      const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream().id));
      expect(ws.status).toBe("cancelled");
      expect(summary.resumed).toStrictEqual([]);
      expect(summary.skipped).toStrictEqual(["job-1"]);
      expect(yield* Queue.size(dispatched)).toBe(0);
      const j1 = Option.getOrThrow(yield* store.jobs.getJob(job().id));
      expect(j1.status).toBe("cancelled");
      // ROOT FIX (CE4.1-R4): the EXECUTION row is settled terminal alongside the Job.
      const s1 = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job().id));
      expect(isExecutionLive(s1)).toBe(false);
    }).pipe(
      Effect.provide(
        testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
      ),
    );
  }),
);

it.effect(
  "does NOT resume a stray non-landed running Job under a truly-done Workstream (CE5-F4)",
  () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<Job>();
      yield* Effect.gen(function* () {
        const store = yield* StateStore;
        // A completed workstream/epic, but a stray `running` Job whose Issue did NOT land
        // (still open on the host, in_progress locally) — the pathological case the
        // narrowed terminal-resume guard must catch: never resurrect it.
        yield* store.repositories.putRepository(repository);
        yield* store.workGraph.putWorkstream(workstream({ status: "done" }));
        yield* store.workGraph.putEpic(epic({ status: "done" }));
        yield* store.workGraph.putIssue(issue(1));
        yield* seedRun(store, job(), execution());

        const startup = yield* StartupReconcile;
        const summary = yield* startup.run;

        // The stray job is settled (cancelled — abandoned, its work never landed), NEVER
        // re-dispatched: nothing resumed, nothing dispatched, no durable `running` limbo.
        expect(summary.resumed).toStrictEqual([]);
        expect(summary.skipped).toStrictEqual(["job-1"]);
        expect(yield* Queue.size(dispatched)).toBe(0);
        const j1 = Option.getOrThrow(yield* store.jobs.getJob(job().id));
        expect(j1.status).toBe("cancelled");
        // ROOT FIX (CE4.1-R4): its EXECUTION row is settled terminal alongside the Job.
        const s1 = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job().id));
        expect(isExecutionLive(s1)).toBe(false);
      }).pipe(
        Effect.provide(
          testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
        ),
      );
    }),
);

// ============================================================================
// resume-failure isolation — one bad dispatch does not abort the startup
// ============================================================================

it.effect("isolates a resume failure so the other in-flight Jobs still resume", () =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream());
      yield* store.workGraph.putEpic(epic({ issues: ["issue-1", "issue-2"] }));
      yield* store.workGraph.putIssue(issue(1));
      yield* store.workGraph.putIssue(issue(2, { id: "issue-2" }));
      // Two running jobs; job-1's dispatch fails, job-2's succeeds.
      yield* seedRun(store, job(), execution());
      yield* seedRun(
        store,
        job({ id: "job-2", issueId: "issue-2", executionId: "execution-job-2" }),
        execution({ id: "execution-job-2", jobId: "job-2" }),
      );

      const startup = yield* StartupReconcile;
      const summary = yield* startup.run;

      // BOTH were re-dispatched (forked) — `resumed` reports re-dispatch, not outcome;
      // job-1's background dispatch fails (isolated, logged) while job-2 records.
      expect([...summary.resumed].sort()).toStrictEqual(["job-1", "job-2"]);
      const recorded = yield* Queue.take(dispatched);
      expect(recorded.id).toBe("job-2");
    }).pipe(
      Effect.provide(
        testLayer(
          host({
            issues: new Map([
              [1, "open"],
              [2, "open"],
            ]),
          }),
          flakyRunner(dispatched, "job-1"),
        ),
      ),
    );
  }),
);
