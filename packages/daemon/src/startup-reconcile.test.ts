/**
 * `StartupReconcile` coverage (AE5.1) — the restart-safety service exercised
 * against the in-memory {@link StateStore} (`layerMemory`), a FAKE {@link Repository}
 * (a canned host, no HTTP), and a FAKE {@link JobRunner} (records dispatches, no
 * `pi` process). Deterministic and OFFLINE (INV-GATE / INV-PORT): the service
 * depends only on the three ports.
 *
 * The suite proves the AE5.1 Done criteria:
 *
 * - **reconcile roll-up with one host error isolated** — a single Issue's host
 *   failure (a 404/403/429) does not abort the whole roll-up; the remaining Issues
 *   still land (carried AE3.2 / #27 F4);
 * - **running-Job resume onto the SAME session id** — a Job persisted `running` is
 *   re-dispatched carrying its persisted `sessionId`, never a new one;
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
import { Effect, Layer, Option, Schema } from "effect";
import { expect } from "vitest";
import {
  Epic,
  Issue,
  Job,
  type JobResult,
  PositiveInt,
  PullRequestRef,
  Session,
  Workstream,
} from "@sprinter/domain";
import { ExecutionRunnerError, JobRunner } from "@sprinter/job";
import { Repository, RepositoryError, RepositoryIssue } from "@sprinter/repository";
import { layerMemory, StateStore } from "@sprinter/state";
import { layer as startupLayer, StartupReconcile } from "./startup-reconcile.ts";

// ============================================================================
// Fixtures — decoded through the owned schemas (no casts, INV-NOCAST)
// ============================================================================

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

const posInt = (n: number): PositiveInt => decode(PositiveInt, n);

const workstream = (over: Partial<(typeof Workstream)["Encoded"]> = {}) =>
  decode(Workstream, {
    id: "ws-a",
    name: "Track A",
    repo: "callajd/sprinter",
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
    sessionId: "session-job-1",
    ...over,
  });

const session = (over: Partial<(typeof Session)["Encoded"]> = {}) =>
  decode(Session, { id: "session-job-1", jobId: "job-1", status: "active", ...over });

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

const fakeRepository = (state: HostState): Layer.Layer<Repository> =>
  Layer.succeed(
    Repository,
    Repository.of({
      code: {
        defaultBranch: Effect.succeed("main"),
        branchExists: () => Effect.succeed(true),
      },
      issues: {
        getIssue: (number) =>
          state.failing.has(number)
            ? Effect.fail(
                new RepositoryError({ operation: "getIssue", detail: `host 404 #${number}` }),
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

/** A recording {@link JobRunner}: appends every dispatched {@link Job} to `log`, then succeeds. */
const recordingRunner = (log: Array<Job>): Layer.Layer<JobRunner> =>
  Layer.succeed(
    JobRunner,
    JobRunner.of({
      dispatch: (dispatchedJob) =>
        Effect.sync(() => {
          log.push(dispatchedJob);
        }).pipe(Effect.as(okResult)),
    }),
  );

/** A {@link JobRunner} whose `dispatch` fails for one job id, else records + succeeds. */
const flakyRunner = (log: Array<Job>, failFor: string): Layer.Layer<JobRunner> =>
  Layer.succeed(
    JobRunner,
    JobRunner.of({
      dispatch: (dispatchedJob) =>
        dispatchedJob.id === failFor
          ? Effect.fail(new ExecutionRunnerError({ operation: "run", detail: "spawn refused" }))
          : Effect.sync(() => {
              log.push(dispatchedJob);
            }).pipe(Effect.as(okResult)),
    }),
  );

/**
 * Compose the service-under-test over one SHARED base (`layerMemory` + fakes) via
 * `provideMerge`, so the `StateStore` the test seeds is the exact instance
 * `StartupReconcile` reads (a plain `Layer.provide` would build a second store).
 */
const testLayer = (state: HostState, runner: Layer.Layer<JobRunner>) =>
  startupLayer.pipe(Layer.provideMerge(Layer.mergeAll(layerMemory, fakeRepository(state), runner)));

// ============================================================================
// reconcile roll-up with one host error isolated (F4)
// ============================================================================

it.effect("isolates one Issue's host error and still lands the rest of the roll-up", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
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
});

// ============================================================================
// running-Job resume onto the SAME persisted session id
// ============================================================================

it.effect("resumes a running Job onto its persisted session id, never a new one", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream());
    yield* store.workGraph.putEpic(epic());
    yield* store.workGraph.putIssue(issue(1));
    yield* store.jobs.putSession(session());
    yield* store.jobs.putJob(job());

    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;

    // The in-flight job was re-dispatched, carrying its PERSISTED session id.
    expect(dispatched.map((dj) => dj.id)).toStrictEqual(["job-1"]);
    expect(dispatched[0]?.sessionId).toBe("session-job-1");
    expect(summary.resumed).toStrictEqual(["job-1"]);
    expect(summary.skipped).toStrictEqual([]);
  }).pipe(
    Effect.provide(
      testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
    ),
  );
});

// ============================================================================
// no double-run — terminal Job, and a Job whose Issue reconciled to landed
// ============================================================================

it.effect("does not re-dispatch an already-terminal Job", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream());
    yield* store.workGraph.putEpic(epic());
    yield* store.workGraph.putIssue(issue(1));
    yield* store.jobs.putSession(session({ status: "completed" }));
    yield* store.jobs.putJob(job({ status: "succeeded" }));

    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;

    // A terminal job is not a resume candidate at all.
    expect(dispatched).toStrictEqual([]);
    expect(summary.resumed).toStrictEqual([]);
    expect(summary.skipped).toStrictEqual([]);
  }).pipe(
    Effect.provide(
      testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
    ),
  );
});

it.effect("does not re-run a running Job whose Issue reconciled to landed", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream());
    yield* store.workGraph.putEpic(epic());
    yield* store.workGraph.putIssue(issue(1));
    yield* store.jobs.putSession(session());
    yield* store.jobs.putJob(job());

    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;

    // Reconcile lands issue-1 (closed + merged), so its running job is held back.
    const i1 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(1).id));
    expect(i1.status).toBe("done");
    expect(dispatched).toStrictEqual([]);
    expect(summary.resumed).toStrictEqual([]);
    expect(summary.skipped).toStrictEqual(["job-1"]);
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
});

// ============================================================================
// control state — a paused/cancelled (blocked) or done Workstream is skipped
// ============================================================================

it.effect("does not re-dispatch Jobs of a blocked Workstream", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream({ status: "blocked" }));
    yield* store.workGraph.putEpic(epic({ status: "blocked" }));
    yield* store.workGraph.putIssue(issue(1));
    yield* store.jobs.putSession(session());
    yield* store.jobs.putJob(job());

    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;

    // The blocked workstream stays blocked; its running job is not resumed.
    const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream().id));
    expect(ws.status).toBe("blocked");
    expect(dispatched).toStrictEqual([]);
    expect(summary.resumed).toStrictEqual([]);
    expect(summary.skipped).toStrictEqual(["job-1"]);
  }).pipe(
    Effect.provide(
      testLayer(host({ issues: new Map([[1, "open"]]) }), recordingRunner(dispatched)),
    ),
  );
});

// ============================================================================
// resume-failure isolation — one bad dispatch does not abort the startup
// ============================================================================

it.effect("isolates a resume failure so the other in-flight Jobs still resume", () => {
  const dispatched: Array<Job> = [];
  return Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream());
    yield* store.workGraph.putEpic(epic({ issues: ["issue-1", "issue-2"] }));
    yield* store.workGraph.putIssue(issue(1));
    yield* store.workGraph.putIssue(issue(2, { id: "issue-2" }));
    // Two running jobs; job-1's dispatch fails, job-2's succeeds.
    yield* store.jobs.putSession(session());
    yield* store.jobs.putJob(job());
    yield* store.jobs.putSession(session({ id: "session-job-2", jobId: "job-2" }));
    yield* store.jobs.putJob(job({ id: "job-2", issueId: "issue-2", sessionId: "session-job-2" }));

    const startup = yield* StartupReconcile;
    const summary = yield* startup.run;

    // The failing job did not abort the startup; job-2 still resumed.
    expect(dispatched.map((dj) => dj.id)).toStrictEqual(["job-2"]);
    expect([...summary.resumed].sort()).toStrictEqual(["job-1", "job-2"]);
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
});
