/**
 * `SprinterRpc` handler coverage (AE4.1) — the real query / events / command
 * handlers exercised end-to-end through an in-memory RPC client (`RpcTest`)
 * against FAKES: the in-memory `StateStore` (`layerMemory`) behind the publishing
 * decorator, the real `WorkGraphEvents` `PubSub`, and a fake `JobRunner` that
 * records the jobs it is asked to dispatch. Deterministic and offline — no `pi`
 * process, no SQLite file (INV-PORT). The frozen contract is untouched, so its own
 * tests stay green (INV-CONTRACT).
 */
import { it } from "@effect/vitest";
import { Context, Effect, Fiber, Layer, Option, PubSub, Queue, Schema, Stream } from "effect";
import type { Scope } from "effect/Scope";
import { RpcTest } from "effect/unstable/rpc";
import { expect } from "vitest";
import {
  IssueNotFound,
  PlanRejected,
  SessionNotFound,
  SprinterRpc,
  WorkstreamNotFound,
} from "@sprinter/contract";
import {
  Epic,
  Issue,
  Job,
  type JobResult,
  Session,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { layerMemory, StateStore, type StateStoreError } from "@sprinter/state";
import { handlers } from "./rpc-handlers.ts";
import { layerPublishing } from "./store-publishing.ts";
import { layer as layerWorkGraphEvents, WorkGraphEvents } from "./work-graph-events.ts";

// ── fixtures (owned domain values, decoded — no casts) ────────────────────────

const workstream = Schema.decodeUnknownSync(Workstream)({
  id: "ws-1",
  name: "Foundation",
  repo: "callajd/sprinter",
  status: "pending",
  epics: ["ep-1"],
});
const epic = Schema.decodeUnknownSync(Epic)({
  id: "ep-1",
  workstreamId: "ws-1",
  name: "AE4",
  status: "pending",
  issues: ["iss-1"],
});
const issue = Schema.decodeUnknownSync(Issue)({
  id: "iss-1",
  epicId: "ep-1",
  number: 28,
  title: "RpcServer handlers",
  status: "ready",
  dependsOn: [],
});
const queuedJob = Schema.decodeUnknownSync(Job)({
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "queued",
});
const session = Schema.decodeUnknownSync(Session)({
  id: "ses-1",
  jobId: "job-1",
  status: "completed",
});

const succeededResult: JobResult = { status: "succeeded" };

// ── harness: RpcTest client over the handlers + fakes ─────────────────────────

const clientEffect = () => RpcTest.makeClient(SprinterRpc);
type Client = Effect.Success<ReturnType<typeof clientEffect>>;

interface Ctx {
  readonly client: Client;
  readonly store: Context.Service.Shape<typeof StateStore>;
  readonly feed: Context.Service.Shape<typeof WorkGraphEvents>;
  readonly dispatched: Queue.Dequeue<Job>;
}

const harness = <A, E>(
  body: (ctx: Ctx) => Effect.Effect<A, E, Scope>,
): Effect.Effect<A, E | StateStoreError> =>
  Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<Job>();
    const runner = Layer.succeed(
      JobRunner,
      JobRunner.of({
        dispatch: (job) => Queue.offer(dispatched, job).pipe(Effect.as(succeededResult)),
      }),
    );
    const app = handlers.pipe(
      Layer.provideMerge(
        Layer.mergeAll(layerPublishing(layerMemory), runner).pipe(
          Layer.provideMerge(layerWorkGraphEvents),
        ),
      ),
    );
    return yield* Effect.gen(function* () {
      const client = yield* clientEffect();
      const store = yield* StateStore;
      const feed = yield* WorkGraphEvents;
      return yield* body({ client, store, feed, dispatched });
    }).pipe(Effect.provide(app));
  }).pipe(Effect.scoped);

const seedGraph = (store: StateStore["Service"]) =>
  Effect.gen(function* () {
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);
    yield* store.workGraph.putIssue(issue);
  });

// ── snapshot ──────────────────────────────────────────────────────────────────

it.effect("snapshot hydrates the full persisted work graph", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* store.jobs.putJob(queuedJob);
      yield* store.jobs.putSession(session);

      const snapshot = yield* client.snapshot();
      expect(snapshot.workstreams).toEqual([workstream]);
      expect(snapshot.epics).toEqual([epic]);
      expect(snapshot.issues).toEqual([issue]);
      expect(snapshot.jobs).toEqual([queuedJob]);
      expect(snapshot.sessions).toEqual([session]);
    }),
  ),
);

it.effect("snapshot of an empty daemon is empty", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const snapshot = yield* client.snapshot();
      expect(snapshot).toEqual({
        workstreams: [],
        epics: [],
        issues: [],
        jobs: [],
        sessions: [],
      });
    }),
  ),
);

// ── createWorkstreamFromPlan ──────────────────────────────────────────────────

it.effect("createWorkstreamFromPlan materializes and persists a new workstream", () =>
  harness(({ client, store, feed }) =>
    Effect.gen(function* () {
      const subscription = yield* feed.subscribe;
      const id = yield* client.createWorkstreamFromPlan({
        plan: { name: "Payments Revamp", repo: "callajd/sprinter", spec: "ship it" },
      });
      expect(id).toBe("ws-payments-revamp");

      const persisted = Option.getOrThrow(yield* store.workGraph.getWorkstream(id));
      expect(persisted.name).toBe("Payments Revamp");
      expect(persisted.status).toBe("pending");
      expect(persisted.epics).toEqual([]);

      const delta = yield* PubSub.take(subscription);
      expect(delta._tag).toBe("WorkstreamChanged");
    }),
  ),
);

it.effect("createWorkstreamFromPlan rejects a blank spec with PlanRejected", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const error = yield* client
        .createWorkstreamFromPlan({
          plan: { name: "Empty", repo: "callajd/sprinter", spec: "   " },
        })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(PlanRejected);
      expect(error.reason).toBe("empty spec");
    }),
  ),
);

it.effect("createWorkstreamFromPlan rejects a name with no derivable id", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const error = yield* client
        .createWorkstreamFromPlan({
          plan: { name: "!@#$", repo: "callajd/sprinter", spec: "real spec" },
        })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(PlanRejected);
      expect(error.reason).toBe("cannot derive a workstream id from the plan name");
    }),
  ),
);

// ── control ─────────────────────────────────────────────────────────────────

it.effect("control start activates the workstream and dispatches its queued jobs", () =>
  harness(({ client, store, dispatched }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* store.jobs.putJob(queuedJob);

      yield* client.control({ workstreamId: workstream.id, action: "start" });

      const dispatchedJob = yield* Queue.take(dispatched);
      expect(dispatchedJob.id).toBe("job-1");

      const persisted = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
      expect(persisted.status).toBe("active");
    }),
  ),
);

it.effect("control pause blocks the workstream without dispatching", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* client.control({ workstreamId: workstream.id, action: "pause" });
      const persisted = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
      expect(persisted.status).toBe("blocked");
    }),
  ),
);

it.effect("control cancel moves the workstream to its terminal status", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* client.control({ workstreamId: workstream.id, action: "resume" });
      expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id)).status).toBe(
        "active",
      );

      yield* client.control({ workstreamId: workstream.id, action: "cancel" });
      expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id)).status).toBe(
        "done",
      );
    }),
  ),
);

it.effect("control fails with WorkstreamNotFound for an unknown workstream", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const unknown = Schema.decodeUnknownSync(WorkstreamId)("ws-missing");
      const error = yield* client
        .control({ workstreamId: unknown, action: "start" })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(WorkstreamNotFound);
      expect(error.id).toBe("ws-missing");
    }),
  ),
);

// ── retryIssue ────────────────────────────────────────────────────────────────

it.effect("retryIssue re-dispatches the issue's existing job, reusing its session id", () =>
  harness(({ client, store, dispatched }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      const priorJob = Schema.decodeUnknownSync(Job)({
        id: "job-1",
        issueId: "iss-1",
        kind: "implement",
        status: "failed",
        sessionId: "ses-1",
      });
      yield* store.jobs.putJob(priorJob);

      yield* client.retryIssue({ issueId: issue.id });

      const dispatchedJob = yield* Queue.take(dispatched);
      expect(dispatchedJob.id).toBe("job-1");
      // Reuses the SAME session id — the JobRunner re-attaches (1 Job = 1 session).
      expect(dispatchedJob.sessionId).toBe("ses-1");
    }),
  ),
);

it.effect("retryIssue mints a fresh implement job when the issue has none", () =>
  harness(({ client, store, dispatched }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* client.retryIssue({ issueId: issue.id });

      const dispatchedJob = yield* Queue.take(dispatched);
      expect(dispatchedJob.id).toBe("job-iss-1");
      expect(dispatchedJob.kind).toBe("implement");
      expect(dispatchedJob.status).toBe("queued");
    }),
  ),
);

it.effect("retryIssue fails with IssueNotFound for an unknown issue", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const error = yield* client.retryIssue({ issueId: issue.id }).pipe(Effect.flip);
      expect(error).toBeInstanceOf(IssueNotFound);
      expect(error.id).toBe("iss-1");
    }),
  ),
);

// ── events (streaming, INV-REACTIVE) ─────────────────────────────────────────

it.effect("events streams a work-graph delta produced by a command", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const collector = yield* client
        .events()
        .pipe(Stream.take(1), Stream.runHead, Effect.forkChild);
      // Drive the command until the (lazily-subscribing) events stream attaches.
      yield* client
        .createWorkstreamFromPlan({
          plan: { name: "Reactive", repo: "callajd/sprinter", spec: "stream me" },
        })
        .pipe(Effect.andThen(Effect.yieldNow), Effect.forever, Effect.forkChild);

      const delta = Option.getOrThrow(yield* Fiber.join(collector));
      expect(delta._tag).toBe("WorkstreamChanged");
      if (delta._tag !== "WorkstreamChanged") throw new Error("expected WorkstreamChanged");
      expect(delta.workstream.id).toBe("ws-reactive");
    }),
  ),
);

// ── session channel (AE4.2 placeholders) ─────────────────────────────────────

it.effect("session-channel procedures answer with SessionNotFound (AE4.2 placeholder)", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const sessionId = Schema.decodeUnknownSync(Session)({
        id: "ses-x",
        jobId: "job-1",
        status: "starting",
      }).id;

      const sendError = yield* client
        .sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } })
        .pipe(Effect.flip);
      expect(sendError).toBeInstanceOf(SessionNotFound);

      const interruptError = yield* client.interrupt({ sessionId }).pipe(Effect.flip);
      expect(interruptError).toBeInstanceOf(SessionNotFound);

      const answerError = yield* client
        .answerUiRequest({
          sessionId,
          response: { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } },
        })
        .pipe(Effect.flip);
      expect(answerError).toBeInstanceOf(SessionNotFound);

      const streamError = yield* client
        .sessionEvents({ sessionId })
        .pipe(Stream.runHead, Effect.flip);
      expect(streamError).toBeInstanceOf(SessionNotFound);
    }),
  ),
);
