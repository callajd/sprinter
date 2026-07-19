/**
 * `SprinterRpc` handler coverage (AE4.1) — the real query / events / command
 * handlers exercised end-to-end through an in-memory RPC client (`RpcTest`)
 * against FAKES: the in-memory `StateStore` (`layerMemory`) behind the journaling
 * decorator (durable offset log + live fan-out), the real `WorkGraphEvents`
 * `PubSub`, and a fake `JobRunner` that
 * records the jobs it is asked to dispatch. Deterministic and offline — no `pi`
 * process, no SQLite file (INV-PORT). The frozen contract is untouched, so its own
 * tests stay green (INV-CONTRACT).
 */
import { it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  PubSub,
  Queue,
  Schema,
  Stream,
} from "effect";
import type { Scope } from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import { RpcTest } from "effect/unstable/rpc";
import { TestClock } from "effect/testing";
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
  type SessionEvent,
  type SessionId,
  type SessionInput,
  type UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { PiTransportError, type SessionHandle } from "@sprinter/runner";
import { layerMemory, StateStore, type StateStoreError } from "@sprinter/state";
import { layerJournaling } from "./event-journal.ts";
import { handlers } from "./rpc-handlers.ts";
import {
  layer as layerSessionRegistry,
  SESSION_RESOLVE_TIMEOUT,
  SessionRegistry,
} from "./session-registry.ts";
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
  readonly sessions: Context.Service.Shape<typeof SessionRegistry>;
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
        Layer.mergeAll(layerJournaling(layerMemory), runner, layerSessionRegistry).pipe(
          Layer.provideMerge(layerWorkGraphEvents),
        ),
      ),
    );
    return yield* Effect.gen(function* () {
      const client = yield* clientEffect();
      const store = yield* StateStore;
      const feed = yield* WorkGraphEvents;
      const sessions = yield* SessionRegistry;
      return yield* body({ client, store, feed, sessions, dispatched });
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
      // The id is derived from BOTH the name and the (repo-scoped, D14) repo.
      expect(id).toBe("ws-payments-revamp-callajd-sprinter");

      const persisted = Option.getOrThrow(yield* store.workGraph.getWorkstream(id));
      expect(persisted.name).toBe("Payments Revamp");
      expect(persisted.status).toBe("pending");
      expect(persisted.epics).toEqual([]);

      const delta = yield* PubSub.take(subscription);
      expect(delta.event._tag).toBe("WorkstreamChanged");
      // The live fan-out carries the durable offset it was journaled at (CE2.0).
      expect(delta.offset).toBeGreaterThan(0);
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

it.effect("createWorkstreamFromPlan rejects a colliding create rather than clobbering", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      const plan = { name: "Payments Revamp", repo: "callajd/sprinter", spec: "v1" } as const;
      const id = yield* client.createWorkstreamFromPlan({ plan });

      // A second create with the same name+repo must NOT upsert-clobber the first
      // (which would reset its name/repo/epics) — it is rejected.
      const error = yield* client
        .createWorkstreamFromPlan({ plan: { ...plan, name: "Payments Revamp", spec: "v2" } })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(PlanRejected);
      expect(error.reason).toBe("a workstream already exists for this plan name and repo");

      // The original workstream is intact.
      const persisted = Option.getOrThrow(yield* store.workGraph.getWorkstream(id));
      expect(persisted.name).toBe("Payments Revamp");
    }),
  ),
);

it.effect("createWorkstreamFromPlan does not collide the same name across different repos", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const a = yield* client.createWorkstreamFromPlan({
        plan: { name: "Revamp", repo: "callajd/one", spec: "s" },
      });
      const b = yield* client.createWorkstreamFromPlan({
        plan: { name: "Revamp", repo: "callajd/two", spec: "s" },
      });
      // Repo-scoped ids (D14): the same name for different repos yields distinct
      // workstreams, not a silent overwrite.
      expect(a).not.toBe(b);
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

it.effect("control cancel moves the workstream to the distinct terminal cancelled status", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      yield* client.control({ workstreamId: workstream.id, action: "resume" });
      expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id)).status).toBe(
        "active",
      );

      // CE5.1: cancel is a distinct terminal status, not collapsed to `done`.
      yield* client.control({ workstreamId: workstream.id, action: "cancel" });
      expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id)).status).toBe(
        "cancelled",
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

it.effect("retryIssue is a no-op when the issue's latest job is still in flight", () =>
  harness(({ client, store, dispatched }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      const runningJob = Schema.decodeUnknownSync(Job)({
        id: "job-1",
        issueId: "iss-1",
        kind: "implement",
        status: "running",
        sessionId: "ses-1",
      });
      yield* store.jobs.putJob(runningJob);

      // Retrying a running job must NOT fork a second dispatch racing the same
      // session rows — nothing is dispatched.
      yield* client.retryIssue({ issueId: issue.id });
      expect(yield* Queue.size(dispatched)).toBe(0);
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
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* seedGraph(store);
      // Subscribe PAST the seeded durable history so `take(1)` observes a LIVE delta,
      // not a replayed one: `events` replays `event_log.tail(sinceOffset)` first.
      const seeded = yield* store.events.read;
      const collector = yield* client
        .events({ sinceOffset: seeded.length })
        .pipe(Stream.take(1), Stream.runHead, Effect.forkChild);
      // Drive a REPEATABLE command until the (lazily-subscribing) events stream
      // attaches: `control start` re-publishes a `WorkstreamChanged` delta on every
      // call (idempotent), unlike `createWorkstreamFromPlan` which now rejects a
      // second create for the same workstream.
      yield* client
        .control({ workstreamId: workstream.id, action: "start" })
        .pipe(Effect.andThen(Effect.yieldNow), Effect.forever, Effect.forkChild);

      const item = Option.getOrThrow(yield* Fiber.join(collector));
      expect(item.event._tag).toBe("WorkstreamChanged");
      if (item.event._tag !== "WorkstreamChanged") throw new Error("expected WorkstreamChanged");
      expect(item.event.workstream.id).toBe("ws-1");
      // The streamed item carries its durable offset, past the seeded cursor.
      expect(item.offset).toBeGreaterThan(seeded.length);
    }),
  ),
);

it.effect("events resumes durable replay after a client-supplied sinceOffset cursor (CE2.0)", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      // Seed durable history: five journaled deltas at offsets 1..5.
      yield* seedGraph(store);
      yield* store.jobs.putJob(queuedJob);
      yield* store.jobs.putSession(session);

      // Subscribe with a cursor PAST the first two entries (offset 2): the served
      // endpoint threads it into `resyncFrom`, so the replay starts STRICTLY AFTER
      // offset 2 — the issue/job/session deltas — never re-sending the earlier ones.
      const replay = yield* client
        .events({ sinceOffset: 2 })
        .pipe(Stream.take(3), Stream.runCollect, Effect.forkChild)
        .pipe(Effect.flatMap(Fiber.join));

      expect(replay.map((item) => item.event._tag)).toEqual([
        "IssueChanged",
        "JobChanged",
        "SessionChanged",
      ]);
      // The response envelope carries the durable offsets: all STRICTLY GREATER than
      // the supplied cursor (2) and contiguous (3, 4, 5) — the coordinate the client
      // feeds back as its next `sinceOffset`, so a resume is gap-free and dup-free.
      expect(replay.map((item) => item.offset)).toEqual([3, 4, 5]);
    }),
  ),
);

// ── session channel (AE4.2 — bridge a live SessionHandle) ─────────────────────

const sessionId = session.id;

const uiRequest: SessionEvent = {
  _tag: "UiRequestRaised",
  id: "req-1",
  kind: "confirm",
  prompt: "Proceed?",
};
const turnStarted: SessionEvent = { _tag: "TurnStarted" };

/**
 * A fake live {@link SessionHandle}: a caller-supplied owned `SessionEvent` stream
 * for `events`, and queues/deferred that RECORD the neutral inputs driven into it
 * so a test can observe the round-trip (input sent, turn interrupted, UI answered).
 * No Pi type appears — the fake speaks only the owned neutral surface.
 */
interface FakeSession {
  readonly handle: SessionHandle;
  readonly sent: Queue.Dequeue<SessionInput>;
  readonly answered: Queue.Dequeue<UiResponse>;
  readonly interrupted: Deferred.Deferred<void>;
}

const makeFakeSession = (events: ReadonlyArray<SessionEvent>): Effect.Effect<FakeSession> =>
  Effect.gen(function* () {
    const sent = yield* Queue.unbounded<SessionInput>();
    const answered = yield* Queue.unbounded<UiResponse>();
    const interrupted = yield* Deferred.make<void>();
    const handle: SessionHandle = {
      pid: ChildProcessSpawner.ProcessId(4242),
      events: Stream.fromIterable(events),
      send: (input) => Queue.offer(sent, input).pipe(Effect.asVoid),
      interrupt: Deferred.succeed(interrupted, undefined).pipe(Effect.asVoid),
      answerUi: (response) => Queue.offer(answered, response).pipe(Effect.asVoid),
      result: Effect.succeed({ _tag: "Completed" }),
    };
    return { handle, sent, answered, interrupted };
  });

it.effect("sessionEvents bridges the live session's owned event stream", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([turnStarted, uiRequest]);
      yield* sessions.register(sessionId, fake.handle);

      const received = yield* client.sessionEvents({ sessionId }).pipe(Stream.runCollect);
      expect(received).toEqual([turnStarted, uiRequest]);
    }),
  ),
);

it.effect("sessionSend drives the input into the live session", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]);
      yield* sessions.register(sessionId, fake.handle);

      const input: SessionInput = { text: "go", mode: "prompt" };
      yield* client.sessionSend({ sessionId, input });

      const driven = yield* Queue.take(fake.sent);
      expect(driven).toEqual(input);
    }),
  ),
);

it.effect("a session infra failure becomes a defect, never a leaked SessionNotFound", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      // A live, REGISTERED session whose `send` fails with an infra error
      // (PiTransportError). The handler `orDie`s it, so it must NOT surface as the
      // typed contract error `SessionNotFound` — the error channel stays exactly
      // SessionNotFound (INV-CONTRACT), and infra failures cross as defects.
      const failing = yield* makeFakeSession([]);
      const handle: SessionHandle = {
        ...failing.handle,
        send: () => Effect.fail(new PiTransportError({ reason: "closed", detail: "gone" })),
      };
      yield* sessions.register(sessionId, handle);

      const exit = yield* client
        .sessionSend({ sessionId, input: { text: "go", mode: "prompt" } })
        .pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      const leaked = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
      // The session IS registered, so any SessionNotFound here would be a leak.
      expect(Option.isSome(leaked) && leaked.value instanceof SessionNotFound).toBe(false);
    }),
  ),
);

it.effect("interrupt aborts the live session's in-flight turn", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]);
      yield* sessions.register(sessionId, fake.handle);

      yield* client.interrupt({ sessionId });
      // The fake resolves this deferred only when its `interrupt` is called.
      yield* Deferred.await(fake.interrupted);
    }),
  ),
);

it.effect("answerUiRequest completes the extension_ui_request round-trip", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([uiRequest]);
      yield* sessions.register(sessionId, fake.handle);

      // A `UiRequestRaised` surfaces on the live sessionEvents feed…
      const raised = Option.getOrThrow(
        yield* client.sessionEvents({ sessionId }).pipe(Stream.runHead),
      );
      expect(raised._tag).toBe("UiRequestRaised");
      if (raised._tag !== "UiRequestRaised") throw new Error("expected UiRequestRaised");

      // …the client answers it, keyed by the raised request id…
      const response: UiResponse = {
        requestId: raised.id,
        answer: { _tag: "Confirmed", confirmed: true },
      };
      yield* client.answerUiRequest({ sessionId, response });

      // …and the neutral UiResponse reaches the live session.
      const observed = yield* Queue.take(fake.answered);
      expect(observed).toEqual(response);
    }),
  ),
);

// The register-after-dispatch window: `JobRunner.dispatch` fans out the `running`
// delta BEFORE `run` registers the session handle, so a client reacting to it can
// open the session channel before registration. The handlers resolve via
// `SessionRegistry.resolve`, which bounded-WAITS out that window rather than erroring
// — so the app needs no retry (CE4.1 server-side fix).
it.effect("a session-channel handler WAITS out the register-after-dispatch window", () =>
  harness(({ client, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]);
      const input: SessionInput = { text: "go", mode: "prompt" };

      // Open the channel BEFORE the session is registered — the handler must park, not
      // fail with SessionNotFound. Registration lands next; the parked send then drives
      // the live session. Event-driven, so no TestClock advance is needed here.
      const fiber = yield* Effect.forkChild(client.sessionSend({ sessionId, input }), {
        startImmediately: true,
      });
      yield* sessions.register(sessionId, fake.handle);
      yield* Fiber.join(fiber);

      const driven = yield* Queue.take(fake.sent);
      expect(driven).toEqual(input);
    }),
  ),
);

it.effect(
  "session-channel procedures fail with SessionNotFound after the bound for an unknown session",
  () =>
    harness(({ client }) =>
      Effect.gen(function* () {
        const missing: SessionId = Schema.decodeUnknownSync(Session)({
          id: "ses-x",
          jobId: "job-1",
          status: "starting",
        }).id;

        // Genuinely absent — no session ever registers. Each procedure parks up to the
        // hard bound and THEN fails with `SessionNotFound` (never hangs). Fork each,
        // advance the TestClock past the bound to fire every parked wait at once, then
        // collect the failures.
        const startImmediately = true;
        const sendFiber = yield* Effect.forkChild(
          client
            .sessionSend({ sessionId: missing, input: { text: "hi", mode: "prompt" } })
            .pipe(Effect.flip),
          { startImmediately },
        );
        const interruptFiber = yield* Effect.forkChild(
          client.interrupt({ sessionId: missing }).pipe(Effect.flip),
          { startImmediately },
        );
        const answerFiber = yield* Effect.forkChild(
          client
            .answerUiRequest({
              sessionId: missing,
              response: { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } },
            })
            .pipe(Effect.flip),
          { startImmediately },
        );
        const streamFiber = yield* Effect.forkChild(
          client.sessionEvents({ sessionId: missing }).pipe(Stream.runHead, Effect.flip),
          { startImmediately },
        );

        yield* TestClock.adjust(SESSION_RESOLVE_TIMEOUT);

        expect(yield* Fiber.join(sendFiber)).toBeInstanceOf(SessionNotFound);
        expect(yield* Fiber.join(interruptFiber)).toBeInstanceOf(SessionNotFound);
        expect(yield* Fiber.join(answerFiber)).toBeInstanceOf(SessionNotFound);
        expect(yield* Fiber.join(streamFiber)).toBeInstanceOf(SessionNotFound);
      }),
    ),
);
