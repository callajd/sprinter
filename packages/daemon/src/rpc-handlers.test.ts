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
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  PubSub,
  Queue,
  Ref,
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
  ResyncRequired,
  SessionNotFound,
  SprinterRpc,
  WorkstreamNotFound,
} from "@sprinter/contract";
import {
  Agent,
  Epic,
  Issue,
  Job,
  type JobResult,
  Session,
  type SessionEvent,
  type SessionId,
  type SessionInput,
  StoreGenerationId,
  type UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import { JobRunner } from "@sprinter/job";
import { PiTransportError, type SessionHandle } from "@sprinter/runner";
import { layerMemory, StateStore, StateStoreError } from "@sprinter/state";
import { layerJournaling } from "./event-journal.ts";
import { handlers } from "./rpc-handlers.ts";
import { layer as layerSessionEvents, SessionEvents } from "./session-events.ts";
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
// The lineage HEAD the retirement below supersedes. It has to be stored first and it
// has to carry the same content: `supersedes` is a referential link in the store (a
// revision may only name an already-stored predecessor) and a retirement is
// lifecycle-only (it repeats the retired revision's content verbatim).
const agentHead = Schema.decodeUnknownSync(Agent)({
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.1.0",
  tools: ["read", "edit"],
});
// A RETIRING registry revision: a new id carrying BOTH `supersedes` (the head it
// retires) and the `retiredAt` stamp — the only shape a retirement takes.
const agent = Schema.decodeUnknownSync(Agent)({
  id: "agt-2",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.1.0",
  tools: ["read", "edit"],
  supersedes: "agt-1",
  retiredAt: "2026-07-20T12:00:00.000Z",
});
// A durable RUNNING Job for `job-1` — genuinely mid-dispatch. The session-channel
// gate keys on the JOB status (CE4.1 FIX A), so a running Job routes to the bounded
// wait that bridges the register-after-dispatch window.
const midDispatchJob = Schema.decodeUnknownSync(Job)({
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "running",
});
// A CRASH-ORPHANED Job: `startup-reconcile` settled a stale `running` Job it could not
// resume by writing ONLY the Job row (terminal `cancelled`), and NEVER settled the
// Session row — so its Session stays NON-TERMINAL. The Job-status gate must fail this
// FAST despite the live Session row (the exact regression a Session-row gate missed).
const orphanedJob = Schema.decodeUnknownSync(Job)({
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "cancelled",
});

const succeededResult: JobResult = { status: "succeeded" };

// ── harness: RpcTest client over the handlers + fakes ─────────────────────────

const clientEffect = () => RpcTest.makeClient(SprinterRpc);
type Client = Effect.Success<ReturnType<typeof clientEffect>>;

interface Ctx {
  readonly client: Client;
  readonly store: Context.Service.Shape<typeof StateStore>;
  readonly feed: Context.Service.Shape<typeof WorkGraphEvents>;
  readonly sessionFeed: Context.Service.Shape<typeof SessionEvents>;
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
          Layer.provideMerge(layerSessionEvents),
        ),
      ),
    );
    return yield* Effect.gen(function* () {
      const client = yield* clientEffect();
      const store = yield* StateStore;
      const feed = yield* WorkGraphEvents;
      const sessionFeed = yield* SessionEvents;
      const sessions = yield* SessionRegistry;
      return yield* body({ client, store, feed, sessionFeed, sessions, dispatched });
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
      yield* store.agents.putAgent(agentHead);
      yield* store.agents.putAgent(agent);

      const snapshot = yield* client.snapshot();
      expect(snapshot.workstreams).toEqual([workstream]);
      expect(snapshot.epics).toEqual([epic]);
      expect(snapshot.issues).toEqual([issue]);
      expect(snapshot.jobs).toEqual([queuedJob]);
      expect(snapshot.sessions).toEqual([session]);
      // The REGISTRY layer hydrates too, whole and flat (never a per-repo slice),
      // and a persisted revision round-trips through the wire schema with both of
      // its optional keys intact — a retired, superseding revision is exactly what
      // a client must be able to resolve a historical execution against.
      expect(snapshot.agents).toEqual([agentHead, agent]);
    }),
  ),
);

it.effect("snapshot of an empty daemon is empty", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      const snapshot = yield* client.snapshot();
      expect(snapshot).toEqual({
        workstreams: [],
        epics: [],
        issues: [],
        jobs: [],
        sessions: [],
        agents: [],
        // Empty of DATA, but never of CONTEXT: the generation is what tells a client
        // which coordinate space the (currently empty) offset log belongs to, so an
        // empty snapshot is still a valid resume context.
        generation: store.generation,
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
        .events({ resume: { sinceOffset: seeded.length, generation: store.generation } })
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
        .events({ resume: { sinceOffset: 2, generation: store.generation } })
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

// DURABLE, transcript-grade session events — the only ones the durable `sessionEvents`
// channel carries: `EntryAppended` records and reconcilable `Notice`s.
const entryAppended: SessionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "AssistantMessage", id: "a1", text: "on it" },
};
const noticeEvent: SessionEvent = { _tag: "Notice", id: "n1", level: "info", message: "started" };
const entryAppended2: SessionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "AssistantMessage", id: "a2", text: "done" },
};
// EPHEMERAL live deltas — the live driving modality the ONE channel must also carry: they
// ride the feed offset-less, are never persisted, and never advance the resume cursor.
const turnStarted: SessionEvent = { _tag: "TurnStarted" };
const messageDelta: SessionEvent = { _tag: "MessageDelta", messageId: "a2", text: "do" };
const uiRequest: SessionEvent = {
  _tag: "UiRequestRaised",
  id: "u1",
  kind: "confirm",
  prompt: "proceed?",
};

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

// A SETTLED session (durable Job + Session rows terminal, NO live handle): the Inspector
// opens its channel to VIEW the transcript. `sessionEvents` replays the whole durable
// transcript as `OffsetSessionEvent`s and COMPLETES — never the old `SessionNotFound` for a
// settled session (the gap this closes) — and a `sinceOffset` cursor resumes strictly after.
it.effect("sessionEvents replays a SETTLED session's durable transcript and completes", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* store.jobs.putSession(session); // `completed` (terminal)
      yield* store.jobs.putJob(orphanedJob); // terminal Job, no live handle
      yield* store.sessionLog.append(sessionId, entryAppended);
      yield* store.sessionLog.append(sessionId, noticeEvent);
      yield* store.sessionLog.append(sessionId, entryAppended2);

      const received = yield* client.sessionEvents({ sessionId }).pipe(Stream.runCollect);
      expect(received.map((i) => i.event)).toEqual([entryAppended, noticeEvent, entryAppended2]);
      // A settled replay is DURABLE-ONLY: every item is offset-BEARING and monotonic (the
      // durable per-session coordinate).
      const offsets = received.flatMap((i) => (i.offset === undefined ? [] : [i.offset]));
      expect(offsets.length).toBe(received.length);
      expect(offsets).toEqual([...offsets].sort((a, b) => a - b));

      // Resume from a mid-transcript cursor: only offsets STRICTLY AFTER it (no re-delivery).
      const cursor = offsets[0] ?? 0;
      const resumed = yield* client
        .sessionEvents({ sessionId, resume: { sinceOffset: cursor, generation: store.generation } })
        .pipe(Stream.runCollect);
      expect(resumed.map((i) => i.event)).toEqual([noticeEvent, entryAppended2]);
      expect(resumed.every((i) => i.offset !== undefined && i.offset > cursor)).toBe(true);
    }),
  ),
);

// The SESSION channel carries the identical stale-generation hazard as the work-graph feed
// — `session_event_log` is `AUTOINCREMENT` too, and a schema-version bump drops it — so it
// gets the identical guard, not a weaker one. These pin BOTH halves of it.
it.effect("sessionEvents REFUSES a cursor from a PRIOR store generation", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* store.jobs.putSession(session);
      yield* store.jobs.putJob(orphanedJob);
      yield* store.sessionLog.append(sessionId, entryAppended);
      yield* store.sessionLog.append(sessionId, noticeEvent);

      // A cursor WITHIN this transcript's extent — an extent check alone would wave it
      // through — but minted under a generation this store never had. Resuming it would
      // tail a transcript the cursor's coordinates never indexed.
      const deadGeneration = Schema.decodeUnknownSync(StoreGenerationId)(
        "00000000-dead-4000-8000-000000000000",
      );
      const failure = yield* client
        .sessionEvents({ sessionId, resume: { sinceOffset: 1, generation: deadGeneration } })
        .pipe(Stream.runCollect, Effect.flip);
      expect(failure).toBeInstanceOf(ResyncRequired);
      if (!(failure instanceof ResyncRequired)) throw new Error("expected ResyncRequired");
      expect(failure.sinceOffset).toBe(1);
      // WITHIN the extent — so this refusal is the identity check, not the extent check.
      expect(failure.sinceOffset).toBeLessThanOrEqual(failure.maxOffset);
      expect(failure.generation).toBe(store.generation);
    }),
  ),
);

it.effect("sessionEvents REFUSES a STALE generation paired with sinceOffset 0 (B1)", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      yield* store.jobs.putSession(session);
      yield* store.jobs.putJob(orphanedJob);
      yield* store.sessionLog.append(sessionId, entryAppended);

      // The session channel's half of the zero-offset door. A zero cursor used to skip the
      // generation comparison outright — it reads as "the origin" numerically — so a
      // request from a DEAD generation was served a resume against a transcript its
      // coordinates never indexed. It is now an ordinary resume: the PRESENCE of the
      // context, never the value of the offset, is what marks it as one.
      const deadGeneration = Schema.decodeUnknownSync(StoreGenerationId)(
        "00000000-dead-4000-8000-000000000000",
      );
      const failure = yield* client
        .sessionEvents({ sessionId, resume: { sinceOffset: 0, generation: deadGeneration } })
        .pipe(Stream.runCollect, Effect.flip);
      expect(failure).toBeInstanceOf(ResyncRequired);
      if (!(failure instanceof ResyncRequired)) throw new Error("expected ResyncRequired");
      expect(failure.sinceOffset).toBe(0);
      expect(failure.generation).toBe(store.generation);

      // A zero cursor under the CURRENT generation is a REAL resume and is honoured —
      // closing the door costs a legitimate client nothing.
      const fromZero = yield* client
        .sessionEvents({ sessionId, resume: { sinceOffset: 0, generation: store.generation } })
        .pipe(Stream.runCollect);
      expect(fromZero.map((i) => i.event)).toEqual([entryAppended]);

      // The ORIGIN request names no coordinate, so it is valid in EVERY generation and is
      // never refused — a first connect (and today's client, which sends no cursor) is
      // untouched by the guard.
      const replayed = yield* client.sessionEvents({ sessionId }).pipe(Stream.runCollect);
      expect(replayed.map((i) => i.event)).toEqual([entryAppended]);
    }),
  ),
);

// A LIVE session (a registered handle → `resolveLive` resolves it): `sessionEvents` replays
// the durable transcript so far, THEN tails new durable entries as the fold journals them —
// replay and live tail one offset coordinate. Uses `it.live` + a settle so the live append
// lands after the subscription (no TestClock: the registered handle resolves without waiting).
it.live("sessionEvents replays a LIVE session's transcript, then tails new durable entries", () =>
  harness(({ client, store, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]);
      // A STILL-RUNNING session: its terminal `result` stays pending, so the live tail keeps
      // tailing new durable entries (it completes only once the session settles).
      const liveHandle: SessionHandle = { ...fake.handle, result: Effect.never };
      yield* sessions.register(sessionId, liveHandle);
      // One durable entry already in the transcript before the client attaches.
      yield* store.sessionLog.append(sessionId, entryAppended);

      const collecting = yield* client
        .sessionEvents({ sessionId })
        .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
      // Let the replay drain and the live subscription settle before the live append.
      yield* Effect.sleep("20 millis");
      // A durable entry appended AFTER attach arrives on the live tail (one coordinate).
      yield* store.sessionLog.append(sessionId, entryAppended2);

      const received = yield* Fiber.join(collecting);
      expect(received.map((i) => i.event)).toEqual([entryAppended, entryAppended2]);
      const [first, second] = received.map((i) => i.offset);
      expect(first !== undefined && second !== undefined && second > first).toBe(true);
    }),
  ),
);

// The live tail must COMPLETE when the session settles (mirroring the old `handle.events`,
// which closed on session end) rather than hang open on the durable feed. The fake handle's
// terminal `result` is already settled, so the live tail interrupts right after replay — the
// stream replays the durable transcript and completes (a `runCollect` that never returns
// would hang the test, so completing at all proves the bound).
it.effect("sessionEvents completes when a live session settles (no hang on the durable feed)", () =>
  harness(({ client, store, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]); // result is already `Completed` → settled
      yield* sessions.register(sessionId, fake.handle);
      yield* store.sessionLog.append(sessionId, entryAppended);

      const received = yield* client.sessionEvents({ sessionId }).pipe(Stream.runCollect);
      expect(received.map((i) => i.event)).toEqual([entryAppended]);
    }),
  ),
);

// The ONE channel serves BOTH modalities: a LIVE driving session must receive its EPHEMERAL
// deltas (turn lifecycle, message partials, `UiRequestRaised`) AND its DURABLE entries,
// interleaved in emission order — ephemerals offset-LESS, durables offset-STAMPED. This is
// the exact regression the correction fixes (the prior pass dropped every ephemeral).
it.live("sessionEvents forwards a LIVE session's ephemeral deltas AND durable entries", () =>
  harness(({ client, store, sessions }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSession([]);
      // A STILL-RUNNING session: `result` stays pending so the live tail keeps forwarding.
      const liveHandle: SessionHandle = { ...fake.handle, result: Effect.never };
      yield* sessions.register(sessionId, liveHandle);

      // Collect the full interleaved flow off the live tail (nothing durable pre-attach).
      const collecting = yield* client
        .sessionEvents({ sessionId })
        .pipe(Stream.take(5), Stream.runCollect, Effect.forkChild);
      yield* Effect.sleep("20 millis"); // let the subscription settle before emitting

      // Tee a mix through the decorated store exactly as the JobRunner fold does: ephemeral
      // deltas via `publishEphemeral` (offset-less), durable entries via `append` (offset).
      yield* store.sessionLog.publishEphemeral(sessionId, turnStarted);
      yield* store.sessionLog.append(sessionId, entryAppended);
      yield* store.sessionLog.publishEphemeral(sessionId, messageDelta);
      yield* store.sessionLog.publishEphemeral(sessionId, uiRequest);
      yield* store.sessionLog.append(sessionId, entryAppended2);

      const received = yield* Fiber.join(collecting);
      // ALL five arrive, in emission order (no ephemeral dropped — the design regression fixed).
      expect(received.map((i) => i.event)).toEqual([
        turnStarted,
        entryAppended,
        messageDelta,
        uiRequest,
        entryAppended2,
      ]);
      // Ephemeral deltas are offset-LESS; durable entries are offset-STAMPED and monotonic.
      const presence = received.map((i) => i.offset !== undefined);
      expect(presence).toEqual([false, true, false, false, true]);
      const durableOffsets = received.flatMap((i) => (i.offset === undefined ? [] : [i.offset]));
      expect(durableOffsets).toEqual([...durableOffsets].sort((a, b) => a - b));

      // Resume IGNORES ephemerals: because they were never persisted, a cursor at the first
      // durable offset replays ONLY the durable suffix (the second entry). The handle is still
      // live, so bound the durable replay with `take(1)` (the one entry past the cursor).
      const cursor = durableOffsets[0] ?? 0;
      const resumed = yield* client
        .sessionEvents({ sessionId, resume: { sinceOffset: cursor, generation: store.generation } })
        .pipe(Stream.take(1), Stream.runCollect);
      expect(resumed.map((i) => i.event)).toEqual([entryAppended2]);
      expect(resumed.every((i) => i.offset !== undefined && i.offset > cursor)).toBe(true);
    }),
  ),
);

// Existence is the Session ROW, not the transcript length. A session that NEVER existed —
// no live handle AND no row — is the ONLY `sessionEvents` miss.
it.effect(
  "sessionEvents fails SessionNotFound only when the session never existed (no row, no handle)",
  () =>
    harness(({ client }) =>
      Effect.gen(function* () {
        const exit = yield* client
          .sessionEvents({ sessionId })
          .pipe(Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);
      }),
    ),
);

// A SETTLED session that emitted ZERO durable events still EXISTS (its Session row is
// terminal): `sessionEvents` replays an EMPTY transcript and COMPLETES — NOT the old
// `SessionNotFound`, which would make the Inspector error on a legitimate empty session.
it.effect(
  "sessionEvents completes with an empty transcript for a settled session that emitted nothing",
  () =>
    harness(({ client, store }) =>
      Effect.gen(function* () {
        yield* store.jobs.putSession(session); // terminal row, NO durable entries, no live handle
        yield* store.jobs.putJob(orphanedJob);

        const received = yield* client.sessionEvents({ sessionId }).pipe(Stream.runCollect);
        expect(received.length).toBe(0);
      }),
    ),
);

// The subscribe→read overlap must NOT double-deliver a durable event: an entry whose offset is
// already covered by the replay (≤ the replay high-water) and is ALSO fanned out on the live
// feed is DROPPED by the live tail — the id-keyed projection cannot dedup an id-LESS `Notice`,
// so the daemon must not emit it twice. A genuinely-newer entry (offset above the high-water)
// still passes, proving the tail keeps running.
it.live(
  "sessionEvents does not re-deliver a durable event already covered by replay (no id-less Notice dup)",
  () =>
    harness(({ client, store, sessions, sessionFeed }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakeSession([]);
        const liveHandle: SessionHandle = { ...fake.handle, result: Effect.never };
        yield* sessions.register(sessionId, liveHandle);
        const idlessNotice: SessionEvent = { _tag: "Notice", level: "info", message: "started" };
        // The id-less notice is in the durable transcript before attach → covered by REPLAY.
        const persisted = yield* store.sessionLog.append(sessionId, idlessNotice);

        const collecting = yield* client
          .sessionEvents({ sessionId })
          .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
        yield* Effect.sleep("20 millis"); // replay drains + subscription settles

        // The overlap: the SAME durable item (offset ≤ high-water) is ALSO on the live feed.
        yield* sessionFeed.publish({ sessionId, offset: persisted.offset, event: idlessNotice });
        // A genuinely-new durable entry (offset > high-water) DOES pass.
        yield* store.sessionLog.append(sessionId, entryAppended2);

        const received = yield* Fiber.join(collecting);
        // Only the replayed notice + the new entry — the re-published duplicate was filtered out.
        expect(received.map((i) => i.event)).toEqual([idlessNotice, entryAppended2]);
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
      const fake = yield* makeFakeSession([]);
      yield* sessions.register(sessionId, fake.handle);

      // The client answers an outstanding UI request, keyed by the request id it echoes…
      const response: UiResponse = {
        requestId: "req-1",
        answer: { _tag: "Confirmed", confirmed: true },
      };
      yield* client.answerUiRequest({ sessionId, response });

      // …and the neutral UiResponse reaches the live session.
      const observed = yield* Queue.take(fake.answered);
      expect(observed).toEqual(response);
    }),
  ),
);

// A durable NON-TERMINAL session row (`starting`) — the register-after-dispatch window
// state: `JobRunner.dispatch` persists this BEFORE `run` registers the handle, so a
// client reacting to the `running` delta arrives with the row already durable but the
// handle not yet registered.
const startingSession = Schema.decodeUnknownSync(Session)({
  id: "ses-1",
  jobId: "job-1",
  status: "starting",
});
// A NON-TERMINAL (`active`) Session row whose Job has since settled — the crash-orphaned
// limbo `startup-reconcile` leaves (Job-only settle). Pairs with {@link orphanedJob}.
const activeSession = Schema.decodeUnknownSync(Session)({
  id: "ses-1",
  jobId: "job-1",
  status: "active",
});

// The register-after-dispatch window: `JobRunner.dispatch` persists the `starting`
// Session + `running` Job BEFORE `run` registers the session handle, so a client
// reacting to the `running` delta can open the session channel before registration.
// Because the durable JOB is `running` (NON-TERMINAL) at that point, the handler's
// durable-state gate routes to `SessionRegistry.resolve`, which bounded-WAITS out that
// window rather than erroring — so the app needs no retry (CE4.1 FIX A: gate on Job).
it.effect(
  "a session-channel handler WAITS out the register-after-dispatch window (mid-dispatch job)",
  () =>
    harness(({ client, store, sessions }) =>
      Effect.gen(function* () {
        // Seed the durable mid-dispatch state: Session row + a RUNNING Job → gate waits.
        yield* store.jobs.putSession(startingSession);
        yield* store.jobs.putJob(midDispatchJob);
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

// FIX A (the regression this closes) — a CRASH-ORPHANED session: `startup-reconcile`
// settled its stale `running` Job to a TERMINAL status (`cancelled`) by writing ONLY
// the Job row, leaving the Session row NON-TERMINAL (`active`). A gate that keyed on the
// SESSION row would see `active` and stall the full 5s bound; the Job-status gate sees
// `cancelled` (terminal) and FAILS FAST. Proven by driving the send INLINE with NO clock
// advance and asserting virtual time is unchanged: had it entered the bounded wait, the
// TestClock would deadlock (no advance ever comes) — completing at all, at time zero,
// proves the fail-fast on the exact BE4.1 path FIX A targets.
it.effect(
  "a CRASH-ORPHANED session fails FAST — no 5s stall (Job terminal, Session row still active)",
  () =>
    harness(({ client, store }) =>
      Effect.gen(function* () {
        // Session row NON-TERMINAL (`active`) but its Job terminal (`cancelled`) — the
        // durable limbo `startup-reconcile` leaves after settling a non-resumable job.
        yield* store.jobs.putSession(activeSession);
        yield* store.jobs.putJob(orphanedJob);

        const before = yield* Clock.currentTimeMillis;
        const error = yield* client
          .sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } })
          .pipe(Effect.flip);
        const after = yield* Clock.currentTimeMillis;

        expect(error).toBeInstanceOf(SessionNotFound);
        // No virtual time consumed → the bounded wait was never entered.
        expect(after).toBe(before);
      }),
    ),
);

// CE4.1-R4 (the queued-orphan this closes) — `startup-reconcile` settled a stale `running`
// Job to `queued` under a paused/`blocked` Workstream. `queued` IS mid-dispatch, so the
// JOB-only gate would bounded-WAIT and stall the full 5s bound. The root fix ALSO settles
// the Session row to a terminal status (`interrupted`), and the gate now fails fast on a
// terminal Session row even while the Job is `queued` — proven by no virtual time consumed.
it.effect(
  "a QUEUED-ORPHAN session fails FAST — no 5s stall (Job queued, Session row settled terminal)",
  () =>
    harness(({ client, store }) =>
      Effect.gen(function* () {
        // Job re-queued for a later `control resume` (mid-dispatch), but its Session row
        // was settled `interrupted` by the reconcile root fix — the airtight gate.
        const queuedOrphanJob = Schema.decodeUnknownSync(Job)({
          id: "job-1",
          issueId: "iss-1",
          kind: "implement",
          status: "queued",
        });
        const interruptedSession = Schema.decodeUnknownSync(Session)({
          id: "ses-1",
          jobId: "job-1",
          status: "interrupted",
        });
        yield* store.jobs.putSession(interruptedSession);
        yield* store.jobs.putJob(queuedOrphanJob);

        const before = yield* Clock.currentTimeMillis;
        const error = yield* client
          .sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } })
          .pipe(Effect.flip);
        const after = yield* Clock.currentTimeMillis;

        expect(error).toBeInstanceOf(SessionNotFound);
        // No virtual time consumed → the bounded wait was never entered despite the
        // `queued` (mid-dispatch) Job — the terminal Session row short-circuits it.
        expect(after).toBe(before);
      }),
    ),
);

// FIX A — a fully SETTLED session (durable Job AND Session row both terminal): the same
// `!isMidDispatchJob → get` branch fails fast, the classic BE4.1 Inspector-on-settled-job
// path. No clock advance; virtual time unchanged.
it.effect("a SETTLED session's channel fails FAST — no 5s stall (Job terminal)", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      // `session` fixture is `completed` (terminal); its Job is terminal too.
      yield* store.jobs.putSession(session);
      yield* store.jobs.putJob(orphanedJob);

      const before = yield* Clock.currentTimeMillis;
      const error = yield* client
        .sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } })
        .pipe(Effect.flip);
      const after = yield* Clock.currentTimeMillis;

      expect(error).toBeInstanceOf(SessionNotFound);
      // No virtual time consumed → the bounded wait was never entered.
      expect(after).toBe(before);
    }),
  ),
);

// FIX A — a NEVER-EXISTED session has NO durable row at all. The gate must route to
// `get` and FAIL FAST across all four procedures, again with no clock advance.
it.effect("session-channel procedures fail FAST for a never-existed session (no durable row)", () =>
  harness(({ client }) =>
    Effect.gen(function* () {
      const missing: SessionId = Schema.decodeUnknownSync(Session)({
        id: "ses-x",
        jobId: "job-1",
        status: "starting",
      }).id;

      // No durable row and nothing registered → each procedure fails immediately (no
      // TestClock advance): the durable-state gate never enters the bounded wait.
      const before = yield* Clock.currentTimeMillis;
      const sendError = yield* client
        .sessionSend({ sessionId: missing, input: { text: "hi", mode: "prompt" } })
        .pipe(Effect.flip);
      const interruptError = yield* client.interrupt({ sessionId: missing }).pipe(Effect.flip);
      const answerError = yield* client
        .answerUiRequest({
          sessionId: missing,
          response: { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } },
        })
        .pipe(Effect.flip);
      const streamError = yield* client
        .sessionEvents({ sessionId: missing })
        .pipe(Stream.runHead, Effect.flip);
      const after = yield* Clock.currentTimeMillis;

      expect(sendError).toBeInstanceOf(SessionNotFound);
      expect(interruptError).toBeInstanceOf(SessionNotFound);
      expect(answerError).toBeInstanceOf(SessionNotFound);
      expect(streamError).toBeInstanceOf(SessionNotFound);
      expect(after).toBe(before);
    }),
  ),
);

// FIX A — a genuinely mid-dispatch session (durable row NON-TERMINAL) whose handle
// NEVER registers: the gate routes to the bounded wait, which must fail with
// `SessionNotFound` AFTER the bound (never hang). Advancing the TestClock past the bound
// fires the parked wait exactly.
it.effect("a mid-dispatch session whose handle never registers fails AFTER the bound", () =>
  harness(({ client, store }) =>
    Effect.gen(function* () {
      // Durable Job is `running` (NON-TERMINAL) → the gate waits; nothing ever registers.
      yield* store.jobs.putSession(startingSession);
      yield* store.jobs.putJob(midDispatchJob);

      const sendFiber = yield* Effect.forkChild(
        client.sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } }).pipe(Effect.flip),
        { startImmediately: true },
      );
      const streamFiber = yield* Effect.forkChild(
        client.sessionEvents({ sessionId }).pipe(Stream.runHead, Effect.flip),
        { startImmediately: true },
      );

      yield* TestClock.adjust(SESSION_RESOLVE_TIMEOUT);

      expect(yield* Fiber.join(sendFiber)).toBeInstanceOf(SessionNotFound);
      expect(yield* Fiber.join(streamFiber)).toBeInstanceOf(SessionNotFound);
    }),
  ),
);

// FIX B (revised #76) — a TRANSIENT `StateStoreError` on the resolve-path durable read
// must be surfaced as a DEFECT, NOT folded into the typed `SessionNotFound` channel.
// Conflating a store hiccup with a genuine registry miss is the #76 bug (a live session
// mis-classified as settled); `resolveLive` now `Effect.die`s the store error. A StateStore
// whose `getSession` fails transiently is substituted for the memory store; the send must
// die with the `StateStoreError` defect and leak NO typed error (INV-CONTRACT: the channel
// stays exactly `SessionNotFound`, so a store failure crosses only as a defect).
it.effect("a transient store read failure surfaces as a DEFECT, never a typed error", () =>
  Effect.gen(function* () {
    const failingStore = Layer.effect(
      StateStore,
      Effect.gen(function* () {
        const base = yield* StateStore;
        return StateStore.of({
          ...base,
          jobs: {
            ...base.jobs,
            getSession: () =>
              Effect.fail(new StateStoreError({ operation: "getSession", detail: "transient" })),
          },
        });
      }),
    ).pipe(Layer.provide(layerJournaling(layerMemory)));
    const runner = Layer.succeed(
      JobRunner,
      JobRunner.of({ dispatch: () => Effect.succeed(succeededResult) }),
    );
    const app = handlers.pipe(
      Layer.provideMerge(
        Layer.mergeAll(failingStore, runner, layerSessionRegistry).pipe(
          Layer.provideMerge(layerWorkGraphEvents),
          Layer.provideMerge(layerSessionEvents),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const client = yield* clientEffect();
      const exit = yield* client
        .sessionSend({ sessionId, input: { text: "hi", mode: "prompt" } })
        .pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      const cause = Exit.isFailure(exit) ? exit.cause : Cause.empty;
      // A DEFECT (the store error died), and NO typed failure leaked (INV-CONTRACT).
      expect(Cause.hasDies(cause)).toBe(true);
      expect(Cause.squash(cause) instanceof StateStoreError).toBe(true);
      expect(Option.isNone(Cause.findErrorOption(cause))).toBe(true);
    }).pipe(Effect.provide(app), Effect.scoped);
  }),
);

// FIX B (revised #76) — the CORE regression: a genuinely LIVE session whose `resolveLive`
// hits a transient `StateStoreError` must NOT be mis-classified as settled and silently
// completed as a truncated durable replay (dropping the live tail). The session's durable
// row is NON-TERMINAL and it has a registered handle + a durable transcript, so before the
// fix `resolveLive`'s store error folded to `SessionNotFound` → `succeedNone` → `live`
// `None` → the stream replayed only the durable prefix and COMPLETED. Now it must DEFECT.
//
// Deterministic + non-hanging: `getSession` fails EXACTLY on its first call (the
// `resolveLive` read) and delegates thereafter, so with the fix the stream dies at once
// (no handle wait), and were the fix reverted the second read would succeed and the stream
// would COMPLETE as a truncated success — which this assertion (a defect, not a success)
// would catch.
it.effect(
  "a transient store error on a LIVE session's resolveLive DEFECTS, not a truncated replay",
  () =>
    Effect.gen(function* () {
      // `armed` trips the FIRST `getSession` (resolveLive's read) into a transient failure,
      // then disarms so the handler's later reads delegate to the real memory store.
      const armed = yield* Ref.make(true);
      const failingStore = Layer.effect(
        StateStore,
        Effect.gen(function* () {
          const base = yield* StateStore;
          return StateStore.of({
            ...base,
            jobs: {
              ...base.jobs,
              getSession: (id) =>
                Ref.getAndSet(armed, false).pipe(
                  Effect.flatMap((wasArmed) =>
                    wasArmed
                      ? Effect.fail(
                          new StateStoreError({ operation: "getSession", detail: "transient" }),
                        )
                      : base.jobs.getSession(id),
                  ),
                ),
            },
          });
        }),
      ).pipe(Layer.provide(layerJournaling(layerMemory)));
      const runner = Layer.succeed(
        JobRunner,
        JobRunner.of({ dispatch: () => Effect.succeed(succeededResult) }),
      );
      const app = handlers.pipe(
        Layer.provideMerge(
          Layer.mergeAll(failingStore, runner, layerSessionRegistry).pipe(
            Layer.provideMerge(layerWorkGraphEvents),
            Layer.provideMerge(layerSessionEvents),
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const store = yield* StateStore;
        const sessions = yield* SessionRegistry;
        // A genuinely LIVE session: NON-TERMINAL row, mid-dispatch Job, a registered handle
        // (result pending so the live tail would keep tailing), and a durable transcript that
        // the buggy path would replay-and-complete.
        const fake = yield* makeFakeSession([]);
        yield* sessions.register(sessionId, { ...fake.handle, result: Effect.never });
        yield* Ref.set(armed, false); // seed with the store DISARMED …
        yield* store.jobs.putSession(startingSession);
        yield* store.jobs.putJob(midDispatchJob);
        yield* store.sessionLog.append(sessionId, entryAppended);
        yield* Ref.set(armed, true); // … then arm for the resolveLive read under test.

        const client = yield* clientEffect();
        const exit = yield* client
          .sessionEvents({ sessionId })
          .pipe(Stream.runCollect, Effect.exit);
        // Must DEFECT (loud), never a truncated-replay success that drops the live tail.
        expect(Exit.isFailure(exit)).toBe(true);
        const cause = Exit.isFailure(exit) ? exit.cause : Cause.empty;
        expect(Cause.hasDies(cause)).toBe(true);
        expect(Cause.squash(cause) instanceof StateStoreError).toBe(true);
        // INV-CONTRACT: the store failure crossed as a defect, not the typed channel.
        expect(Option.isNone(Cause.findErrorOption(cause))).toBe(true);
      }).pipe(Effect.provide(app), Effect.scoped);
    }),
);
