/**
 * Publishing `StateStore` decorator coverage (AE4.1) — the INV-REACTIVE seam.
 * Exercised against the in-memory store (`layerMemory`) + the real
 * `WorkGraphEvents` feed: every `put*` fans out the matching owned
 * {@link WorkGraphEvent} delta, while reads and the durable event log pass
 * straight through unchanged. Deterministic and offline.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, PubSub, Schema } from "effect";
import { expect } from "vitest";
import { Epic, Issue, Job, Session, Workstream } from "@sprinter/domain";
import { AppendEvent, layerMemory, StateStore } from "@sprinter/state";
import { layerPublishing } from "./store-publishing.ts";
import { layer as layerWorkGraphEvents, WorkGraphEvents } from "./work-graph-events.ts";

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
const job = Schema.decodeUnknownSync(Job)({
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "queued",
});
const session = Schema.decodeUnknownSync(Session)({
  id: "ses-1",
  jobId: "job-1",
  status: "starting",
});

const TestLayer = Layer.provideMerge(layerPublishing(layerMemory), layerWorkGraphEvents);

it.effect("publishes an owned delta for every work-graph mutation", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;
    const subscription = yield* feed.subscribe;

    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);
    yield* store.workGraph.putIssue(issue);
    yield* store.jobs.putJob(job);
    yield* store.jobs.putSession(session);

    const tags: Array<string> = [];
    for (let i = 0; i < 5; i++) tags.push((yield* PubSub.take(subscription))._tag);
    expect(tags).toEqual([
      "WorkstreamChanged",
      "EpicChanged",
      "IssueChanged",
      "JobChanged",
      "SessionChanged",
    ]);
  }).pipe(Effect.scoped, Effect.provide(TestLayer)),
);

it.effect("passes reads through to the base store unchanged", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);
    yield* store.workGraph.putIssue(issue);
    yield* store.jobs.putJob(job);
    yield* store.jobs.putSession(session);

    expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id))).toStrictEqual(
      workstream,
    );
    expect(Option.getOrThrow(yield* store.workGraph.getEpic(epic.id))).toStrictEqual(epic);
    expect(Option.getOrThrow(yield* store.workGraph.getIssue(issue.id))).toStrictEqual(issue);
    expect(Option.getOrThrow(yield* store.jobs.getJob(job.id))).toStrictEqual(job);
    expect(Option.getOrThrow(yield* store.jobs.getSession(session.id))).toStrictEqual(session);
    expect(Option.getOrThrow(yield* store.jobs.getSessionForJob(job.id))).toStrictEqual(session);

    expect(yield* store.workGraph.listWorkstreams).toEqual([workstream]);
    expect(yield* store.workGraph.listEpics(workstream.id)).toEqual([epic]);
    expect(yield* store.workGraph.listIssues(epic.id)).toEqual([issue]);
    expect(yield* store.jobs.listJobsForIssue(issue.id)).toEqual([job]);
  }).pipe(Effect.scoped, Effect.provide(TestLayer)),
);

it.effect("passes the durable event log through unchanged", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const event = Schema.decodeUnknownSync(AppendEvent)({
      kind: "IssueStatusChanged",
      payload: { id: "iss-1", status: "ready" },
    });
    const persisted = yield* store.events.append(event);
    expect(persisted.kind).toBe("IssueStatusChanged");

    expect(yield* store.events.read).toEqual([persisted]);
    expect(yield* store.events.tail(persisted.offset - 1)).toEqual([persisted]);
    expect(yield* store.events.tail(persisted.offset)).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(TestLayer)),
);
