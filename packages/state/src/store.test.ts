/**
 * `StateStore` coverage (AE2.1) — the port exercised THROUGH the SQLite adapter
 * against an in-memory database (deterministic, offline, no filesystem). Every
 * test provides {@link layerMemory}, so each runs on a fresh `:memory:` database.
 *
 * The suite proves the three capability groups round-trip the OWNED domain
 * schemas: the work graph (`Workstream ⊃ Epic ⊃ Issue`), the dependency DAG
 * (`Issue.dependsOn`) as real replaceable edges, the `Job` model and the
 * `Issue → Job → session → PR` mapping, and the append-only event feed
 * (append / ordered read / tail-from-offset). It also covers absent-optional
 * elision, missing-node `Option.none`, and upsert idempotency.
 *
 * The tests depend ONLY on the `@sprinter/state` public surface — the port, its
 * schemas, and the adapter layer — never on any SQL/SQLite type (INV-PORT).
 */
import { it } from "@effect/vitest";
import { Array as Arr, Effect, Option, Schema } from "effect";
import { expect } from "vitest";
import { Epic, Issue, Job, Session, Workstream } from "@sprinter/domain";
import {
  AppendEvent,
  layer,
  layerMemory,
  PersistedEvent,
  StateStore,
  StateStoreError,
} from "./index.ts";

// ============================================================================
// Fixtures — decoded through the owned domain schemas (no casts)
// ============================================================================

const decode = <S extends Schema.Top>(schema: S, raw: S["Encoded"]) =>
  Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.orDie);

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
    name: "StateStore",
    status: "pending",
    issues: ["issue-22"],
    ...over,
  });

const issue = (over: Partial<(typeof Issue)["Encoded"]> = {}) =>
  decode(Issue, {
    id: "issue-22",
    epicId: "epic-1",
    number: 22,
    title: "AE2.1 — StateStore port + SQLite adapter",
    status: "in_progress",
    dependsOn: [],
    ...over,
  });

const job = (over: Partial<(typeof Job)["Encoded"]> = {}) =>
  decode(Job, {
    id: "job-1",
    issueId: "issue-22",
    kind: "implement",
    status: "running",
    ...over,
  });

const session = (over: Partial<(typeof Session)["Encoded"]> = {}) =>
  decode(Session, { id: "session-1", jobId: "job-1", status: "active", ...over });

const prRef = { number: 42, url: "https://github.com/callajd/sprinter/pull/42", merged: true };

// ============================================================================
// WorkGraphStore
// ============================================================================

it.effect("round-trips the work graph and lists children", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const ws = yield* workstream();
    const ep = yield* epic();
    const iss = yield* issue({ pr: prRef, status: "done" });

    yield* store.workGraph.putWorkstream(ws);
    yield* store.workGraph.putEpic(ep);
    yield* store.workGraph.putIssue(iss);

    expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(ws.id))).toStrictEqual(ws);
    expect(Option.getOrThrow(yield* store.workGraph.getEpic(ep.id))).toStrictEqual(ep);
    expect(Option.getOrThrow(yield* store.workGraph.getIssue(iss.id))).toStrictEqual(iss);

    expect(yield* store.workGraph.listWorkstreams).toStrictEqual([ws]);
    expect(yield* store.workGraph.listEpics(ws.id)).toStrictEqual([ep]);
    expect(yield* store.workGraph.listIssues(ep.id)).toStrictEqual([iss]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("persists the dependency DAG as ordered edges and replaces them on re-upsert", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const iss = yield* issue({ dependsOn: ["issue-7", "issue-3", "issue-9"] });
    yield* store.workGraph.putIssue(iss);

    const first = Option.getOrThrow(yield* store.workGraph.getIssue(iss.id));
    expect(first.dependsOn).toStrictEqual(["issue-7", "issue-3", "issue-9"]);

    // Re-upsert with a different edge set — the old edges must be fully replaced.
    yield* store.workGraph.putIssue(yield* issue({ dependsOn: ["issue-1"] }));
    const second = Option.getOrThrow(yield* store.workGraph.getIssue(iss.id));
    expect(second.dependsOn).toStrictEqual(["issue-1"]);

    // And down to none.
    yield* store.workGraph.putIssue(yield* issue({ dependsOn: [] }));
    const third = Option.getOrThrow(yield* store.workGraph.getIssue(iss.id));
    expect(third.dependsOn).toStrictEqual([]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("elides absent optional fields on an issue without a PR", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const iss = yield* issue();
    yield* store.workGraph.putIssue(iss);
    const read = Option.getOrThrow(yield* store.workGraph.getIssue(iss.id));
    expect(read).toStrictEqual(iss);
    expect("pr" in read).toBe(false);
  }).pipe(Effect.provide(layerMemory)),
);

// ============================================================================
// JobStore
// ============================================================================

it.effect("round-trips a Job with its full session/transcript/PR mapping", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const jb = yield* job({
      status: "succeeded",
      sessionId: "session-1",
      transcriptRef: "transcript://job-1",
      pr: prRef,
    });
    yield* store.jobs.putJob(jb);

    expect(Option.getOrThrow(yield* store.jobs.getJob(jb.id))).toStrictEqual(jb);
    expect(yield* store.jobs.listJobsForIssue(jb.issueId)).toStrictEqual([jb]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("elides a Job's absent optional links", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const jb = yield* job();
    yield* store.jobs.putJob(jb);
    const read = Option.getOrThrow(yield* store.jobs.getJob(jb.id));
    expect(read).toStrictEqual(jb);
    expect("sessionId" in read).toBe(false);
    expect("transcriptRef" in read).toBe(false);
    expect("pr" in read).toBe(false);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("maps a session to and from its job", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const se = yield* session();
    yield* store.jobs.putSession(se);
    expect(Option.getOrThrow(yield* store.jobs.getSession(se.id))).toStrictEqual(se);
    expect(Option.getOrThrow(yield* store.jobs.getSessionForJob(se.jobId))).toStrictEqual(se);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("enforces 1 Job = 1 session, surfacing the backing failure as StateStoreError", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const first = yield* session({ id: "session-1", jobId: "job-1" });
    yield* store.jobs.putSession(first);
    // A second, DISTINCT session for the same job violates the 1-session-per-job
    // invariant. This drives a real backing (UNIQUE-constraint) failure — the one
    // place a SQL error is produced — and asserts INV-PORT: it crosses the port as
    // the owned StateStoreError (operation named), never as a SQL/SQLite type.
    const error = yield* store.jobs
      .putSession(yield* session({ id: "session-2", jobId: "job-1" }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putSession");
    expect(error.detail.length).toBeGreaterThan(0);
    // The first session remains the deterministic answer for the job.
    expect(Option.getOrThrow(yield* store.jobs.getSessionForJob(first.jobId)).id).toBe("session-1");
  }).pipe(Effect.provide(layerMemory)),
);

// ============================================================================
// EventLogStore
// ============================================================================

it.effect("appends events, reads the feed in order, and tails from an offset", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const payloads = [
      { kind: "IssueReady", payload: { issueId: "issue-22" } },
      { kind: "JobDispatched", payload: { jobId: "job-1" } },
      { kind: "PrOpened", payload: 42 },
    ];
    const decoded = yield* Effect.forEach(payloads, (p) => decode(AppendEvent, p));

    const appended = yield* Effect.forEach(decoded, (e) => store.events.append(e));
    const offsets = appended.map((e) => e.offset);
    // Offsets are strictly monotonic and unique in append order.
    expect(offsets).toStrictEqual([...offsets].sort((a, b) => a - b));
    expect(new Set(offsets).size).toBe(offsets.length);

    const feed = yield* store.events.read;
    expect(feed.map((e) => ({ kind: e.kind, payload: e.payload }))).toStrictEqual(payloads);
    expect(feed.map((e) => e.offset)).toStrictEqual(offsets);

    const first = Option.getOrThrow(Arr.head(appended));
    // Tail strictly after the first entry's offset → the remaining two.
    const tail = yield* store.events.tail(first.offset);
    expect(tail.map((e) => e.kind)).toStrictEqual(["JobDispatched", "PrOpened"]);

    // The append return value is itself a well-formed PersistedEvent.
    const roundTrip = yield* Schema.decodeUnknownEffect(PersistedEvent)(first).pipe(Effect.orDie);
    expect(roundTrip).toStrictEqual(first);
  }).pipe(Effect.provide(layerMemory)),
);

// ============================================================================
// Missing nodes & idempotency
// ============================================================================

it.effect("returns None for every missing node", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const ws = yield* workstream();
    const ep = yield* epic();
    const iss = yield* issue();
    const jb = yield* job();
    const se = yield* session();
    expect(yield* store.workGraph.getWorkstream(ws.id)).toStrictEqual(Option.none());
    expect(yield* store.workGraph.getEpic(ep.id)).toStrictEqual(Option.none());
    expect(yield* store.workGraph.getIssue(iss.id)).toStrictEqual(Option.none());
    expect(yield* store.jobs.getJob(jb.id)).toStrictEqual(Option.none());
    expect(yield* store.jobs.getSession(se.id)).toStrictEqual(Option.none());
    expect(yield* store.jobs.getSessionForJob(jb.id)).toStrictEqual(Option.none());
    expect(yield* store.workGraph.listWorkstreams).toStrictEqual([]);
    expect(yield* store.workGraph.listEpics(ws.id)).toStrictEqual([]);
    expect(yield* store.workGraph.listIssues(ep.id)).toStrictEqual([]);
    expect(yield* store.jobs.listJobsForIssue(iss.id)).toStrictEqual([]);
    expect(yield* store.events.read).toStrictEqual([]);
    expect(yield* store.events.tail(0)).toStrictEqual([]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("upserts idempotently — a re-put updates in place", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.workGraph.putWorkstream(yield* workstream({ name: "Original" }));
    yield* store.workGraph.putWorkstream(yield* workstream({ name: "Renamed", status: "done" }));

    const all = yield* store.workGraph.listWorkstreams;
    expect(all).toHaveLength(1);
    expect(all[0]?.name).toBe("Renamed");
    expect(all[0]?.status).toBe("done");
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("the file-backed adapter constructor is usable (layer factory)", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const ws = yield* workstream();
    yield* store.workGraph.putWorkstream(ws);
    expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(ws.id))).toStrictEqual(ws);
    // Exercise the general `layer(config)` path (not just `layerMemory`).
  }).pipe(Effect.provide(layer({ filename: ":memory:", disableWAL: true }))),
);
