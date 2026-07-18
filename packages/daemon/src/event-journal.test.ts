/**
 * Durable journaling + offset-based resync coverage (CE1.2) — the `./event-journal.ts`
 * seam that makes the `events` feed a deterministic, offset-based catch-up rather
 * than snapshot-on-connect (D17). Exercised against the in-memory `StateStore`
 * (`layerMemory`) behind the journaling + publishing decorators and the real
 * `WorkGraphEvents` `PubSub` — deterministic and offline (INV-PORT).
 */
import { it } from "@effect/vitest";
import { Context, Effect, Fiber, Schema, Stream } from "effect";
import { expect } from "vitest";
import type { WorkGraphEvent } from "@sprinter/contract";
import { Epic, Issue, Job, Session, Workstream } from "@sprinter/domain";
import { layerMemory, StateStore, StateStoreError } from "@sprinter/state";
import { layerJournaling, resyncEvents, resyncFrom } from "./event-journal.ts";
import { layerPublishing } from "./store-publishing.ts";
import { layer as layerWorkGraphEvents, WorkGraphEvents } from "./work-graph-events.ts";

// ── fixtures ──────────────────────────────────────────────────────────────────

const workstream = Schema.decodeUnknownSync(Workstream)({
  id: "ws-1",
  name: "Convergence",
  repo: "callajd/sprinter",
  status: "active",
  epics: ["ep-1"],
});
const epic = Schema.decodeUnknownSync(Epic)({
  id: "ep-1",
  workstreamId: "ws-1",
  name: "CE1",
  status: "active",
  issues: ["iss-1"],
});
const issue = Schema.decodeUnknownSync(Issue)({
  id: "iss-1",
  epicId: "ep-1",
  number: 52,
  title: "Daemon main",
  status: "in_progress",
  dependsOn: [],
});
const job = Schema.decodeUnknownSync(Job)({
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "running",
  sessionId: "ses-1",
});
const session = Schema.decodeUnknownSync(Session)({
  id: "ses-1",
  jobId: "job-1",
  status: "active",
});

// ── harness: publishing over journaling over layerMemory ──────────────────────

const seedGraph = (store: Context.Service.Shape<typeof StateStore>) =>
  Effect.gen(function* () {
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);
    yield* store.workGraph.putIssue(issue);
    yield* store.jobs.putJob(job);
    yield* store.jobs.putSession(session);
  });

// ── tests ─────────────────────────────────────────────────────────────────────

it.effect("journals every persisted mutation to the durable offset log, in order", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* seedGraph(store);

    const entries = yield* store.events.tail(0);
    expect(entries.map((e) => e.kind)).toEqual([
      "WorkstreamChanged",
      "EpicChanged",
      "IssueChanged",
      "JobChanged",
      "SessionChanged",
    ]);
    // Offsets are strictly increasing (the monotonic tail cursor).
    const offsets = entries.map((e) => e.offset);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    expect(new Set(offsets).size).toBe(offsets.length);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerPublishing(layerJournaling(layerMemory))),
    Effect.provide(layerWorkGraphEvents),
  ),
);

it.effect(
  "journals the node write AND its delta ATOMICALLY: a failed transaction rolls back both (INV-RESTART)",
  () =>
    Effect.gen(function* () {
      const store = yield* StateStore;

      // A transaction that writes a node (which itself journals its delta) then
      // fails. The node write and its `event_log` append must be a SINGLE unit: on
      // failure BOTH roll back — never the node-persisted-but-delta-missing split a
      // crash between two separate commits would leave (the offset feed's history
      // would then be incomplete).
      const boom = new StateStoreError({ operation: "test", detail: "boom" });
      const failure = yield* store.workGraph
        .putWorkstream(workstream)
        .pipe(Effect.andThen(Effect.fail(boom)), store.withTransaction, Effect.flip);
      expect(failure).toBe(boom);

      // Nothing committed: neither the node row nor its offset-log delta survive.
      const persisted = yield* store.workGraph.getWorkstream(workstream.id);
      expect(persisted._tag).toBe("None");
      const entries = yield* store.events.tail(0);
      expect(entries).toEqual([]);

      // And a NORMAL put commits both together — the node is readable and its delta
      // is journaled (the two halves the transaction binds).
      yield* store.workGraph.putWorkstream(workstream);
      const committed = yield* store.workGraph.getWorkstream(workstream.id);
      expect(committed._tag).toBe("Some");
      const journaled = yield* store.events.tail(0);
      expect(journaled.map((e) => e.kind)).toEqual(["WorkstreamChanged"]);
    }).pipe(Effect.scoped, Effect.provide(layerJournaling(layerMemory))),
);

it.effect("resyncFrom decodes the durable log back into owned WorkGraphEvents", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* seedGraph(store);

    const replay = yield* resyncFrom(store, 0).pipe(Stream.runCollect);
    expect(replay).toEqual([
      { _tag: "WorkstreamChanged", workstream },
      { _tag: "EpicChanged", epic },
      { _tag: "IssueChanged", issue },
      { _tag: "JobChanged", job },
      { _tag: "SessionChanged", session },
    ]);

    // A cursor past the first two entries resumes from the third (offset-based).
    const fromThird = yield* resyncFrom(store, replay.length > 0 ? 2 : 0).pipe(Stream.runCollect);
    expect(fromThird.map((e) => e._tag)).toEqual(["IssueChanged", "JobChanged", "SessionChanged"]);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerPublishing(layerJournaling(layerMemory))),
    Effect.provide(layerWorkGraphEvents),
  ),
);

it.live("resyncEvents replays durable history, then streams the live tail", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    // A mutation BEFORE any client attaches — only a durable resync can catch it up.
    yield* store.workGraph.putWorkstream(workstream);

    // Attach: eager-subscribe live, replay the durable log, then the live tail.
    const collecting = yield* resyncEvents(store, feed).pipe(
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild,
    );
    // Let the replay drain and the live subscription settle before the live mutation.
    yield* Effect.sleep("20 millis");

    // A mutation AFTER attach arrives on the live tail.
    yield* store.workGraph.putEpic(epic);

    const received = yield* Fiber.join(collecting);
    const first = received[0];
    const second = received[1];
    expect(first?._tag).toBe("WorkstreamChanged");
    expect(second?._tag).toBe("EpicChanged");
  }).pipe(
    Effect.scoped,
    Effect.provide(layerPublishing(layerJournaling(layerMemory))),
    Effect.provide(layerWorkGraphEvents),
  ),
);

it.live("resyncEvents on a fresh daemon replays nothing, then streams live", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    const collecting = yield* resyncEvents(store, feed).pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Effect.sleep("20 millis");
    yield* store.workGraph.putWorkstream(workstream);

    const received = yield* Fiber.join(collecting);
    const only: WorkGraphEvent | undefined = received[0];
    expect(only?._tag).toBe("WorkstreamChanged");
  }).pipe(
    Effect.scoped,
    Effect.provide(layerPublishing(layerJournaling(layerMemory))),
    Effect.provide(layerWorkGraphEvents),
  ),
);
