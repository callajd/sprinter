/**
 * Durable journaling + offset-based resync coverage (CE1.2 / CE2.0) — the
 * `./event-journal.ts` seam that makes the `events` feed a deterministic,
 * offset-based catch-up rather than snapshot-on-connect (D17), with each streamed
 * item carrying its durable offset. Exercised against the in-memory `StateStore`
 * (`layerMemory`) behind the journaling decorator (which both journals durably AND
 * fans out live) and the real `WorkGraphEvents` `PubSub` — deterministic and
 * offline (INV-PORT).
 */
import { it } from "@effect/vitest";
import { Context, Effect, Fiber, Option, PubSub, Schema, Stream } from "effect";
import { expect } from "vitest";
import { type OffsetEvent, ResyncRequired } from "@sprinter/contract";
import {
  Agent,
  Epic,
  Issue,
  Job,
  type SessionEvent,
  SessionId,
  Session,
  Workstream,
} from "@sprinter/domain";
import { layerMemory, StateStore, StateStoreError } from "@sprinter/state";
import { layerJournaling, resyncEvents, resyncFrom, resyncSessionFrom } from "./event-journal.ts";
import { layer as layerSessionEvents, SessionEvents } from "./session-events.ts";
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
const agent = Schema.decodeUnknownSync(Agent)({
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read", "edit"],
});
// An issue WITH dependency edges, so `putIssue` is genuinely multi-statement
// (rewrite the issue row + its `issue_dependency` edges) and opens its own inner
// transaction — the NESTED SAVEPOINT case (FIX 3).
const issueWithDeps = Schema.decodeUnknownSync(Issue)({
  id: "iss-2",
  epicId: "ep-1",
  number: 53,
  title: "Nested rollback",
  status: "in_progress",
  dependsOn: ["iss-1"],
});

// ── harness: journaling (durable + live fan-out) over layerMemory ─────────────

const seedGraph = (store: Context.Service.Shape<typeof StateStore>) =>
  Effect.gen(function* () {
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);
    yield* store.workGraph.putIssue(issue);
    yield* store.jobs.putJob(job);
    yield* store.jobs.putSession(session);
    yield* store.agents.putAgent(agent);
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
      "AgentChanged",
    ]);
    // Offsets are strictly increasing (the monotonic tail cursor).
    const offsets = entries.map((e) => e.offset);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    expect(new Set(offsets).size).toBe(offsets.length);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.effect("journals an agent append ONCE — an idempotent re-append adds no second delta", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;
    const subscription = yield* feed.subscribe;

    // The crash-retry case: the SAME revision appended twice. The registry write is a
    // genuine no-op the second time (the row is byte-identical and already stored), so
    // the journal must record ONE `AgentChanged` — not one per attempt. Journaling
    // unconditionally would let a retry loop grow the durable log without bound and
    // re-fan a delta nothing changed.
    expect(yield* store.agents.putAgent(agent)).toBe("appended");
    expect(yield* store.agents.putAgent(agent)).toBe("unchanged");

    const entries = yield* store.events.read;
    expect(entries.map((e) => e.kind)).toEqual(["AgentChanged"]);
    // And the LIVE fan-out is skipped on the no-op too, not merely the durable append:
    // the only published item is the first one (the second `take` would block, so a
    // second publish is proven absent by driving a DIFFERENT delta through after it).
    yield* store.workGraph.putWorkstream(workstream);
    const published = [yield* PubSub.take(subscription), yield* PubSub.take(subscription)];
    expect(published.map((e) => e.event._tag)).toEqual(["AgentChanged", "WorkstreamChanged"]);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
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
    }).pipe(
      Effect.scoped,
      Effect.provide(layerJournaling(layerMemory)),
      Effect.provide(layerWorkGraphEvents),
      Effect.provide(layerSessionEvents),
    ),
);

it.effect(
  "journals a NESTED multi-statement putIssue ATOMICALLY: a failed outer transaction rolls back BOTH the issue's node writes and its delta (INV-RESTART)",
  () =>
    Effect.gen(function* () {
      const store = yield* StateStore;

      // `putIssue` is multi-statement (rewrite the issue row + its dependency edges)
      // and opens its OWN inner transaction, so under the outer `withTransaction` the
      // journaling decorator opens it must nest as a SAVEPOINT. A failure after the
      // journaled put must roll back BOTH the nested savepoint's issue rows AND the
      // offset-log delta — never the node-persisted-but-delta-missing split a crash
      // between two separate commits would leave. This exercises the deep savepoint
      // nesting (outer BEGIN → journaling SAVEPOINT → putIssue's own SAVEPOINT).
      const boom = new StateStoreError({ operation: "test", detail: "boom" });
      const failure = yield* store.workGraph
        .putIssue(issueWithDeps)
        .pipe(Effect.andThen(Effect.fail(boom)), store.withTransaction, Effect.flip);
      expect(failure).toBe(boom);

      // Nothing committed: neither the issue node (with its edges) nor its delta survive.
      const persisted = yield* store.workGraph.getIssue(issueWithDeps.id);
      expect(persisted._tag).toBe("None");
      const entries = yield* store.events.tail(0);
      expect(entries).toEqual([]);

      // And a NORMAL nested putIssue commits both together — the issue (with its
      // reconstructed `dependsOn` edges) is readable and exactly one delta is journaled.
      yield* store.workGraph.putIssue(issueWithDeps);
      const committed = yield* store.workGraph.getIssue(issueWithDeps.id);
      expect(committed._tag).toBe("Some");
      expect(Option.getOrThrow(committed).dependsOn).toEqual(["iss-1"]);
      const journaled = yield* store.events.tail(0);
      expect(journaled.map((e) => e.kind)).toEqual(["IssueChanged"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(layerJournaling(layerMemory)),
      Effect.provide(layerWorkGraphEvents),
      Effect.provide(layerSessionEvents),
    ),
);

it.effect("resyncFrom decodes the durable log into offset-stamped owned deltas", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* seedGraph(store);

    const replay = yield* resyncFrom(store, 0).pipe(Stream.runCollect);
    // Each item pairs the owned delta with its durable offset (CE2.0).
    expect(replay.map((e) => e.event)).toEqual([
      { _tag: "WorkstreamChanged", workstream },
      { _tag: "EpicChanged", epic },
      { _tag: "IssueChanged", issue },
      { _tag: "JobChanged", job },
      { _tag: "SessionChanged", session },
      // The REGISTRY delta rides the same durable log as the work graph.
      { _tag: "AgentChanged", agent },
    ]);
    // Offsets are strictly increasing (the monotonic tail cursor) and all > 0.
    const offsets = replay.map((e) => e.offset);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    expect(offsets.every((o) => o > 0)).toBe(true);
    expect(new Set(offsets).size).toBe(offsets.length);

    // A cursor past the first two entries resumes from the third (offset-based):
    // every replayed offset is STRICTLY GREATER than the cursor and contiguous.
    const cursor = offsets[1] ?? 0;
    const fromThird = yield* resyncFrom(store, cursor).pipe(Stream.runCollect);
    expect(fromThird.map((e) => e.event._tag)).toEqual([
      "IssueChanged",
      "JobChanged",
      "SessionChanged",
      "AgentChanged",
    ]);
    expect(fromThird.every((e) => e.offset > cursor)).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.effect("the live feed stamps every fanned-out delta with its durable offset (CE2.0)", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;
    // Subscribe BEFORE mutating so the live fan-out is observed in order.
    const subscription = yield* feed.subscribe;

    yield* seedGraph(store);

    const received: Array<OffsetEvent> = [];
    for (let i = 0; i < 6; i++) received.push(yield* PubSub.take(subscription));

    // The live tail carries the SAME coordinate space as the durable replay: the
    // published offsets match the journaled `event_log` offsets exactly.
    const journaled = yield* store.events.tail(0);
    expect(received.map((e) => e.offset)).toEqual(journaled.map((j) => j.offset));
    expect(received.map((e) => e.event._tag)).toEqual([
      "WorkstreamChanged",
      "EpicChanged",
      "IssueChanged",
      "JobChanged",
      "SessionChanged",
      "AgentChanged",
    ]);
    // Monotonic, strictly increasing.
    const offsets = received.map((e) => e.offset);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    expect(new Set(offsets).size).toBe(offsets.length);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
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
    expect(first?.event._tag).toBe("WorkstreamChanged");
    expect(second?.event._tag).toBe("EpicChanged");
    // Replay (durable) then live-tail offsets are one strictly-increasing coordinate.
    expect(first !== undefined && second !== undefined && second.offset > first.offset).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.live("resyncEvents resumes durable replay from a sinceOffset cursor (CE2.0)", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    // Two mutations BEFORE any client attaches — journaled at offsets 1 and 2.
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);

    // Attach with a cursor PAST the first entry (offset 1): the resync must replay
    // only the second durable delta (strictly-after semantics), not re-send offset 1.
    const collecting = yield* resyncEvents(store, feed, 1).pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );

    const received = yield* Fiber.join(collecting);
    const only: OffsetEvent | undefined = received[0];
    expect(only?.event._tag).toBe("EpicChanged");
    // Strictly-after semantics: the replayed offset is greater than the cursor (1).
    expect((only?.offset ?? 0) > 1).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.live("resyncEvents fails a cursor BEYOND the log's extent with ResyncRequired", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    // Two mutations, journaled at offsets 1 and 2 — the WHOLE log of this generation.
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);

    // A cursor from a DEAD STORE GENERATION: bumping SCHEMA_VERSION drops and
    // recreates `event_log`, whose AUTOINCREMENT offsets restart at 1, so a client's
    // durable cursor can sit far beyond the new log's extent. Neither silent repair
    // works — honouring it strands the client, replaying from the origin behind its
    // back leaves its contiguous cursor stale so it discards everything — so the
    // generation is made EXPLICIT and the request FAILS.
    const failure = yield* Effect.flip(
      resyncEvents(store, feed, 999).pipe(Stream.take(2), Stream.runCollect),
    );
    expect(failure).toBeInstanceOf(ResyncRequired);
    // Self-describing: the rejected cursor and the extent it exceeded.
    expect({ sinceOffset: failure.sinceOffset, maxOffset: failure.maxOffset }).toStrictEqual({
      sinceOffset: 999,
      maxOffset: 2,
    });
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.live("resyncEvents fails a non-zero cursor against an EMPTY log — the post-reset case", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    // The EXACT shape of a freshly reset daemon: the schema version was bumped, the
    // database was dropped and recreated, and NOTHING has been journaled yet — while a
    // client reconnects holding a cursor from the destroyed generation. `maxOffset` is
    // `0` (the empty-log sentinel), which every non-zero cursor exceeds.
    const failure = yield* Effect.flip(
      resyncEvents(store, feed, 7).pipe(Stream.take(1), Stream.runCollect),
    );
    expect(failure).toBeInstanceOf(ResyncRequired);
    expect(failure.maxOffset).toBe(0);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.live("resyncEvents accepts the ORIGIN cursor against an empty log — it is never stale", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;

    // `sinceOffset: 0` (and an absent cursor) asks for the whole log, which is a valid
    // request against EVERY generation including an empty one — a first connect must
    // never be told to resync. It replays nothing and streams the live tail.
    const collecting = yield* resyncEvents(store, feed, 0).pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Effect.sleep("20 millis");
    yield* store.workGraph.putWorkstream(workstream);

    const received = yield* Fiber.join(collecting);
    expect(received[0]?.event._tag).toBe("WorkstreamChanged");
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.live("resyncEvents replays nothing for a caught-up cursor AT the log's max offset", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const feed = yield* WorkGraphEvents;
    yield* store.workGraph.putWorkstream(workstream);
    yield* store.workGraph.putEpic(epic);

    // The boundary the stale-epoch rule must NOT swallow: a fully caught-up client
    // (cursor == max offset) is not stale, so it replays nothing and only streams
    // the live tail — it is never re-sent history it already acknowledged.
    const collecting = yield* resyncEvents(store, feed, 2).pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Effect.sleep("20 millis");
    yield* store.workGraph.putIssue(issue);

    const received = yield* Fiber.join(collecting);
    const only: OffsetEvent | undefined = received[0];
    expect(only?.event._tag).toBe("IssueChanged");
    expect((only?.offset ?? 0) > 2).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
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
    const only: OffsetEvent | undefined = received[0];
    expect(only?.event._tag).toBe("WorkstreamChanged");
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

// ── session-transcript journaling + durable replay ───────────────

const sessionA: SessionId = session.id; // "ses-1"
const sessionB: SessionId = Schema.decodeUnknownSync(SessionId)("ses-2");
const entryA1: SessionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "AssistantMessage", id: "a1", text: "hello" },
};
const noticeA: SessionEvent = { _tag: "Notice", id: "n1", level: "warn", message: "heads up" };
const entryA2: SessionEvent = {
  _tag: "EntryAppended",
  entry: { _tag: "AssistantMessage", id: "a2", text: "done" },
};
// Ephemeral live deltas — NOT transcript-grade, so `publishEphemeral` fans them out
// offset-less and never persists them.
const turnStartedA: SessionEvent = { _tag: "TurnStarted" };
const deltaA: SessionEvent = { _tag: "MessageDelta", messageId: "a1", text: "hel" };

it.effect(
  "resyncSessionFrom replays a session's durable transcript, offset-stamped and scoped",
  () =>
    Effect.gen(function* () {
      const store = yield* StateStore;
      // Interleave two sessions to prove per-session scoping AND that the shared global
      // offset counter keeps each session's replay strictly increasing (not contiguous).
      yield* store.sessionLog.append(sessionA, entryA1);
      yield* store.sessionLog.append(sessionB, entryA1);
      yield* store.sessionLog.append(sessionA, noticeA);
      yield* store.sessionLog.append(sessionA, entryA2);

      const replay = yield* resyncSessionFrom(store, sessionA, 0).pipe(Stream.runCollect);
      // Only session A's entries, in order — session B is scoped out.
      expect(replay.map((e) => e.event)).toEqual([entryA1, noticeA, entryA2]);
      // A durable replay is offset-BEARING throughout (every item has a present offset).
      const offsets = replay.flatMap((e) => (e.offset === undefined ? [] : [e.offset]));
      expect(offsets.length).toBe(replay.length);
      expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
      expect(offsets.every((o) => o > 0)).toBe(true);

      // A cursor past the first entry resumes STRICTLY AFTER it (offset-based resume).
      const cursor = offsets[0] ?? 0;
      const fromSecond = yield* resyncSessionFrom(store, sessionA, cursor).pipe(Stream.runCollect);
      expect(fromSecond.map((e) => e.event)).toEqual([noticeA, entryA2]);
      expect(fromSecond.every((e) => e.offset !== undefined && e.offset > cursor)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(layerJournaling(layerMemory)),
      Effect.provide(layerWorkGraphEvents),
      Effect.provide(layerSessionEvents),
    ),
);

it.effect("the SessionEvents feed stamps every appended durable entry with its offset", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const sessionFeed = yield* SessionEvents;
    // Subscribe BEFORE appending so the live fan-out is observed in order.
    const subscription = yield* sessionFeed.subscribe;

    yield* store.sessionLog.append(sessionA, entryA1);
    yield* store.sessionLog.append(sessionA, noticeA);

    const first = yield* PubSub.take(subscription);
    const second = yield* PubSub.take(subscription);
    expect([first.event, second.event]).toEqual([entryA1, noticeA]);
    expect(first.sessionId).toBe(sessionA);
    // The live offsets match the durably-journaled ones exactly — one coordinate space.
    // Both are durable appends, so both feed items are offset-BEARING (present).
    const journaled = yield* store.sessionLog.tail(sessionA, 0);
    expect([first.offset, second.offset]).toEqual(journaled.map((j) => j.offset));
    expect(
      first.offset !== undefined && second.offset !== undefined && second.offset > first.offset,
    ).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.effect("the SessionEvents feed fans out ephemeral deltas OFFSET-LESS, without persisting", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const sessionFeed = yield* SessionEvents;
    const subscription = yield* sessionFeed.subscribe;

    // Interleave a durable append and two ephemeral publishes, as the fold's tee does.
    yield* store.sessionLog.append(sessionA, entryA1);
    yield* store.sessionLog.publishEphemeral(sessionA, turnStartedA);
    yield* store.sessionLog.publishEphemeral(sessionA, deltaA);

    const first = yield* PubSub.take(subscription);
    const second = yield* PubSub.take(subscription);
    const third = yield* PubSub.take(subscription);
    // All three reach the live feed, in emission order (the dual modality on ONE channel).
    expect([first.event, second.event, third.event]).toEqual([entryA1, turnStartedA, deltaA]);
    // The durable entry is offset-STAMPED; the ephemeral deltas are offset-LESS.
    expect(first.offset).not.toBeUndefined();
    expect(second.offset).toBeUndefined();
    expect(third.offset).toBeUndefined();
    // Ephemeral deltas are NOT persisted: the durable transcript holds only the one entry.
    const persisted = yield* store.sessionLog.read(sessionA);
    expect(persisted.map((e) => e.event)).toEqual([entryA1]);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);

it.effect("a re-dispatch of the same session APPENDS without resetting the offset sequence", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // First run persists two entries…
    yield* store.sessionLog.append(sessionA, entryA1);
    yield* store.sessionLog.append(sessionA, noticeA);
    const afterFirst = yield* store.sessionLog.read(sessionA);
    // …a re-dispatch (same session id) appends a third at a STRICTLY HIGHER offset — the
    // sequence is never reset or duplicated (offsets stay monotonic and unique).
    yield* store.sessionLog.append(sessionA, entryA2);
    const afterSecond = yield* store.sessionLog.read(sessionA);

    expect(afterSecond.map((e) => e.event)).toEqual([entryA1, noticeA, entryA2]);
    const offsets = afterSecond.map((e) => e.offset);
    expect(new Set(offsets).size).toBe(offsets.length);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    const last = afterFirst.at(-1)?.offset ?? 0;
    expect((afterSecond.at(-1)?.offset ?? 0) > last).toBe(true);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerJournaling(layerMemory)),
    Effect.provide(layerWorkGraphEvents),
    Effect.provide(layerSessionEvents),
  ),
);
