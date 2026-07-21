/**
 * `StateStore` coverage (AE2.1) — the port exercised THROUGH the SQLite adapter
 * against an in-memory database (deterministic, offline, no filesystem). Every
 * test provides {@link layerMemory}, so each runs on a fresh `:memory:` database.
 *
 * The suite proves the capability groups round-trip the OWNED domain schemas: the
 * append-only `Agent` registry (append + read, no delete, and no rewrite of a
 * stored revision), the work graph (`Workstream ⊃ Epic ⊃ Issue`), the dependency
 * DAG (`Issue.dependsOn`) as real replaceable edges, the `Job` model and the
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
import { Agent, Epic, Issue, isLineageRetired, Job, Session, Workstream } from "@sprinter/domain";
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

const agent = (over: Partial<(typeof Agent)["Encoded"]> = {}) =>
  decode(Agent, {
    id: "agt-1",
    name: "implementer",
    model: "claude-opus-4-8",
    version: "1.0.0",
    tools: ["read", "edit"],
    ...over,
  });

const prRef = { number: 42, url: "https://github.com/callajd/sprinter/pull/42", merged: true };

// ============================================================================
// AgentStore — the append-only, globally-scoped registry
// ============================================================================

it.effect("round-trips an Agent revision and lists the registry", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const first = yield* agent();
    const revised = yield* agent({ id: "agt-2", version: "1.1.0", supersedes: "agt-1" });

    yield* store.agents.putAgent(first);
    yield* store.agents.putAgent(revised);

    expect(Option.getOrThrow(yield* store.agents.getAgent(first.id))).toStrictEqual(first);
    expect(Option.getOrThrow(yield* store.agents.getAgent(revised.id))).toStrictEqual(revised);
    // Both revisions persist: an edit APPENDS, it never replaces the record it
    // supersedes, so a past execution still resolves to the exact revision that ran.
    expect(yield* store.agents.listAgents).toStrictEqual([first, revised]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("elides an Agent's absent optional keys, and retirement is a NEW revision", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const plain = yield* agent();
    yield* store.agents.putAgent(plain);
    const read = Option.getOrThrow(yield* store.agents.getAgent(plain.id));
    expect("supersedes" in read).toBe(false);
    expect("retiredAt" in read).toBe(false);

    // Retiring appends a NEW id carrying BOTH `supersedes` (naming the head it
    // retires) AND the `retiredAt` stamp — never a stamp applied to the existing
    // row. Carrying `supersedes` is what makes it a RETIREMENT of `agt-1` rather
    // than a brand-new lineage that was born retired.
    const retired = yield* agent({
      id: "agt-3",
      supersedes: "agt-1",
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    yield* store.agents.putAgent(retired);
    const all = yield* store.agents.listAgents;
    expect(all).toHaveLength(2);
    const stored = Option.getOrThrow(yield* store.agents.getAgent(retired.id));
    expect(stored.retiredAt).toBe("2026-07-20T12:00:00.000Z");
    expect(stored.supersedes).toBe("agt-1");
    // The revision it retires is untouched — still readable, still not retired, so
    // a past execution that ran on it still resolves.
    expect(Option.getOrThrow(yield* store.agents.getAgent(plain.id))).toStrictEqual(plain);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("re-appending a byte-identical Agent revision is an idempotent no-op", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const first = yield* agent();
    // The crash-retry case: the same write replayed must SUCCEED (so a retry is not
    // an error) without appending a second row or altering the stored one. The two
    // calls are REPORTED apart, though — a decorator that journals a delta per append
    // needs to know which one actually wrote, or a retry loop grows the event log
    // without bound.
    expect(yield* store.agents.putAgent(first)).toBe("appended");
    expect(yield* store.agents.putAgent(yield* agent())).toBe("unchanged");

    expect(yield* store.agents.listAgents).toStrictEqual([first]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("rejects a RETIRING revision that also rewrites the content it retires", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const head = yield* agent({ id: "agt-2", tools: ["read", "edit"] });
    yield* store.agents.putAgent(head);

    // A retirement is LIFECYCLE-ONLY. This append retires `agt-2` while also blanking
    // its `tools`, fusing an EDIT and a RETIREMENT into one indistinguishable record:
    // a reader walking back from it could no longer tell which happened, and the
    // retired head's content would appear to have changed as it went out of service.
    const error = yield* store.agents
      .putAgent(
        yield* agent({
          id: "agt-3",
          supersedes: "agt-2",
          tools: [],
          retiredAt: "2026-07-20T12:00:00.000Z",
        }),
      )
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    // Nothing was written: the registry still holds only the head.
    expect(yield* store.agents.listAgents).toStrictEqual([head]);

    // The SAME retirement, content-preserving, is accepted — the rule constrains
    // retirement only, and there is a legitimate way to express the intent: append
    // the edit as its OWN revision, then retire that.
    const retirement = yield* agent({
      id: "agt-3",
      supersedes: "agt-2",
      tools: ["read", "edit"],
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    expect(yield* store.agents.putAgent(retirement)).toBe("appended");
    expect((yield* store.agents.listAgents).map((found) => found.id)).toStrictEqual([
      "agt-2",
      "agt-3",
    ]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("rejects a RETIRING revision that retires an ALREADY-RETIRED revision", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const head = yield* agent({ id: "agt-2" });
    yield* store.agents.putAgent(head);
    const retirement = yield* agent({
      id: "agt-3",
      supersedes: "agt-2",
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    yield* store.agents.putAgent(retirement);

    // A lineage goes out of service ONCE. This second retiring revision preserves the
    // content perfectly, so the lifecycle-only rule — which ignores the lifecycle
    // columns by design — cannot see it; it is exactly the append that rule leaves
    // through. Allowing it would leave TWO revisions each carrying a `retiredAt`, at
    // two different instants, with nothing to say which one the lineage actually
    // stopped at.
    const error = yield* store.agents
      .putAgent(
        yield* agent({
          id: "agt-4",
          supersedes: "agt-3",
          retiredAt: "2026-07-21T12:00:00.000Z",
        }),
      )
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    // Nothing was written — the history still holds exactly one retirement.
    expect((yield* store.agents.listAgents).map((found) => found.id)).toStrictEqual([
      "agt-2",
      "agt-3",
    ]);

    // And the retirement itself stays IDEMPOTENT: a byte-identical re-append of the
    // SAME retiring revision is the crash-retry case and is still a no-op, not a
    // second stamp. The rule rejects a NEW retirement, never a replayed one.
    expect(yield* store.agents.putAgent(retirement)).toBe("unchanged");
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("rejects ANY revision superseding a RETIRED one — a lineage is never resurrected", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.agents.putAgent(yield* agent({ id: "agt-1" }));
    yield* store.agents.putAgent(
      yield* agent({ id: "agt-2", supersedes: "agt-1", retiredAt: "2026-07-20T12:00:00.000Z" }),
    );
    const registry = yield* store.agents.listAgents;
    const original = registry.find((found) => found.id === "agt-1");
    if (original === undefined) throw new Error("expected agt-1");
    expect(isLineageRetired(original, registry)).toBe(true);

    // THE RESURRECTION. The retired-head rule used to be checked ONLY when the incoming
    // revision was ITSELF retiring, so an ordinary EDIT superseding the retired head
    // sailed through — and an edit carries no `retiredAt` of its own, so
    // `isLineageRetired` then walked forward off the retired revision onto a live head
    // and the whole lineage read as back IN SERVICE. Retirement is terminal: nothing may
    // be appended after it, retiring or not.
    const error = yield* store.agents
      .putAgent(yield* agent({ id: "agt-3", supersedes: "agt-2", version: "2.0.0" }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");

    // Nothing was written, and the lineage is still retired read from EVERY revision in
    // it — the property `store.ts` promises ("neither ... un-retire a retired agent").
    const after = yield* store.agents.listAgents;
    expect(after.map((found) => found.id)).toStrictEqual(["agt-1", "agt-2"]);
    expect(after.every((found) => isLineageRetired(found, after))).toBe(true);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("rejects a SECOND revision superseding the same one — a lineage is a chain", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.agents.putAgent(yield* agent({ id: "agt-1" }));
    expect(yield* store.agents.putAgent(yield* agent({ id: "agt-2", supersedes: "agt-1" }))).toBe(
      "appended",
    );

    // A FORK: two revisions claiming the same predecessor. There is no defined head,
    // and the damage is not cosmetic — `isLineageRetired` builds the reverse index and
    // keeps the FIRST successor it meets, so a forked history makes the SAME registry
    // answer "retired" or "not retired" depending on `listAgents`' (presentational,
    // id-ordered) order. The `agent_supersedes` UNIQUE index makes it unstorable, so the
    // predicate is order-independent for every history the store can produce.
    const error = yield* store.agents
      .putAgent(
        yield* agent({ id: "agt-3", supersedes: "agt-1", retiredAt: "2026-07-20T12:00:00.000Z" }),
      )
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    expect((yield* store.agents.listAgents).map((found) => found.id)).toStrictEqual([
      "agt-1",
      "agt-2",
    ]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("lets a NON-retiring revision change content freely — that is what an edit IS", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.agents.putAgent(yield* agent({ id: "agt-2", tools: ["read"] }));
    // An EDIT supersedes without retiring, so the content-preservation rule does not
    // apply to it: changing `tools`/`version` is precisely its purpose.
    const edited = yield* agent({
      id: "agt-3",
      supersedes: "agt-2",
      version: "2.0.0",
      tools: ["read", "edit", "bash"],
    });
    expect(yield* store.agents.putAgent(edited)).toBe("appended");
    expect(Option.getOrThrow(yield* store.agents.getAgent(edited.id))).toStrictEqual(edited);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("accepts a retiring revision whose superseded revision is not stored", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // The content rule is only CHECKABLE against a stored revision. A dangling
    // `supersedes` is the same writer obligation the acyclicity precondition already
    // carries, so the append succeeds rather than inventing a referential constraint
    // the append-only registry does not have.
    const retirement = yield* agent({
      id: "agt-3",
      supersedes: "agt-absent",
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    expect(yield* store.agents.putAgent(retirement)).toBe("appended");
    expect(yield* store.agents.listAgents).toStrictEqual([retirement]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("refuses to rewrite a stored Agent revision with different content", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const first = yield* agent();
    yield* store.agents.putAgent(first);

    // A stored revision is IMMUTABLE: an append under an existing id with different
    // content is a mutation attempt, not an edit path, so it FAILS rather than
    // silently rewriting the revision (and fanning that rewrite out as a delta).
    const error = yield* store.agents
      .putAgent(yield* agent({ version: "1.1.0", tools: ["read", "edit", "bash"] }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    expect(error.detail.length).toBeGreaterThan(0);

    // And the stored revision is byte-identical to what was appended.
    expect(Option.getOrThrow(yield* store.agents.getAgent(first.id))).toStrictEqual(first);
    expect(yield* store.agents.listAgents).toStrictEqual([first]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("cannot un-retire a retired Agent by re-appending its id", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const retired = yield* agent({
      id: "agt-3",
      supersedes: "agt-1",
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    yield* store.agents.putAgent(retired);

    // Dropping `retiredAt` on the SAME id would resurrect a retired agent. The
    // append-only write path rejects it; the stamp survives untouched.
    const error = yield* store.agents
      .putAgent(yield* agent({ id: "agt-3", supersedes: "agt-1" }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(Option.getOrThrow(yield* store.agents.getAgent(retired.id))).toStrictEqual(retired);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("rejects an Agent revision that supersedes itself", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // The one `supersedes` cycle a single append can create: a consumer walking the
    // chain backwards would never terminate, so the port refuses the write.
    const error = yield* store.agents
      .putAgent(yield* agent({ id: "agt-1", supersedes: "agt-1" }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    expect(yield* store.agents.listAgents).toStrictEqual([]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("lists the registry in BYTE order by id — the pinned contract order", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // Appended out of order, and NOT in lineage order: `agt-b` supersedes `agt-c`.
    const c = yield* agent({ id: "agt-c" });
    const b = yield* agent({ id: "agt-b", supersedes: "agt-c" });
    const a = yield* agent({ id: "agt-a" });
    yield* store.agents.putAgent(c);
    yield* store.agents.putAgent(b);
    yield* store.agents.putAgent(a);

    // The pinned order is BY ID — neither insertion nor lineage order. It is
    // presentational: consumers upsert by id, and a lineage is reconstructed by
    // walking `supersedes`, never by reading this order.
    expect((yield* store.agents.listAgents).map((found) => found.id)).toStrictEqual([
      "agt-a",
      "agt-b",
      "agt-c",
    ]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("orders ids by BYTE sequence, not by a human alphabetical reading", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // The distinction the port docstring pins: `id` is a `TEXT` column with no
    // `COLLATE`, so SQLite compares UTF-8 BYTES. `"agt-10"` therefore sorts BEFORE
    // `"agt-2"` ("1" < "2" byte-wise), and a non-ASCII id sorts by its encoding —
    // neither is what "alphabetical" would suggest. Documenting it vaguely would
    // invite a consumer to assume numeric or locale ordering; this pins the real one.
    for (const id of ["agt-2", "agt-10", "agt-Z", "agt-a", "agt-é"]) {
      yield* store.agents.putAgent(yield* agent({ id }));
    }
    expect((yield* store.agents.listAgents).map((found) => found.id)).toStrictEqual([
      "agt-10",
      "agt-2",
      "agt-Z",
      "agt-a",
      "agt-é",
    ]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("exposes NO delete on the AgentStore surface (append + read only)", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // The port surface is exactly append + read. A delete/remove/retire method
    // would be a contract break — there is nowhere to call one from.
    expect(Object.keys(store.agents).toSorted()).toStrictEqual([
      "getAgent",
      "listAgents",
      "putAgent",
    ]);
  }).pipe(Effect.provide(layerMemory)),
);

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

    // `maxOffset` is the log's EXTENT — the same coordinate the last entry carries,
    // answered from the backing's index rather than by materialising the feed. It is
    // what a resume cursor is validated against, so it must agree with `read` exactly.
    expect(yield* store.events.maxOffset).toBe(Option.getOrThrow(Arr.last(appended)).offset);

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
    // `0` is the empty-log sentinel: durable offsets are strictly `> 0`, so it is
    // below every real entry and needs no separate "no entries" shape.
    expect(yield* store.events.maxOffset).toBe(0);
    expect(yield* store.agents.getAgent((yield* agent()).id)).toStrictEqual(Option.none());
    expect(yield* store.agents.listAgents).toStrictEqual([]);
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
