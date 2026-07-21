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
import { Array as Arr, type Context, Effect, Exit, Option, Schema } from "effect";
import { expect } from "vitest";
import {
  Agent,
  Epic,
  Issue,
  isLineageRetired,
  Job,
  Repository,
  Session,
  Workstream,
} from "@sprinter/domain";
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

const SHA_MAIN = "0123456789abcdef0123456789abcdef01234567";
const SHA_FEAT = "89abcdef0123456789abcdef0123456789abcdef";

const repository = (over: Partial<(typeof Repository)["Encoded"]> = {}) =>
  decode(Repository, {
    id: "repo:github:callajd/sprinter",
    host: "github",
    owner: "callajd",
    name: "sprinter",
    refs: [{ name: "main", sha: SHA_MAIN }],
    observedAt: "2026-07-20T12:00:00.000Z",
    ...over,
  });

/**
 * Store the repository every {@link workstream} fixture is anchored to.
 *
 * `workstream.repositoryId` is a real FOREIGN KEY, so the anchor has to exist before
 * anything can reference it — that ordering requirement IS the constraint working, and
 * a test that forgot it would be rejected rather than quietly storing a dangling
 * reference (see the FK test below, which asserts exactly that).
 */
const seedRepository = (store: Context.Service.Shape<typeof StateStore>) =>
  repository().pipe(Effect.flatMap(store.repositories.putRepository), Effect.orDie);

const workstream = (over: Partial<(typeof Workstream)["Encoded"]> = {}) =>
  decode(Workstream, {
    id: "ws-a",
    name: "Track A",
    repositoryId: "repo:github:callajd/sprinter",
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

it.effect("rejects a revision whose superseded revision is not stored — no dangling link", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // REFERENTIAL INTEGRITY is what makes every OTHER lineage rule order-independent.
    // All three of them (no successor to a retired revision, a retirement may not
    // rewrite content, at most one successor) are decided by READING the superseded
    // row — so while a dangling `supersedes` was storable, a writer bypassed all of
    // them for free by simply REVERSING the write order: append the successor first
    // (nothing to check against) and its predecessor second (an ordinary first
    // revision). The `agent.supersedes` FOREIGN KEY removes the shape those attacks
    // are built from: a predecessor always exists BEFORE anything can name it, so
    // every rule sees the row it needs no matter what order the writer chose.
    const retirement = yield* agent({
      id: "agt-3",
      supersedes: "agt-absent",
      retiredAt: "2026-07-20T12:00:00.000Z",
    });
    const error = yield* store.agents.putAgent(retirement).pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    expect(error.detail.length).toBeGreaterThan(0);
    // Nothing was written — a half-linked history never exists, not even briefly.
    expect(yield* store.agents.listAgents).toStrictEqual([]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("cannot RESURRECT a lineage by appending the successor before its predecessor", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.agents.putAgent(yield* agent({ id: "agt-1" }));

    // THE REORDERED RESURRECTION. Forwards this is impossible: `agt-2` retires
    // `agt-1`, and nothing may then supersede the retired `agt-2`. Backwards it used
    // to work — append `agt-3` naming the not-yet-stored `agt-2` (dangling, so the
    // retired-head rule had nothing to look at), then append `agt-2` retiring `agt-1`
    // (an ordinary retirement of a live head). Two individually legal appends,
    // producing byte-for-byte the history the forwards test proves is unstorable, with
    // `agt-3` carrying no stamp so `isLineageRetired` walked past the retirement onto a
    // live head and the whole lineage read as back IN SERVICE.
    const error = yield* store.agents
      .putAgent(yield* agent({ id: "agt-3", supersedes: "agt-2", version: "2.0.0" }))
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");

    // The retirement still lands, and the lineage reads retired from EVERY revision —
    // the property that was FALSE on the reordered history.
    yield* store.agents.putAgent(
      yield* agent({ id: "agt-2", supersedes: "agt-1", retiredAt: "2026-07-20T12:00:00.000Z" }),
    );
    const after = yield* store.agents.listAgents;
    expect(after.map((found) => found.id)).toStrictEqual(["agt-1", "agt-2"]);
    expect(after.every((found) => isLineageRetired(found, after))).toBe(true);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("cannot REWRITE CONTENT while retiring by appending the retirement first", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;

    // THE REORDERED CONTENT REWRITE. `agt-6` retires `agt-5` while changing both
    // `model` and `tools` — the fused edit-and-retirement the lifecycle-only rule
    // exists to reject. Appending it FIRST used to skip that rule entirely (nothing
    // stored to compare against), and appending the real `agt-5` afterwards then
    // completed a history in which the retirement had rewritten content — exactly what
    // `docs/contract-mirror.md` promises the mirror never sees.
    const error = yield* store.agents
      .putAgent(
        yield* agent({
          id: "agt-6",
          supersedes: "agt-5",
          model: "gpt",
          tools: [],
          retiredAt: "2026-07-20T12:00:00.000Z",
        }),
      )
      .pipe(Effect.flip);
    expect(error).toBeInstanceOf(StateStoreError);
    expect(error.operation).toBe("putAgent");
    expect(yield* store.agents.listAgents).toStrictEqual([]);
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("cannot close a `supersedes` CYCLE — the backwards walk always terminates", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;

    // A TWO-REVISION CYCLE: `agt-a` supersedes `agt-b` and `agt-b` supersedes `agt-a`.
    // Neither append names itself, so the port's self-reference rule never fires, and
    // while dangling links were storable the pair landed and made
    // `isOriginalRevision`'s backwards walk non-terminating. A cycle can only ever be
    // CLOSED by an edge naming a row that does not exist yet, so the foreign key
    // rejects the first append and there is no order that recovers it — acyclicity
    // stops being an unenforceable writer obligation.
    const first = yield* store.agents
      .putAgent(yield* agent({ id: "agt-a", supersedes: "agt-b" }))
      .pipe(Effect.flip);
    expect(first).toBeInstanceOf(StateStoreError);

    // And the mirror image, so this is not an artefact of which id went first.
    const second = yield* store.agents
      .putAgent(yield* agent({ id: "agt-b", supersedes: "agt-a" }))
      .pipe(Effect.flip);
    expect(second).toBeInstanceOf(StateStoreError);
    expect(yield* store.agents.listAgents).toStrictEqual([]);
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
    // The head first: a revision may only name a predecessor that is already stored
    // (the `supersedes` foreign key), so a lineage is always built in order.
    yield* store.agents.putAgent(yield* agent({ id: "agt-1" }));
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
// RepositoryStore — the STATE layer's anchor, and the constraints behind it
// ============================================================================

it.effect("round-trips a repository observation, refs and all", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const repo = yield* repository({
      refs: [
        { name: "feat/x-1", sha: SHA_FEAT },
        { name: "main", sha: SHA_MAIN },
      ],
    });
    yield* store.repositories.putRepository(repo);

    expect(Option.getOrThrow(yield* store.repositories.getRepository(repo.id))).toStrictEqual(repo);
    // The natural key is what IDENTIFIES a repository, so it is a real read — the
    // path a caller holding a plan (and no id) takes.
    expect(
      Option.getOrThrow(
        yield* store.repositories.findRepository({
          host: "github",
          owner: "callajd",
          name: "sprinter",
        }),
      ),
    ).toStrictEqual(repo);
    expect(yield* store.repositories.listRepositories).toStrictEqual([repo]);
  }).pipe(Effect.provide(layerMemory)),
);

// An EMPTY ref set is a valid observation ("nothing observed yet", D4) and must
// survive the child-table round trip — the join has to produce `[]`, not a missing
// record or a decode failure.
it.effect("round-trips a repository with an EMPTY ref set", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const repo = yield* repository({ refs: [] });
    yield* store.repositories.putRepository(repo);
    expect(Option.getOrThrow(yield* store.repositories.getRepository(repo.id))).toStrictEqual(repo);
  }).pipe(Effect.provide(layerMemory)),
);

// D7: a refresh REPLACES the record wholesale under a new `observedAt` — it does not
// merge. So a branch that disappeared upstream must be GONE from the stored refs, not
// left behind from the earlier read.
it.effect("a refresh REPLACES the record wholesale and advances observedAt", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.repositories.putRepository(
      yield* repository({
        refs: [
          { name: "main", sha: SHA_MAIN },
          { name: "feat/gone", sha: SHA_FEAT },
        ],
      }),
    );
    const refreshed = yield* repository({
      refs: [{ name: "main", sha: SHA_FEAT }],
      observedAt: "2026-07-21T09:30:00.000Z",
    });
    yield* store.repositories.putRepository(refreshed);

    const stored = yield* store.repositories.listRepositories;
    expect(stored).toStrictEqual([refreshed]);
    // The stale branch is gone, and the surviving one carries the NEW tip.
    expect(stored[0]?.refs.map((ref) => ref.name)).toStrictEqual(["main"]);
    expect(stored[0]?.observedAt).toBe("2026-07-21T09:30:00.000Z");
  }).pipe(Effect.provide(layerMemory)),
);

// D3 — the NATURAL KEY is UNIQUE, and it is the BACKING that says so. Two rows for
// one repository is "two records disagreeing about the same thing"; a resolve-time
// read-then-check would let a concurrent or reordered write straight past it.
it.effect("REJECTS a second repository row for the same (host, owner, name)", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* store.repositories.putRepository(yield* repository());
    const duplicate = yield* repository({ id: "repo:github:callajd/sprinter#2" });
    // `flip` makes the REJECTION the success: a write that succeeded would flip into
    // the error channel and fail the test rather than pass it.
    const rejected = yield* store.repositories.putRepository(duplicate).pipe(Effect.flip);
    expect(rejected).toBeInstanceOf(StateStoreError);
    expect(rejected.operation).toBe("putRepository");
    // And nothing landed — the original row is untouched.
    expect(yield* store.repositories.listRepositories).toHaveLength(1);
  }).pipe(Effect.provide(layerMemory)),
);

// D4(a) — a repeated branch name within ONE repository is unconstructible. The domain
// schema refuses to build such a value at all, so the store never sees one; this
// asserts the schema-side half, which is what keeps a hand-built wire payload or
// fixture from carrying it either.
it.effect("REJECTS a ref list repeating a branch name before it can reach the store", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      Schema.decodeUnknownEffect(Repository)({
        id: "repo:github:callajd/sprinter",
        host: "github",
        owner: "callajd",
        name: "sprinter",
        refs: [
          { name: "main", sha: SHA_MAIN },
          { name: "main", sha: SHA_FEAT },
        ],
        observedAt: "2026-07-20T12:00:00.000Z",
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

// The FOREIGN KEY on `workstream.repositoryId`: a workstream naming a repository that
// was never observed is REJECTED at the write. NOT stored and reconciled later — that
// is the DE1.1 `supersedes` lesson applied here, and it is what makes every rule
// decided by READING the referent order-independent.
it.effect("REJECTS a workstream whose repositoryId names no stored repository", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const orphan = yield* workstream({ repositoryId: "repo:github:callajd/never-observed" });
    const rejected = yield* store.workGraph.putWorkstream(orphan).pipe(Effect.flip);
    expect(rejected).toBeInstanceOf(StateStoreError);
    expect(rejected.operation).toBe("putWorkstream");
    // Nothing was stored and later fixed up: the graph is still empty.
    expect(yield* store.workGraph.listWorkstreams).toStrictEqual([]);
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

    yield* seedRepository(store);
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
    yield* seedRepository(store);
    yield* store.workGraph.putWorkstream(yield* workstream({ name: "Original" }));
    yield* seedRepository(store);
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
    yield* seedRepository(store);
    yield* store.workGraph.putWorkstream(ws);
    expect(Option.getOrThrow(yield* store.workGraph.getWorkstream(ws.id))).toStrictEqual(ws);
    // Exercise the general `layer(config)` path (not just `layerMemory`).
  }).pipe(Effect.provide(layer({ filename: ":memory:", disableWAL: true }))),
);
