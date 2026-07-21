/**
 * Status roll-up coverage (AE3.2) — the one-directional reconciler exercised
 * against a FAKE {@link CodeHost} + the in-memory {@link StateStore}
 * (`layerMemory`). Deterministic and OFFLINE (INV-GATE): no HTTP, no live GitHub.
 *
 * The suite proves the D13 roll-up: reading which Issues closed / which PRs merged
 * from the host and rolling that up into Epic/Workstream `WorkStatus` in the state
 * store — flipping an Epic AND its Workstream to `done` once all children land, and
 * leaving them untouched while any child is unlanded. It also proves the AE2 / #23
 * wiring-constraint: the roll-up repairs the twice-stored parentage, so a child
 * reached via its FK (`Issue.epicId`) is added to its parent's child-list
 * (`Epic.issues`) even when it was missing there.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Schema } from "effect";
import { expect } from "vitest";
import {
  Epic,
  isComplete,
  isIssueLanded,
  Issue,
  PositiveInt,
  PullRequestRef,
  Workstream,
} from "@sprinter/domain";
import { Repository as DomainRepository } from "@sprinter/domain";
import { layerMemory, StateStore } from "@sprinter/state";
import { reconcileWorkstream, CodeHost, CodeHostError, RepositoryIssue } from "./index.ts";

// ============================================================================
// Fixtures — decoded through the owned schemas (no casts)
// ============================================================================

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

/**
 * The repository the workstream fixtures are anchored to. `repositoryId` is a real
 * FOREIGN KEY, so this has to be stored before any workstream references it.
 */
const repository = decode(DomainRepository, {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
});

const workstream = decode(Workstream, {
  id: "ws-a",
  name: "Track A",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["epic-1"],
});

// The epic's `issues` list deliberately OMITS issue-101 — an inconsistent
// twice-stored parentage the roll-up must repair via the FK (wiring-constraint).
const epic = decode(Epic, {
  id: "epic-1",
  workstreamId: "ws-a",
  name: "CodeHost",
  status: "pending",
  issues: ["issue-100"],
});

const issue = (number: number) =>
  decode(Issue, {
    id: `issue-${number}`,
    epicId: "epic-1",
    number,
    title: `Issue ${number}`,
    status: "in_progress",
    dependsOn: [],
  });

// ============================================================================
// Fake CodeHost — a canned host, no HTTP
// ============================================================================

interface HostState {
  /** Issue number → host state; absent ⇒ still open. */
  readonly issues: ReadonlyMap<number, "open" | "closed">;
  /** Issue number → the PR number that closes it; absent ⇒ no closing PR. */
  readonly closing: ReadonlyMap<number, number>;
  /** PR number → whether it merged; absent ⇒ not merged. */
  readonly pulls: ReadonlyMap<number, boolean>;
  /** Issue numbers whose `getIssue` fails with a host error (a 404/403/429). */
  readonly failing?: ReadonlySet<number>;
}

const posInt = (n: number): PositiveInt => decode(PositiveInt, n);

const repoIssue = (number: number, state: "open" | "closed"): RepositoryIssue =>
  decode(RepositoryIssue, { number, title: `Issue ${number}`, state });

const pullRef = (number: number, merged: boolean): PullRequestRef =>
  decode(PullRequestRef, {
    number,
    url: `https://github.com/callajd/sprinter/pull/${number}`,
    merged,
  });

const fakeRepository = (host: HostState): Layer.Layer<CodeHost> =>
  Layer.succeed(
    CodeHost,
    CodeHost.of({
      repositories: {
        // Resolves ANY key to a canned observation: these suites exercise Issue/PR
        // reconciliation, not repository resolution (which is tested on its own).
        resolve: (key) =>
          Effect.succeed(
            Option.some(
              decode(DomainRepository, {
                id: `repo:${key.host}:${key.owner}/${key.name}`,
                host: key.host,
                owner: key.owner,
                name: key.name,
                refs: [{ name: "main", sha: "a".repeat(40) }],
                observedAt: "2026-07-20T12:00:00.000Z",
              }),
            ),
          ),
      },
      code: {
        defaultBranch: Effect.succeed("main"),
        branchExists: () => Effect.succeed(true),
      },
      issues: {
        getIssue: (number) =>
          host.failing?.has(number) === true
            ? Effect.fail(
                new CodeHostError({ operation: "getIssue", detail: `host 404 #${number}` }),
              )
            : Effect.succeed(repoIssue(number, host.issues.get(number) ?? "open")),
      },
      pullRequests: {
        closingPullRequest: (issueNumber) => {
          const pr = host.closing.get(issueNumber);
          return Effect.succeed(pr === undefined ? Option.none() : Option.some(posInt(pr)));
        },
        getPullRequest: (number) =>
          Effect.succeed(pullRef(number, host.pulls.get(number) ?? false)),
      },
    }),
  );

const seed = Effect.gen(function* () {
  const store = yield* StateStore;
  yield* store.repositories.putRepository(repository);
  yield* store.workGraph.putWorkstream(workstream);
  yield* store.workGraph.putEpic(epic);
  yield* store.workGraph.putIssue(issue(100));
  yield* store.workGraph.putIssue(issue(101));
});

// ============================================================================
// Tests
// ============================================================================

it.effect("rolls a fully-landed workstream up to done and repairs parentage", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* seed;

    yield* reconcileWorkstream(workstream.id);

    // Both issues landed: done + merged PR.
    const i100 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(100).id));
    const i101 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(101).id));
    expect(i100.status).toBe("done");
    expect(isIssueLanded(i100)).toBe(true);
    expect(i100.pr?.number).toBe(100);
    expect(isIssueLanded(i101)).toBe(true);

    // Epic flipped to done, and its child-list REPAIRED to include issue-101.
    const ep = Option.getOrThrow(yield* store.workGraph.getEpic(epic.id));
    expect(isComplete(ep)).toBe(true);
    expect([...ep.issues].sort()).toStrictEqual(["issue-100", "issue-101"]);

    // Workstream flipped to done, child-list consistent with the epic FK.
    const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
    expect(isComplete(ws)).toBe(true);
    expect(ws.epics).toStrictEqual(["epic-1"]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        layerMemory,
        fakeRepository({
          issues: new Map([
            [100, "closed"],
            [101, "closed"],
          ]),
          closing: new Map([
            [100, 100],
            [101, 101],
          ]),
          pulls: new Map([
            [100, true],
            [101, true],
          ]),
        }),
      ),
    ),
  ),
);

it.effect("never overwrites a cancelled epic/workstream to done, even once its issues land", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    // A cancelled epic + workstream whose issues nonetheless land on the host: the
    // roll-up treats cancelled as terminal-but-not-done and must NOT resurrect them.
    yield* store.repositories.putRepository(repository);
    yield* store.workGraph.putWorkstream({ ...workstream, status: "cancelled" });
    yield* store.workGraph.putEpic({ ...epic, status: "cancelled" });
    yield* store.workGraph.putIssue(issue(100));
    yield* store.workGraph.putIssue(issue(101));

    yield* reconcileWorkstream(workstream.id);

    // The issues still land (Issue/PR-level reconciliation is unconditional)...
    const i100 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(100).id));
    expect(isIssueLanded(i100)).toBe(true);

    // ...but the terminal cancelled nodes are left cancelled, not flipped to done.
    const ep = Option.getOrThrow(yield* store.workGraph.getEpic(epic.id));
    const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
    expect(ep.status).toBe("cancelled");
    expect(ws.status).toBe("cancelled");
    expect(isComplete(ep)).toBe(false);
    expect(isComplete(ws)).toBe(false);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        layerMemory,
        fakeRepository({
          issues: new Map([
            [100, "closed"],
            [101, "closed"],
          ]),
          closing: new Map([
            [100, 100],
            [101, 101],
          ]),
          pulls: new Map([
            [100, true],
            [101, true],
          ]),
        }),
      ),
    ),
  ),
);

it.effect("leaves an epic and workstream unfinished while any issue is unlanded", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    yield* seed;

    yield* reconcileWorkstream(workstream.id);

    // issue-100 landed; issue-101 still open ⇒ unchanged.
    const i100 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(100).id));
    const i101 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(101).id));
    expect(i100.status).toBe("done");
    expect(i101.status).toBe("in_progress");
    expect("pr" in i101).toBe(false);

    // Neither the epic nor the workstream is complete.
    const ep = Option.getOrThrow(yield* store.workGraph.getEpic(epic.id));
    const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
    expect(isComplete(ep)).toBe(false);
    expect(isComplete(ws)).toBe(false);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        layerMemory,
        fakeRepository({
          issues: new Map([
            [100, "closed"],
            [101, "open"],
          ]),
          closing: new Map([[100, 100]]),
          pulls: new Map([[100, true]]),
        }),
      ),
    ),
  ),
);

it.effect(
  "isolates one issue's host error, still lands its siblings, and surfaces the failure",
  () =>
    Effect.gen(function* () {
      const store = yield* StateStore;
      yield* seed;

      // issue-100's host read fails (a 404/403/429); issue-101 is closed + merged.
      const outcome = yield* reconcileWorkstream(workstream.id);

      // The sibling still landed — one flaky read did not abort the roll-up.
      const i100 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(100).id));
      const i101 = Option.getOrThrow(yield* store.workGraph.getIssue(issue(101).id));
      expect(i100.status).toBe("in_progress");
      expect(isIssueLanded(i101)).toBe(true);

      // The failure is SURFACED (not swallowed), naming the failed issue number.
      expect(outcome.failures).toStrictEqual([{ issueNumber: 100, detail: "host 404 #100" }]);

      // Roll-up stays conservative: a failed issue can't be observed landed, so its
      // Epic and Workstream are held back from auto-`done`.
      const ep = Option.getOrThrow(yield* store.workGraph.getEpic(epic.id));
      const ws = Option.getOrThrow(yield* store.workGraph.getWorkstream(workstream.id));
      expect(isComplete(ep)).toBe(false);
      expect(isComplete(ws)).toBe(false);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          layerMemory,
          fakeRepository({
            issues: new Map([[101, "closed"]]),
            closing: new Map([[101, 101]]),
            pulls: new Map([[101, true]]),
            failing: new Set([100]),
          }),
        ),
      ),
    ),
);

it.effect(
  "repairs the Epic→Workstream child-list from the FK when the workstream omits an epic",
  () =>
    Effect.gen(function* () {
      const store = yield* StateStore;
      // The workstream lists NO epics, but epic-9 names it via its FK (`workstreamId`)
      // — an inconsistent twice-stored parentage the roll-up must repair (wiring-
      // constraint), the Epic→Workstream direction (`consistentEpics`).
      const ws = decode(Workstream, {
        id: "ws-x",
        name: "X",
        repositoryId: "repo:github:1296269",
        status: "active",
        epics: [],
      });
      const ep = decode(Epic, {
        id: "epic-9",
        workstreamId: "ws-x",
        name: "E",
        status: "pending",
        issues: ["issue-900"],
      });
      const iss = decode(Issue, {
        id: "issue-900",
        epicId: "epic-9",
        number: 900,
        title: "I",
        status: "in_progress",
        dependsOn: [],
      });
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(ws);
      yield* store.workGraph.putEpic(ep);
      yield* store.workGraph.putIssue(iss);

      yield* reconcileWorkstream(ws.id);

      // The workstream's `epics` list is repaired to include epic-9 (reached via its
      // FK), even though it started empty; and the fully-landed workstream is done.
      const repaired = Option.getOrThrow(yield* store.workGraph.getWorkstream(ws.id));
      expect(repaired.epics).toStrictEqual(["epic-9"]);
      expect(isComplete(repaired)).toBe(true);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          layerMemory,
          fakeRepository({
            issues: new Map([[900, "closed"]]),
            closing: new Map([[900, 900]]),
            pulls: new Map([[900, true]]),
          }),
        ),
      ),
    ),
);

it.effect("is a no-op for a missing workstream", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const missing = decode(Workstream, {
      id: "ws-missing",
      name: "Nope",
      repositoryId: "repo:github:1296269",
      status: "active",
      epics: [],
    });
    yield* reconcileWorkstream(missing.id);
    expect(yield* store.workGraph.getWorkstream(missing.id)).toStrictEqual(Option.none());
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        layerMemory,
        fakeRepository({ issues: new Map(), closing: new Map(), pulls: new Map() }),
      ),
    ),
  ),
);
