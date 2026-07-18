/**
 * Status roll-up coverage (AE3.2) — the one-directional reconciler exercised
 * against a FAKE {@link Repository} + the in-memory {@link StateStore}
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
import { layerMemory, StateStore } from "@sprinter/state";
import { reconcileWorkstream, Repository, RepositoryIssue } from "./index.ts";

// ============================================================================
// Fixtures — decoded through the owned schemas (no casts)
// ============================================================================

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

const workstream = decode(Workstream, {
  id: "ws-a",
  name: "Track A",
  repo: "callajd/sprinter",
  status: "active",
  epics: ["epic-1"],
});

// The epic's `issues` list deliberately OMITS issue-101 — an inconsistent
// twice-stored parentage the roll-up must repair via the FK (wiring-constraint).
const epic = decode(Epic, {
  id: "epic-1",
  workstreamId: "ws-a",
  name: "Repository",
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
// Fake Repository — a canned host, no HTTP
// ============================================================================

interface HostState {
  /** Issue number → host state; absent ⇒ still open. */
  readonly issues: ReadonlyMap<number, "open" | "closed">;
  /** Issue number → the PR number that closes it; absent ⇒ no closing PR. */
  readonly closing: ReadonlyMap<number, number>;
  /** PR number → whether it merged; absent ⇒ not merged. */
  readonly pulls: ReadonlyMap<number, boolean>;
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

const fakeRepository = (host: HostState): Layer.Layer<Repository> =>
  Layer.succeed(
    Repository,
    Repository.of({
      code: {
        defaultBranch: Effect.succeed("main"),
        branchExists: () => Effect.succeed(true),
      },
      issues: {
        getIssue: (number) => Effect.succeed(repoIssue(number, host.issues.get(number) ?? "open")),
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

it.effect("is a no-op for a missing workstream", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const missing = decode(Workstream, {
      id: "ws-missing",
      name: "Nope",
      repo: "callajd/sprinter",
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
