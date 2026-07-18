/**
 * The one-directional status roll-up (Track A, task AE3.2) — Sprinter's bridge
 * FROM the code host INTO its own durable planning state.
 *
 * Reconciliation is one-directional and Issue/PR-level (D13): it READS which
 * Issues have closed and which PRs have merged from the {@link Repository} port,
 * then ROLLS that up into Epic/Workstream `WorkStatus` persisted via the
 * `@sprinter/state` {@link StateStore}. It NEVER writes planning state back to the
 * host — Workstreams and Epics live entirely in Sprinter (D13). Both ports are
 * fakeable seams, so this is exercised offline against a fake `Repository` + the
 * in-memory `StateStore` ({@link ./reconcile.test.ts}).
 *
 * An Issue is LANDED when the host reports it closed AND its closing PR merged
 * (the owned `isIssueLanded`). When an Issue lands, its domain node is updated to
 * `status: "done"` with the merged {@link PullRequestRef}. An Epic rolls to `done`
 * once all its Issues have landed; a Workstream rolls to `done` once all its Epics
 * are complete.
 *
 * **Wiring-constraint (AE2 / #23 F4).** The work graph stores parentage TWICE —
 * parent child-lists (`Workstream.epics` / `Epic.issues`) AND child FK refs
 * (`Epic.workstreamId` / `Issue.epicId`) — with no FK enforcement. This roll-up
 * keeps the two consistent on every write: when it writes a child, it also upserts
 * the parent with that child present in its list (`ensureIssueInEpic` /
 * `ensureEpicInWorkstream`), so a child reached via its FK is never missing from
 * its parent's list.
 */
import { Effect, Option } from "effect";
import {
  type Epic,
  type Issue,
  isComplete,
  isIssueLanded,
  type IssueId,
  type Workstream,
  type WorkstreamId,
} from "@sprinter/domain";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { Repository, type RepositoryError } from "./repository.ts";

/** Append `id` to `list` if absent, preserving order; returns the list unchanged if present. */
const withChild = <A>(list: ReadonlyArray<A>, id: A): ReadonlyArray<A> =>
  list.includes(id) ? list : [...list, id];

/**
 * Reconcile ONE Issue against the host and, if it has landed, persist the update.
 * Landed = host reports the Issue closed AND its closing PR merged (D13). On
 * landing, upsert the Issue node (`done` + merged PR, keeping its `epicId`) and —
 * per the wiring-constraint — ensure the parent Epic lists it.
 */
const reconcileIssue = (
  epic: Epic,
  issue: Issue,
): Effect.Effect<void, RepositoryError | StateStoreError, Repository | StateStore> =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const store = yield* StateStore;

    const hostIssue = yield* repo.issues.getIssue(issue.number);
    if (hostIssue.state !== "closed") return;

    const closingPr = yield* repo.pullRequests.closingPullRequest(issue.number);
    if (Option.isNone(closingPr)) return;

    const pr = yield* repo.pullRequests.getPullRequest(closingPr.value);
    if (!pr.merged) return;

    const landed: Issue = { ...issue, status: "done", pr };
    yield* store.workGraph.putIssue(landed);
    yield* ensureIssueInEpic(epic.id, issue.id);
  });

/**
 * Wiring-constraint repair for the `Issue → Epic` edge: ensure the Epic's `issues`
 * child-list contains `issueId` (the Issue already names the Epic via its FK). Reads
 * the CURRENT Epic so repeated repairs accumulate rather than clobber.
 */
const ensureIssueInEpic = (
  epicId: Epic["id"],
  issueId: IssueId,
): Effect.Effect<void, StateStoreError, StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const current = yield* store.workGraph.getEpic(epicId);
    if (Option.isNone(current)) return;
    const epic = current.value;
    const issues = withChild(epic.issues, issueId);
    if (issues === epic.issues) return;
    yield* store.workGraph.putEpic({ ...epic, issues });
  });

/**
 * Wiring-constraint repair for the `Epic → Workstream` edge: ensure the
 * Workstream's `epics` child-list contains `epicId`.
 */
const ensureEpicInWorkstream = (
  workstreamId: WorkstreamId,
  epicId: Epic["id"],
): Effect.Effect<void, StateStoreError, StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const current = yield* store.workGraph.getWorkstream(workstreamId);
    if (Option.isNone(current)) return;
    const workstream = current.value;
    const epics = withChild(workstream.epics, epicId);
    if (epics === workstream.epics) return;
    yield* store.workGraph.putWorkstream({ ...workstream, epics });
  });

/**
 * Roll one Epic up: reconcile each of its Issues, then flip the Epic to `done` if
 * every Issue has landed. On flipping, upsert the Epic (`done`) AND — per the
 * wiring-constraint — ensure the parent Workstream lists it. Returns whether the
 * Epic is now complete, so the Workstream roll-up can decide its own status.
 */
const reconcileEpic = (
  epic: Epic,
): Effect.Effect<boolean, RepositoryError | StateStoreError, Repository | StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const issues = yield* store.workGraph.listIssues(epic.id);
    yield* Effect.forEach(issues, (issue) => reconcileIssue(epic, issue));

    const refreshed = yield* store.workGraph.listIssues(epic.id);
    const allLanded = refreshed.length > 0 && refreshed.every(isIssueLanded);

    // Re-read the Epic so any `ensureIssueInEpic` repair from the loop is preserved.
    const currentEpic = Option.getOrElse(yield* store.workGraph.getEpic(epic.id), () => epic);
    if (allLanded && currentEpic.status !== "done") {
      const done: Epic = { ...currentEpic, status: "done" };
      yield* store.workGraph.putEpic(done);
      yield* ensureEpicInWorkstream(currentEpic.workstreamId, currentEpic.id);
      return true;
    }
    return isComplete(currentEpic);
  });

/**
 * Reconcile an entire Workstream one-directionally (D13): roll each Epic up from
 * the host, then flip the Workstream to `done` once every Epic is complete. A
 * missing Workstream is a no-op. The Workstream upsert re-includes all its Epics
 * (wiring-constraint), so its child-list stays consistent with the Epics' FKs.
 */
export const reconcileWorkstream = (
  workstreamId: WorkstreamId,
): Effect.Effect<void, RepositoryError | StateStoreError, Repository | StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const found = yield* store.workGraph.getWorkstream(workstreamId);
    if (Option.isNone(found)) return;

    const epics = yield* store.workGraph.listEpics(workstreamId);
    const completed = yield* Effect.forEach(epics, reconcileEpic);
    const allComplete = epics.length > 0 && completed.every((done) => done);

    // Re-read so any `ensureEpicInWorkstream` repair is preserved, then keep the
    // child-list consistent with the current Epics' FKs on the final upsert.
    const currentWs = Option.getOrElse(
      yield* store.workGraph.getWorkstream(workstreamId),
      () => found.value,
    );
    const currentEpics = yield* store.workGraph.listEpics(workstreamId);
    const epicIds = currentEpics.map((epic) => epic.id);
    const consistentEpics = epicIds.reduce<ReadonlyArray<Epic["id"]>>(withChild, currentWs.epics);
    const status: Workstream["status"] =
      allComplete && currentWs.status !== "done" ? "done" : currentWs.status;

    if (status !== currentWs.status || consistentEpics !== currentWs.epics) {
      yield* store.workGraph.putWorkstream({ ...currentWs, status, epics: consistentEpics });
    }
  });
