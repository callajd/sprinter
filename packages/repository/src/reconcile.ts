/**
 * The one-directional status roll-up (Track A, task AE3.2) — Sprinter's bridge
 * FROM the code host INTO its own durable planning state.
 *
 * Reconciliation is one-directional and Issue/PR-level (D13): it READS which
 * Issues have closed and which PRs have merged from the {@link CodeHost} port,
 * then ROLLS that up into Epic/Workstream `WorkStatus` persisted via the
 * `@sprinter/state` {@link StateStore}. It NEVER writes planning state back to the
 * host — Workstreams and Epics live entirely in Sprinter (D13). Both ports are
 * fakeable seams, so this is exercised offline against a fake `CodeHost` + the
 * in-memory `StateStore` ({@link ./reconcile.test.ts}).
 *
 * An Issue is LANDED when the host reports it closed AND its closing PR merged
 * (the owned `isIssueLanded`). When an Issue lands, its domain node is updated to
 * `status: "done"` with the merged {@link PullRequestRef}. An Epic rolls to `done`
 * once all its Issues have landed; a Workstream rolls to `done` once all its Epics
 * are complete.
 *
 * **Per-issue error isolation (AE3.2 / #27 F4, resolved AE5.1 / #32; hardened
 * CE1.3 / #53).** The Issue loop is catch-and-continue on a host failure: a single
 * Issue's `CodeHostError` (a 404 for a deleted Issue, a 403/429 rate-limit) is
 * caught, logged, AND COLLECTED, and the roll-up proceeds with the remaining Issues
 * — one flaky host read never aborts the whole Workstream roll-up. A
 * {@link StateStoreError} is NOT isolated: our own durable store failing is a real
 * abort, not a transient host hiccup.
 *
 * **Partial roll-up semantics (CE1.3 / #53).** A partially-failed roll-up is
 * BEST-EFFORT and does not throw: every Issue that read cleanly still lands and its
 * Epic/Workstream still rolls up, while the Issues that failed are surfaced in the
 * returned {@link ReconcileOutcome#failures} rather than swallowed into logs alone.
 * The status roll-up is naturally conservative under partial failure — a failed
 * Issue cannot be observed as landed, so its Epic (and thus its Workstream) is held
 * back from auto-`done` until a later reconcile reads it cleanly (roll-up is
 * one-directional and idempotent, D13). A caller can therefore both apply the
 * best-effort progress AND see, per run, which Issues need a retry.
 *
 * **Wiring-constraint (AE2 / #23 F4).** The work graph stores parentage TWICE —
 * parent child-lists (`Workstream.epics` / `Epic.issues`) AND child FK refs
 * (`Epic.workstreamId` / `Issue.epicId`) — with no FK enforcement. This roll-up
 * keeps the two consistent: the `Issue → Epic` edge is repaired per-landing
 * (`ensureIssueInEpic`), and the `Epic → Workstream` edge is repaired once,
 * unconditionally, by `reconcileWorkstream`'s final `consistentEpics` fold — so a
 * child reached via its FK is never missing from its parent's list.
 */
import { Effect, Option } from "effect";
import {
  type Epic,
  type Issue,
  isComplete,
  isIssueLanded,
  type IssueId,
  isTerminal,
  type Workstream,
  type WorkstreamId,
} from "@sprinter/domain";
import { StateStore, type StateStoreError } from "@sprinter/state";
import { CodeHost, type CodeHostError } from "./code-host.ts";

/**
 * One Issue that a roll-up could NOT read from the host — a per-issue
 * `CodeHostError` (a 404/403/429) that was isolated so the rest of the roll-up
 * proceeded. Surfaced in the {@link ReconcileOutcome} so a partially-failed run is
 * observable, not silent.
 */
export interface ReconcileFailure {
  /** The host Issue number whose read failed. */
  readonly issueNumber: number;
  /** The neutral, human-readable cause carried by the isolated `CodeHostError`. */
  readonly detail: string;
}

/**
 * The best-effort result of {@link reconcileWorkstream}: the Issues whose host read
 * failed and were isolated (`failures`). An empty `failures` is a fully-clean
 * roll-up. The status updates for the Issues that DID succeed are applied to the
 * store regardless — a partial failure never rolls them back (see the module's
 * partial roll-up semantics).
 */
export interface ReconcileOutcome {
  /** The Issues skipped after an isolated host failure; empty ⇒ a clean roll-up. */
  readonly failures: ReadonlyArray<ReconcileFailure>;
}

/** The best-effort outcome of rolling ONE Epic up: its completion + isolated failures. */
interface EpicOutcome {
  /** Whether the Epic is now complete (all Issues landed), for the parent roll-up. */
  readonly complete: boolean;
  /** The Epic's Issues skipped after an isolated host failure. */
  readonly failures: ReadonlyArray<ReconcileFailure>;
}

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
): Effect.Effect<void, CodeHostError | StateStoreError, CodeHost | StateStore> =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const store = yield* StateStore;

    const hostIssue = yield* host.issues.getIssue(issue.number);
    if (hostIssue.state !== "closed") return;

    const closingPr = yield* host.pullRequests.closingPullRequest(issue.number);
    if (Option.isNone(closingPr)) return;

    const pr = yield* host.pullRequests.getPullRequest(closingPr.value);
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
 * Roll one Epic up: reconcile each of its Issues (isolating per-issue host
 * failures), then flip the Epic to `done` if every Issue has landed. Returns its
 * {@link EpicOutcome} — completion (so the Workstream roll-up can decide its own
 * status and, per the wiring-constraint, keep its `epics` child-list consistent
 * with the Epics' FKs in one final upsert) plus any isolated failures to surface.
 */
const reconcileEpic = (
  epic: Epic,
): Effect.Effect<EpicOutcome, StateStoreError, CodeHost | StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const issues = yield* store.workGraph.listIssues(epic.id);
    // Per-issue error isolation (F4): a host failure on one Issue is caught, logged,
    // AND collected so the roll-up continues; a StateStoreError is NOT caught (our
    // own store failing is a real abort). `catchTag` narrows the loop's error to the
    // owned StateStoreError, leaving `CodeHost` in the requirement. Each Issue
    // yields the failures it produced (none on success), flattened for the Epic.
    const perIssue = yield* Effect.forEach(issues, (issue) =>
      reconcileIssue(epic, issue).pipe(
        Effect.as<ReadonlyArray<ReconcileFailure>>([]),
        Effect.catchTag("CodeHostError", (error) =>
          Effect.logWarning(
            `reconcile: skipping issue #${issue.number} after host error: ${error.detail}`,
          ).pipe(Effect.as([{ issueNumber: issue.number, detail: error.detail }])),
        ),
      ),
    );
    const failures = perIssue.flat();

    const refreshed = yield* store.workGraph.listIssues(epic.id);
    const allLanded = refreshed.length > 0 && refreshed.every(isIssueLanded);

    // Re-read the Epic so any `ensureIssueInEpic` repair from the loop is preserved.
    // A terminal Epic (`done` OR `cancelled`) is never overwritten: a cancelled Epic
    // stays cancelled even once its Issues land (isTerminal, CE5.1).
    const currentEpic = Option.getOrElse(yield* store.workGraph.getEpic(epic.id), () => epic);
    if (allLanded && !isTerminal(currentEpic)) {
      const done: Epic = { ...currentEpic, status: "done" };
      yield* store.workGraph.putEpic(done);
      // The Epic→Workstream child-list repair is handled once, unconditionally, by
      // `reconcileWorkstream`'s final `consistentEpics` fold — not here.
      return { complete: true, failures };
    }
    // Intentionally done-only (not `isTerminal`): a `cancelled` Epic is terminal but
    // NOT complete, so it deliberately holds its parent Workstream back from
    // auto-`done` (abandoned work must not auto-declare the parent finished).
    // Unreachable today — `control cancel` targets Workstreams only, nothing sets an
    // Epic to `cancelled` — so whoever adds Epic-level cancellation owns revisiting
    // this roll-up semantics.
    return { complete: isComplete(currentEpic), failures };
  });

/**
 * Reconcile an entire Workstream one-directionally (D13): roll each Epic up from
 * the host, then flip the Workstream to `done` once every Epic is complete. A
 * missing Workstream is a no-op (a clean, empty {@link ReconcileOutcome}). The
 * Workstream upsert re-includes all its Epics (wiring-constraint), so its
 * child-list stays consistent with the Epics' FKs. Returns the best-effort
 * outcome: the roll-up is applied for every Issue that read cleanly, and any
 * host-failed Issue is surfaced in `failures` (never swallowed) so a partial run is
 * observable.
 */
export const reconcileWorkstream = (
  workstreamId: WorkstreamId,
): Effect.Effect<ReconcileOutcome, StateStoreError, CodeHost | StateStore> =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const found = yield* store.workGraph.getWorkstream(workstreamId);
    if (Option.isNone(found)) return { failures: [] };

    const epics = yield* store.workGraph.listEpics(workstreamId);
    const outcomes = yield* Effect.forEach(epics, reconcileEpic);
    const allComplete = epics.length > 0 && outcomes.every((outcome) => outcome.complete);
    const failures = outcomes.flatMap((outcome) => outcome.failures);

    // Re-read so any `ensureEpicInWorkstream` repair is preserved, then keep the
    // child-list consistent with the current Epics' FKs on the final upsert.
    const currentWs = Option.getOrElse(
      yield* store.workGraph.getWorkstream(workstreamId),
      () => found.value,
    );
    const currentEpics = yield* store.workGraph.listEpics(workstreamId);
    const epicIds = currentEpics.map((epic) => epic.id);
    const consistentEpics = epicIds.reduce<ReadonlyArray<Epic["id"]>>(withChild, currentWs.epics);
    // A terminal Workstream (`done` OR `cancelled`) is never overwritten — a
    // cancelled Workstream stays cancelled even if every Epic is complete (CE5.1).
    const status: Workstream["status"] =
      allComplete && !isTerminal(currentWs) ? "done" : currentWs.status;

    if (status !== currentWs.status || consistentEpics !== currentWs.epics) {
      yield* store.workGraph.putWorkstream({ ...currentWs, status, epics: consistentEpics });
    }

    return { failures };
  });
