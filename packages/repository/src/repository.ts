/**
 * `Repository` — the repo-scoped code-host PORT the daemon reads through (Track A,
 * task AE3.2), expressed as an Effect `Context.Service` (D5/D14, INV-PORT).
 *
 * This module is the SEAM the daemon core and every consumer depend on: it
 * describes WHAT Sprinter needs from a code host, never HOW. It references NO
 * concrete client — no `gh`, no octokit, no GitHub URL — those live ONLY behind an
 * adapter `Layer` ({@link ./github.ts}). One `Repository` binds ONE repository
 * (D14): cross-repo work is many instances.
 *
 * GitHub holds only Issues and the PRs that close them (D13); the port mirrors
 * exactly that surface, composed of three capability groups (INV-NAMING, `*Ops`
 * suffix):
 *
 * - {@link CodeOps} — branch/merge-relevant reads the daemon needs from the host.
 * - {@link IssueOps} — read an Issue (its state / closed-ness).
 * - {@link PullRequestOps} — PR detection + reconciliation: which PR closes an
 *   Issue, and whether that PR merged.
 *
 * The port speaks Sprinter's words (PR / Issue / repository), never a vendor's, in
 * OWNED, provider-neutral schemas — {@link RepositoryIssue} and the reused
 * `PullRequestRef` from `@sprinter/domain`. All failures are the owned
 * {@link RepositoryError}; no host-specific error (an HTTP error, a decode error)
 * ever crosses the port (INV-PORT). Reconciliation is one-directional (D13): the
 * daemon reads from here and rolls status up into `@sprinter/state`
 * ({@link ./reconcile.ts}); it never writes planning state back to the host.
 */
import { Context, type Effect, type Option, Schema } from "effect";
import { PositiveInt, type PullRequestRef } from "@sprinter/domain";

// ============================================================================
// Errors
// ============================================================================

/**
 * The single owned failure raised by every {@link Repository} operation
 * (INV-NAMING, `*Error` via `Schema.TaggedErrorClass`). `operation` names the port
 * method that failed; `detail` carries a neutral, human-readable cause.
 *
 * This is the ONLY error the port exposes: an adapter translates its host-specific
 * failures (an `HttpClientError`, a `Schema.SchemaError`) into this type at the
 * boundary, so no consumer ever depends on a concrete host's error shape
 * (INV-PORT).
 */
export class RepositoryError extends Schema.TaggedErrorClass<RepositoryError>()("RepositoryError", {
  /** The port operation that failed, e.g. `"getIssue"` or `"getPullRequest"`. */
  operation: Schema.String,
  /** A neutral, human-readable description of the cause. */
  detail: Schema.String,
}) {}

// ============================================================================
// Owned, provider-neutral schemas
// ============================================================================

/**
 * An Issue's lifecycle on the code host: `open` or `closed`. This is the raw host
 * state — distinct from the richer Sprinter planning `IssueStatus` — and the only
 * Issue lifecycle GitHub tracks (D13).
 */
export const IssueState = Schema.Literals(["open", "closed"]);
export type IssueState = (typeof IssueState)["Type"];

/**
 * The host's view of an Issue: its `number`, `title`, and {@link IssueState}. This
 * is deliberately smaller than the owned domain `Issue` (which also carries
 * Sprinter-only planning: `epicId`, `dependsOn`, the planning `status`) — the host
 * knows only what D13 lets it know. The reconciler ({@link ./reconcile.ts}) maps
 * this onto the domain `Issue` in `@sprinter/state`.
 */
export const RepositoryIssue = Schema.Struct({
  number: PositiveInt,
  title: Schema.NonEmptyString,
  state: IssueState,
});
export type RepositoryIssue = (typeof RepositoryIssue)["Type"];

// ============================================================================
// Capability groups
// ============================================================================

/**
 * Branch/merge-relevant reads the daemon needs from the code host. The daemon
 * targets the repository's default branch when opening PRs and checks whether a
 * work branch already exists before dispatching.
 */
export interface CodeOps {
  /** The repository's default branch name (e.g. `"main"`). */
  readonly defaultBranch: Effect.Effect<string, RepositoryError>;
  /** Whether a branch with the given name exists on the repository. */
  readonly branchExists: (name: string) => Effect.Effect<boolean, RepositoryError>;
}

/**
 * Read an Issue from the code host. GitHub holds only Issues + the PRs that close
 * them (D13), so this reads the host-side view — enough to tell whether an Issue
 * has closed, which the reconciler pairs with PR-merged detection to decide that
 * an Issue's work has landed.
 */
export interface IssueOps {
  /** Read the {@link RepositoryIssue} for a given Issue number. */
  readonly getIssue: (number: PositiveInt) => Effect.Effect<RepositoryIssue, RepositoryError>;
}

/**
 * PR detection + reconciliation. Two capabilities, composed by the reconciler:
 * {@link closingPullRequest} finds a PR REFERENCING an Issue (a heuristic for the
 * closing PR), and {@link getPullRequest} reads WHETHER that PR merged. Together —
 * with the Issue also reported closed — they answer "has this Issue's work landed?"
 * one-directionally (D13).
 */
export interface PullRequestOps {
  /**
   * A PR that references the given Issue, if one exists — a HEURISTIC for the
   * closing PR (the GitHub adapter scans the Issue timeline for a cross-referencing
   * PR). `Option.none` when no PR references it yet. The reconciler gates on the
   * Issue being closed AND this PR being merged; robust closing-PR detection is a
   * live-wiring concern deferred to AE4/AE5.
   */
  readonly closingPullRequest: (
    issueNumber: PositiveInt,
  ) => Effect.Effect<Option.Option<PositiveInt>, RepositoryError>;
  /** Read the {@link PullRequestRef} for a PR number — notably whether it merged. */
  readonly getPullRequest: (number: PositiveInt) => Effect.Effect<PullRequestRef, RepositoryError>;
}

// ============================================================================
// Port
// ============================================================================

/**
 * The repo-scoped `Repository` port — everything Sprinter needs from a code host,
 * composed of the three capability groups. The daemon core and reconciler depend
 * on THIS service; a host is chosen by providing one of its adapter `Layer`s (the
 * GitHub adapter in {@link ./github.ts}). The tag id follows INV-NAMING:
 * `sprinter/<area>/<Name>`.
 */
export class Repository extends Context.Service<
  Repository,
  {
    /** Branch/merge-relevant reads. */
    readonly code: CodeOps;
    /** Issue reads. */
    readonly issues: IssueOps;
    /** PR detection + reconciliation reads. */
    readonly pullRequests: PullRequestOps;
  }
>()("sprinter/repository/Repository") {}
