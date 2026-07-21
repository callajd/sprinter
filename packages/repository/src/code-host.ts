/**
 * `CodeHost` — the PORT the daemon reads a code host through (Track A, task AE3.2;
 * renamed from `Repository` by DE1.2), expressed as an Effect `Context.Service`
 * (D5/D14, INV-PORT).
 *
 * This module is the SEAM the daemon core and every consumer depend on: it
 * describes WHAT Sprinter needs from a code host, never HOW. It references NO
 * concrete client — no `gh`, no octokit, no GitHub URL — those live ONLY behind an
 * adapter `Layer` ({@link ./github.ts}).
 *
 * ## Why the port is called `CodeHost` and not `Repository` (DE1.2 D1)
 *
 * `conventions.md` names a port by its ROLE-NOUN, and reserves PLAIN names for OWNED
 * domain types. This port's role is *the code host* — the external system Sprinter
 * observes repositories on — and `CodeHost` says exactly that. `Repository` is now an
 * OWNED ENTITY (`@sprinter/domain`'s {@link Repository}: the thing that is observed,
 * with an id, a natural key, its observed refs and an `observedAt`), so the plain name
 * belongs to it. Holding both under one name was the source of a real confusion: "the
 * Repository" meant either the remote system or the record read off it depending on
 * the sentence.
 *
 * A `CodeHost` adapter is repo-scoped for the ISSUE/PR reads (D14 — one bound
 * `owner/repo`); the repository-OBSERVATION capability below takes its target
 * explicitly, because its whole job is to answer "which repository is this key?" for a
 * caller that holds no id yet.
 *
 * The port speaks Sprinter's words (PR / Issue / repository), never a vendor's, in
 * OWNED, provider-neutral schemas — {@link RepositoryIssue}, the reused
 * `PullRequestRef`, and the owned `Repository` entity. All failures are the owned
 * {@link CodeHostError}; no host-specific error (an HTTP error, a decode error) ever
 * crosses the port (INV-PORT). Reconciliation is one-directional (D13): the daemon
 * reads from here and rolls status up into `@sprinter/state` ({@link ./reconcile.ts});
 * it never writes planning state back to the host.
 */
import { Context, type Effect, type Option, Schema } from "effect";
import {
  PositiveInt,
  type PullRequestRef,
  type Repository,
  type RepositoryKey,
} from "@sprinter/domain";

// ============================================================================
// Errors
// ============================================================================

/**
 * The single owned failure raised by every {@link CodeHost} operation
 * (INV-NAMING, `*Error` via `Schema.TaggedErrorClass`). `operation` names the port
 * method that failed; `detail` carries a neutral, human-readable cause.
 *
 * This is the ONLY error the port exposes: an adapter translates its host-specific
 * failures (an `HttpClientError`, a `Schema.SchemaError`) into this type at the
 * boundary, so no consumer ever depends on a concrete host's error shape
 * (INV-PORT).
 */
export class CodeHostError extends Schema.TaggedErrorClass<CodeHostError>()("CodeHostError", {
  /** The port operation that failed, e.g. `"getIssue"` or `"resolveRepository"`. */
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
 * OBSERVE a repository on the code host — the capability that mints and refreshes the
 * owned {@link Repository} entity (DE1.2).
 *
 * It takes a {@link RepositoryKey} (`host / owner / name`) rather than reading the
 * adapter's bound repo, because its callers hold exactly that and nothing more: a
 * `WorkstreamPlan` arrives from a client with no `RepositoryId` (D6), and turning the
 * key it names into an identity is precisely this port's job.
 *
 * `resolve` answers `Option.none` for a repository the host does NOT know. That is a
 * NORMAL outcome, not a failure: "no such repository" is information the host
 * legitimately has, whereas {@link CodeHostError} means the host could not be asked.
 * The distinction matters upstream — a plan naming an unknown repository is rejected
 * with a reason and writes NOTHING, and must never fall through to creating an
 * unobserved row.
 *
 * A REFRESH is the same call: it returns a complete new observation under a NEW
 * `observedAt`, which the caller stores wholesale (D7). There is no partial/merge
 * variant, and no staleness parameter — this port never decides that an observation is
 * too old to return.
 *
 * The returned entity's `id` is minted BY the adapter, as a deterministic function of
 * the repository the host resolved the key TO — an identifier the HOST owns, never the
 * key itself, which is mutable. So re-observing one repository yields the same id even
 * across a RENAME, while the natural key on the returned record follows the rename (see
 * `RepositoryId`). Nothing above the port may parse it.
 */
export interface RepositoryOps {
  /**
   * Observe the repository named by `key`, as a complete {@link Repository} stamped
   * with the instant of the observation. `Option.none` when the host does not know it.
   */
  readonly resolve: (key: RepositoryKey) => Effect.Effect<Option.Option<Repository>, CodeHostError>;
}

/**
 * Branch/merge-relevant reads the daemon needs from the code host. The daemon
 * targets the repository's default branch when opening PRs and checks whether a
 * work branch already exists before dispatching.
 */
export interface CodeOps {
  /** The repository's default branch name (e.g. `"main"`). */
  readonly defaultBranch: Effect.Effect<string, CodeHostError>;
  /** Whether a branch with the given name exists on the repository. */
  readonly branchExists: (name: string) => Effect.Effect<boolean, CodeHostError>;
}

/**
 * Read an Issue from the code host. GitHub holds only Issues + the PRs that close
 * them (D13), so this reads the host-side view — enough to tell whether an Issue
 * has closed, which the reconciler pairs with PR-merged detection to decide that
 * an Issue's work has landed.
 */
export interface IssueOps {
  /** Read the {@link RepositoryIssue} for a given Issue number. */
  readonly getIssue: (number: PositiveInt) => Effect.Effect<RepositoryIssue, CodeHostError>;
}

/**
 * PR detection + reconciliation. Two capabilities, composed by the reconciler:
 * {@link closingPullRequest} finds the PR that CLOSED an Issue, and
 * {@link getPullRequest} reads WHETHER that PR merged. Together — with the Issue
 * also reported closed — they answer "has this Issue's work landed?"
 * one-directionally (D13).
 */
export interface PullRequestOps {
  /**
   * The PR that CLOSED the given Issue, if any — the authoritative signal (the
   * GitHub adapter reads GraphQL `closedByPullRequestsReferences`, the PRs GitHub
   * itself records as closing the Issue). `Option.none` when no PR closed it. This
   * no longer false-positives on a PR that merely *references* a hand-closed Issue
   * (the retired timeline heuristic did — D18, resolved by CE1.3). The reconciler
   * still gates on the Issue being closed AND this PR being merged.
   */
  readonly closingPullRequest: (
    issueNumber: PositiveInt,
  ) => Effect.Effect<Option.Option<PositiveInt>, CodeHostError>;
  /** Read the {@link PullRequestRef} for a PR number — notably whether it merged. */
  readonly getPullRequest: (number: PositiveInt) => Effect.Effect<PullRequestRef, CodeHostError>;
}

// ============================================================================
// Port
// ============================================================================

/**
 * The `CodeHost` port — everything Sprinter needs from a code host, composed of its
 * capability groups. The daemon core and reconciler depend on THIS service; a host is
 * chosen by providing one of its adapter `Layer`s (the GitHub adapter in
 * {@link ./github.ts}). The tag id follows INV-NAMING: `sprinter/<area>/<Name>`.
 */
export class CodeHost extends Context.Service<
  CodeHost,
  {
    /** Repository OBSERVATION — resolve/refresh the owned `Repository` entity. */
    readonly repositories: RepositoryOps;
    /** Branch/merge-relevant reads. */
    readonly code: CodeOps;
    /** Issue reads. */
    readonly issues: IssueOps;
    /** PR detection + reconciliation reads. */
    readonly pullRequests: PullRequestOps;
  }
>()("sprinter/repository/CodeHost") {}
