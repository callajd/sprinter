/**
 * `@sprinter/repository` — the code-host bridge (Track A, epic AE3).
 *
 * The public surface is the provider-neutral {@link CodeHost} PORT (an Effect
 * `Context.Service`) plus its owned error and view schemas, the GitHub ADAPTER
 * `Layer`s behind it, and the one-directional status {@link reconcileWorkstream}
 * roll-up. Consumers depend ONLY on the port and choose a host by providing an
 * adapter layer (INV-PORT); the GitHub/HTTP backing is sealed inside
 * {@link ./github.ts} and never leaks a type here (D5/D13/D14).
 *
 * The port is named for its ROLE — the code host — which is what frees the plain name
 * `Repository` for the OWNED ENTITY it now belongs to (`@sprinter/domain`, DE1.2 D1):
 * the port is the external system, the entity is the record read off it.
 */
export type { CodeOps, IssueOps, PullRequestOps, RepositoryOps } from "./code-host.ts";
export {
  CodeHost,
  CodeHostError,
  CodeHostFailure,
  IssueState,
  RepositoryIssue,
} from "./code-host.ts";
export type { RepositoryConfig } from "./github.ts";
export { hostInstant, layer, layerFetch, repositoryIdFor } from "./github.ts";
export type { ReconcileFailure, ReconcileOutcome } from "./reconcile.ts";
export { reconcileWorkstream } from "./reconcile.ts";
