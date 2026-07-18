/**
 * `@sprinter/repository` — the repo-scoped code-host bridge (Track A, epic AE3).
 *
 * The public surface is the provider-neutral {@link Repository} PORT (an Effect
 * `Context.Service`) plus its owned error and view schemas, the GitHub ADAPTER
 * `Layer`s behind it, and the one-directional status {@link reconcileWorkstream}
 * roll-up. Consumers depend ONLY on the port and choose a host by providing an
 * adapter layer (INV-PORT); the GitHub/HTTP backing is sealed inside
 * {@link ./github.ts} and never leaks a type here (D5/D13/D14).
 */
export type { CodeOps, IssueOps, PullRequestOps } from "./repository.ts";
export { IssueState, Repository, RepositoryError, RepositoryIssue } from "./repository.ts";
export type { RepositoryConfig } from "./github.ts";
export { layer, layerFetch } from "./github.ts";
export { reconcileWorkstream } from "./reconcile.ts";
