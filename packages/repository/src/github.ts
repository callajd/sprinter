/**
 * The GitHub ADAPTER behind the {@link Repository} port (Track A, task AE3.2).
 *
 * This is the ONLY module in the codebase permitted to reference the code host
 * concretely (INV-PORT / D5 / D14): GitHub's REST + GraphQL endpoints, its resource
 * paths, and its wire JSON. Nothing above it — no consumer, no reconciler — imports
 * a GitHub client, a URL, `gh`, or learns that a closing PR is found via GraphQL;
 * they build to the port ({@link ./repository.ts}) and this instance is provided
 * behind a `Layer`.
 *
 * The transport is Effect-native and Bun-native (INV: Bun + Effect, never Node):
 * `effect/unstable/http` `HttpClient` over {@link FetchHttpClient} (`globalThis.fetch`,
 * which Bun implements) — NO Node-only SDK, no `node:*`. Reads are REST; the
 * authoritative closing-PR signal is a GraphQL POST through the SAME injected
 * transport seam (so both stay fakeable offline). Every response is decoded through
 * `Schema` (safe parsing, INV-NOCAST) into the OWNED, provider-neutral port schemas;
 * GitHub's snake_case wire fields (`default_branch`, `html_url`) are mapped to
 * Sprinter's words at this boundary and never leak upward. Every host failure — an
 * `HttpClientError`, a `Schema.SchemaError`, a GraphQL `errors[]` — is translated
 * into the owned {@link RepositoryError}, so no consumer sees an HTTP or decode type.
 *
 * The adapter is repo-scoped (D14): a {@link RepositoryConfig} binds ONE
 * `owner/repo`; cross-repo work provides many instances.
 */
import { Effect, Layer, Option, Schema } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";
import { PositiveInt, type PullRequestRef } from "@sprinter/domain";
import {
  type CodeOps,
  type IssueOps,
  type PullRequestOps,
  Repository,
  RepositoryError,
  RepositoryIssue,
} from "./repository.ts";

// ============================================================================
// Error mapping — host failures → the owned RepositoryError (INV-PORT)
// ============================================================================

/**
 * Translate any host failure (an `HttpClientError` from the transport / status
 * filter, a `Schema.SchemaError` from decoding) into the owned
 * {@link RepositoryError}. Every such error carries a `message`; that is the only
 * surface this adapter leaks upward, so no consumer ever sees an HTTP or decode
 * type (INV-PORT).
 */
const fail =
  (operation: string) =>
  (error: { readonly message: string }): RepositoryError =>
    new RepositoryError({ operation, detail: error.message });

// ============================================================================
// Wire schemas — GitHub REST JSON (decoded, then mapped to owned port types)
// ============================================================================

/** The `GET /repos/{owner}/{repo}` fields we read: the default branch. */
const GhRepo = Schema.Struct({ default_branch: Schema.NonEmptyString });

/** The `GET /repos/{owner}/{repo}/pulls/{number}` fields we read. */
const GhPull = Schema.Struct({
  number: PositiveInt,
  html_url: Schema.NonEmptyString,
  merged: Schema.Boolean,
});

/**
 * The AUTHORITATIVE closing-PR signal — GitHub GraphQL's
 * `closedByPullRequestsReferences` on an Issue: the PRs GitHub itself records as
 * having CLOSED the Issue (via a closing keyword that merged), NOT merely any PR
 * that references it. This retires the timeline `cross-referenced` heuristic
 * (D18, resolved by CE1.3): an unrelated merged PR that only *mentions* a
 * hand-closed Issue is never returned here, so it can no longer be mis-attributed
 * as the closer.
 *
 * `first: 10` with `includeClosedPrs: true` — the connection can hold MORE than one
 * reference (an Issue closed by an unmerged PR, reopened, then closed/merged by a
 * later PR), and a closed-but-UNMERGED reference can sort ahead of the MERGED one.
 * So we fetch a small page and each node also carries `merged`, and the reconciler's
 * landing signal is "is there a MERGED closing PR among the nodes" — NOT blindly
 * `nodes[0]`. `includeClosedPrs` keeps a closing PR that is itself in a closed state
 * so the merged reference is not filtered away before we can see it.
 *
 * VERSION NOTE (NB1): `closedByPullRequestsReferences` exists only on GitHub.com and
 * on GitHub Enterprise Server at or above the release that shipped it — the field
 * landed on GitHub.com on 2024-07-16. Older GHE hosts do NOT expose it and this
 * closing-PR path is unsupported there; there is no runtime guard because Sprinter
 * targets github.com (D14).
 */
export const CLOSING_PR_QUERY = `query ($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      closedByPullRequestsReferences(first: 10, includeClosedPrs: true) {
        nodes { number merged }
      }
    }
  }
}`;

/**
 * A `closedByPullRequestsReferences` node — a PR that closed the Issue, with the
 * `merged` flag GitHub itself reports so the MERGED closing PR can be selected among
 * several references (NB2) without a second round-trip.
 */
const GhClosingPrNode = Schema.Struct({ number: PositiveInt, merged: Schema.Boolean });

/** A single GraphQL `errors[]` entry — only its human-readable `message`. */
const GhGraphqlError = Schema.Struct({ message: Schema.String });

/**
 * The GraphQL response envelope for {@link CLOSING_PR_QUERY}. GitHub answers a
 * malformed/failed GraphQL query with HTTP 200 and an `errors` array, so this
 * decodes both channels: the `data` path (each level nullable — a missing repo or
 * issue is `null`, not an error) and the top-level `errors`. Excess wire fields are
 * ignored on decode; nothing is cast (INV-NOCAST).
 */
const GhClosingPrResponse = Schema.Struct({
  data: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        repository: Schema.NullOr(
          Schema.Struct({
            issue: Schema.NullOr(
              Schema.Struct({
                closedByPullRequestsReferences: Schema.Struct({
                  nodes: Schema.Array(GhClosingPrNode),
                }),
              }),
            ),
          }),
        ),
      }),
    ),
  ),
  errors: Schema.optionalKey(Schema.Array(GhGraphqlError)),
});

/**
 * Extract the closing PR number from a decoded GraphQL response. A non-empty
 * `errors` array is a real host failure (surfaced as a `message` the caller maps to
 * {@link RepositoryError}); otherwise the MERGED `closedByPullRequestsReferences`
 * node is the authoritative closing PR (NB2 — a closed-but-unmerged reference can
 * precede the merged one, so we select on `merged` rather than taking `nodes[0]`),
 * and the absence of any merged closer (no closing PR, only unmerged references, or
 * a missing repo/issue) is `Option.none`.
 */
const closingPrFromResponse = (
  body: (typeof GhClosingPrResponse)["Type"],
): Effect.Effect<Option.Option<PositiveInt>, { readonly message: string }> => {
  if (body.errors !== undefined && body.errors.length > 0) {
    return Effect.fail({
      message: `GraphQL error: ${body.errors.map((error) => error.message).join("; ")}`,
    });
  }
  const nodes = body.data?.repository?.issue?.closedByPullRequestsReferences.nodes ?? [];
  const merged = nodes.find((node) => node.merged);
  return Effect.succeed(merged === undefined ? Option.none() : Option.some(merged.number));
};

/**
 * Derive the GraphQL endpoint from the REST base URL. GitHub.com's REST base is
 * `https://api.github.com` with GraphQL at `.../graphql`; a GitHub Enterprise host
 * exposes REST at `https://HOST/api/v3` and GraphQL at `https://HOST/api/graphql`.
 * The `Repository` port is unchanged — this is still the GitHub adapter, just
 * addressing the host's GraphQL surface for the authoritative closing-PR signal. A
 * trailing slash on the base (`https://api.github.com/`) is normalized first so the
 * endpoint is `.../graphql`, never `...//graphql` (NB3).
 */
const graphqlEndpoint = (baseUrl: string): string => {
  const normalized = baseUrl.replace(/\/$/, "");
  const enterpriseRest = "/api/v3";
  return normalized.endsWith(enterpriseRest)
    ? `${normalized.slice(0, -enterpriseRest.length)}/api/graphql`
    : `${normalized}/graphql`;
};

// ============================================================================
// Configuration
// ============================================================================

/** Configuration for the GitHub adapter — repo-scoped (D14). */
export interface RepositoryConfig {
  /** The repository owner (user or org), e.g. `"callajd"`. */
  readonly owner: string;
  /** The repository name, e.g. `"sprinter"`. */
  readonly repo: string;
  /**
   * A GitHub token for authenticated requests — REQUIRED (B1, CE1.3). Token-less
   * access is NOT a supported mode: GitHub's GraphQL API rejects ALL unauthenticated
   * requests with 401 (even on public repos), so the authoritative closing-PR
   * signal ({@link CLOSING_PR_QUERY}) cannot work anonymously, and the daemon drives
   * authenticated Issue→PR work regardless. Sent as a `Bearer` credential on every request; never
   * logged. Must be non-empty — the composition root fails fast at boot when
   * `GITHUB_TOKEN` is absent, and {@link make} refuses to construct the adapter with
   * an empty/blank token (a single, universal guard covering EVERY operation) rather
   * than ever sending an empty `Bearer` or silently reporting "no closing PR".
   */
  readonly token: string;
  /**
   * The REST API base URL. Defaults to `https://api.github.com` (GitHub.com); a
   * GitHub Enterprise host overrides it. The port abstraction is unchanged — this
   * is still the GitHub adapter, just against a different base.
   */
  readonly baseUrl?: string;
}

const DEFAULT_BASE_URL = "https://api.github.com";

// ============================================================================
// Service construction
// ============================================================================

/**
 * Build the {@link Repository} implementation over the ambient {@link HttpClient}.
 * Each method issues a parameterised GitHub REST request (base URL + auth/accept
 * headers pre-applied) and decodes the response through `Schema` into the owned
 * port types; all host failures are mapped to {@link RepositoryError} at the
 * boundary.
 */
const make = (config: RepositoryConfig) =>
  Effect.gen(function* () {
    // The token is REQUIRED for EVERY operation (B1, NB3). GitHub's GraphQL endpoint
    // 401s every unauthenticated request, and the REST ops must never carry an empty
    // `Bearer`. So refuse to construct the adapter with an empty/blank token: a single,
    // universal fail-fast with a distinct, loud RepositoryError (never a silent empty
    // credential or `Option.none`), backing up the boot-time fail-fast in the daemon
    // composition root. The token itself is never logged.
    if (config.token.trim() === "") {
      return yield* Effect.fail(
        new RepositoryError({
          operation: "make",
          detail:
            "a GitHub token is required (set GITHUB_TOKEN); GitHub's GraphQL API rejects unauthenticated requests with 401",
        }),
      );
    }
    const base = yield* HttpClient.HttpClient;
    const baseUrl = config.baseUrl ?? DEFAULT_BASE_URL;
    // Token verified non-empty above — always authenticate; token-less is not a
    // supported mode.
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      Authorization: `Bearer ${config.token}`,
    };
    const client = base.pipe(
      HttpClient.mapRequest((request) =>
        request.pipe(HttpClientRequest.prependUrl(baseUrl), HttpClientRequest.setHeaders(headers)),
      ),
    );

    const repoPath = `/repos/${config.owner}/${config.repo}`;
    // The GraphQL POST addresses an ABSOLUTE endpoint (github.com vs. GHE differ),
    // so it goes through the raw `base` client — not `client`, which prepends the
    // REST base — with the same auth/accept headers applied per request.
    const graphqlUrl = graphqlEndpoint(baseUrl);

    const code: CodeOps = {
      defaultBranch: client.execute(HttpClientRequest.get(repoPath)).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap((response) => response.json),
        Effect.flatMap(Schema.decodeUnknownEffect(GhRepo)),
        Effect.map((repo) => repo.default_branch),
        Effect.mapError(fail("defaultBranch")),
      ),
      branchExists: (name) =>
        client
          .execute(HttpClientRequest.get(`${repoPath}/branches/${encodeURIComponent(name)}`))
          .pipe(
            Effect.mapError(fail("branchExists")),
            Effect.flatMap(
              HttpClientResponse.matchStatus({
                200: () => Effect.succeed(true),
                404: () => Effect.succeed(false),
                orElse: (response) =>
                  Effect.fail(
                    new RepositoryError({
                      operation: "branchExists",
                      detail: `unexpected status ${response.status}`,
                    }),
                  ),
              }),
            ),
          ),
    };

    const issues: IssueOps = {
      getIssue: (number) =>
        client.execute(HttpClientRequest.get(`${repoPath}/issues/${number}`)).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap((response) => response.json),
          Effect.flatMap(Schema.decodeUnknownEffect(RepositoryIssue)),
          Effect.mapError(fail("getIssue")),
        ),
    };

    const pullRequests: PullRequestOps = {
      closingPullRequest: (issueNumber) =>
        // The token is guaranteed non-empty (universal guard in `make`, NB3), so the
        // authenticated GraphQL POST always carries a real `Bearer`; a token-less
        // adapter can never be constructed to reach here.
        base
          .execute(
            HttpClientRequest.post(graphqlUrl).pipe(
              HttpClientRequest.setHeaders(headers),
              HttpClientRequest.bodyJsonUnsafe({
                query: CLOSING_PR_QUERY,
                variables: { owner: config.owner, repo: config.repo, number: issueNumber },
              }),
            ),
          )
          .pipe(
            Effect.flatMap(HttpClientResponse.filterStatusOk),
            Effect.flatMap((response) => response.json),
            Effect.flatMap(Schema.decodeUnknownEffect(GhClosingPrResponse)),
            Effect.flatMap(closingPrFromResponse),
            Effect.mapError(fail("closingPullRequest")),
          ),
      getPullRequest: (number) =>
        client.execute(HttpClientRequest.get(`${repoPath}/pulls/${number}`)).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap((response) => response.json),
          Effect.flatMap(Schema.decodeUnknownEffect(GhPull)),
          Effect.map(
            (pull): PullRequestRef => ({
              number: pull.number,
              url: pull.html_url,
              merged: pull.merged,
            }),
          ),
          Effect.mapError(fail("getPullRequest")),
        ),
    };

    return Repository.of({ code, issues, pullRequests });
  });

// ============================================================================
// Adapter layers
// ============================================================================

/**
 * The GitHub adapter for {@link Repository}, requiring an ambient
 * {@link HttpClient} — the seam a test provides a fake HTTP backend into (a fake
 * `Fetch` under {@link FetchHttpClient}), and production satisfies with
 * {@link layerFetch}. Exposes no HTTP type on its public shape beyond the standard
 * `HttpClient` requirement (INV-PORT).
 */
export const layer = (
  config: RepositoryConfig,
): Layer.Layer<Repository, RepositoryError, HttpClient.HttpClient> =>
  Layer.effect(Repository, make(config));

/**
 * The self-contained GitHub adapter: {@link layer} with the Bun-native fetch
 * transport ({@link FetchHttpClient.layer}, `globalThis.fetch`) provided. This is
 * the production wiring — a `Layer<Repository>` with the transport sealed inside,
 * exposing no HTTP type (INV-PORT).
 */
export const layerFetch = (config: RepositoryConfig): Layer.Layer<Repository, RepositoryError> =>
  layer(config).pipe(Layer.provide(FetchHttpClient.layer));
