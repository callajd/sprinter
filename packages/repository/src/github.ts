/**
 * The GitHub ADAPTER behind the {@link Repository} port (Track A, task AE3.2).
 *
 * This is the ONLY module in the codebase permitted to reference the code host
 * concretely (INV-PORT / D5 / D14): GitHub's REST base URL, its resource paths,
 * and its wire JSON. Nothing above it — no consumer, no reconciler — imports a
 * GitHub client, a URL, or `gh`; they build to the port ({@link ./repository.ts})
 * and this instance is provided behind a `Layer`.
 *
 * The transport is Effect-native and Bun-native (INV: Bun + Effect, never Node):
 * `effect/unstable/http` `HttpClient` over {@link FetchHttpClient} (`globalThis.fetch`,
 * which Bun implements) — NO Node-only SDK, no `node:*`. Every response is decoded
 * through `Schema` (safe parsing, INV-NOCAST) into the OWNED, provider-neutral port
 * schemas; GitHub's snake_case wire fields (`default_branch`, `html_url`,
 * `pull_request`) are mapped to Sprinter's words at this boundary and never leak
 * upward. Every host failure — an `HttpClientError`, a `Schema.SchemaError` — is
 * translated into the owned {@link RepositoryError}, so no consumer sees an HTTP or
 * decode type.
 *
 * The adapter is repo-scoped (D14): a {@link RepositoryConfig} binds ONE
 * `owner/repo`; cross-repo work provides many instances.
 */
import { Effect, Layer, Option, Schema } from "effect";
import {
  FetchHttpClient,
  Headers,
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
 * A `GET /repos/{owner}/{repo}/issues/{number}/timeline` entry — only the fields
 * that identify a PR *referencing* the Issue. Excess wire fields are ignored on
 * decode. A `cross-referenced` event whose `source` issue carries a `pull_request`
 * is a referencing PR.
 */
const GhTimelineEvent = Schema.Struct({
  event: Schema.String,
  source: Schema.optionalKey(
    Schema.Struct({
      issue: Schema.optionalKey(
        Schema.Struct({
          number: PositiveInt,
          pull_request: Schema.optionalKey(Schema.Unknown),
        }),
      ),
    }),
  ),
});

/**
 * The first PR that cross-references the Issue in its timeline, if any — a
 * HEURISTIC for the closing PR, not a guarantee: GitHub emits `cross-referenced`
 * whenever an Issue is merely mentioned, so this can pick a PR that references but
 * does not close the Issue. The caller (`reconcileIssue`) further gates on the
 * Issue being closed AND the referenced PR being merged, which is sufficient for
 * the common case. Keeping this offline heuristic (gated by closed + merged) with
 * its residual risk documented — vs. a robust GraphQL
 * `closedByPullRequestsReferences` / `closed`-event signal — is a resolved decision
 * (D18); the robust live-wiring signal is tracked as deferred provisioning.
 */
const findClosingPr = (
  events: ReadonlyArray<(typeof GhTimelineEvent)["Type"]>,
): Option.Option<PositiveInt> => {
  for (const event of events) {
    if (event.event !== "cross-referenced") continue;
    const issue = event.source?.issue;
    if (issue !== undefined && issue.pull_request !== undefined) {
      return Option.some(issue.number);
    }
  }
  return Option.none();
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
   * A GitHub token for authenticated requests. Omit for anonymous access (public
   * reads). Sent as a `Bearer` credential; never logged.
   */
  readonly token?: string;
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
    const base = yield* HttpClient.HttpClient;
    const baseUrl = config.baseUrl ?? DEFAULT_BASE_URL;
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      ...(config.token !== undefined ? { Authorization: `Bearer ${config.token}` } : {}),
    };
    const client = base.pipe(
      HttpClient.mapRequest((request) =>
        request.pipe(HttpClientRequest.prependUrl(baseUrl), HttpClientRequest.setHeaders(headers)),
      ),
    );

    const repoPath = `/repos/${config.owner}/${config.repo}`;

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
        Effect.gen(function* () {
          // The timeline is paginated (30/page by default, and comments/labels/
          // reviews all consume slots), so a single page can omit the closing PR's
          // cross-reference. Request 100/page and follow `Link: rel="next"` to the
          // end, accumulating every event before scanning for the referencing PR.
          const collected: Array<(typeof GhTimelineEvent)["Type"]> = [];
          let page = 1;
          let more = true;
          while (more) {
            const response = yield* client
              .execute(
                HttpClientRequest.get(
                  `${repoPath}/issues/${issueNumber}/timeline?per_page=100&page=${page}`,
                ),
              )
              .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
            const json = yield* response.json;
            const events = yield* Schema.decodeUnknownEffect(Schema.Array(GhTimelineEvent))(json);
            collected.push(...events);
            const link = Headers.get(response.headers, "link");
            more = events.length > 0 && Option.isSome(link) && link.value.includes('rel="next"');
            page += 1;
          }
          return findClosingPr(collected);
        }).pipe(Effect.mapError(fail("closingPullRequest"))),
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
): Layer.Layer<Repository, never, HttpClient.HttpClient> => Layer.effect(Repository, make(config));

/**
 * The self-contained GitHub adapter: {@link layer} with the Bun-native fetch
 * transport ({@link FetchHttpClient.layer}, `globalThis.fetch`) provided. This is
 * the production wiring — a `Layer<Repository>` with the transport sealed inside,
 * exposing no HTTP type (INV-PORT).
 */
export const layerFetch = (config: RepositoryConfig): Layer.Layer<Repository> =>
  layer(config).pipe(Layer.provide(FetchHttpClient.layer));
