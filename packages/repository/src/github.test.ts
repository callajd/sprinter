/**
 * GitHub adapter coverage (AE3.2) — the {@link Repository} port exercised THROUGH
 * the GitHub adapter against a FAKE HTTP backend (a fake `Fetch` under
 * {@link FetchHttpClient}). Deterministic and OFFLINE — no live GitHub call in the
 * gate (INV-GATE): every request is answered by a canned in-memory route table.
 *
 * The suite proves the adapter issues the right GitHub REST paths AND the GraphQL
 * closing-PR query (`closedByPullRequestsReferences`), decodes each wire response
 * through `Schema` into the OWNED port types (never a cast, INV-NOCAST), and
 * translates every host failure — an HTTP error, a decode error, a GraphQL
 * `errors[]` — into the owned {@link RepositoryError} (INV-PORT). The tests depend
 * ONLY on the package's public surface plus the standard `FetchHttpClient` transport
 * seam — never a concrete GitHub client.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { expect } from "vitest";
import { PositiveInt } from "@sprinter/domain";
import { layer, layerFetch, Repository, RepositoryError } from "./index.ts";

// ============================================================================
// Fake HTTP backend — a canned route table over a fake `Fetch`
// ============================================================================

type Route = () => Response;

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const makeFetch = (routes: Record<string, Route>): typeof globalThis.fetch =>
  Object.assign(
    (input: string | URL | Request, _init?: RequestInit): Promise<Response> => {
      const href = input instanceof URL ? input.href : input instanceof Request ? input.url : input;
      const path = new URL(href).pathname;
      const route = routes[path];
      return Promise.resolve(
        route === undefined ? new Response("not found", { status: 404 }) : route(),
      );
    },
    { preconnect: globalThis.fetch.preconnect },
  );

const config = { owner: "callajd", repo: "sprinter", token: "test-token" };

const backend = (routes: Record<string, Route>): Layer.Layer<Repository> =>
  layer(config).pipe(
    Layer.provide(
      FetchHttpClient.layer.pipe(
        Layer.provide(Layer.succeed(FetchHttpClient.Fetch, makeFetch(routes))),
      ),
    ),
  );

const num = (n: number): PositiveInt => Schema.decodeUnknownSync(PositiveInt)(n);

// ============================================================================
// IssueOps
// ============================================================================

it.effect("reads an Issue's host state (open / closed)", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const issue = yield* repo.issues.getIssue(num(25));
    expect(issue).toStrictEqual({
      number: 25,
      title: "AE3.2 — Repository port + GitHub adapter",
      state: "closed",
    });
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25": () =>
          json({
            number: 25,
            title: "AE3.2 — Repository port + GitHub adapter",
            state: "closed",
            body: "ignored excess field",
          }),
      }),
    ),
  ),
);

// ============================================================================
// CodeOps
// ============================================================================

it.effect("reads the default branch and detects branch existence (present / absent)", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    expect(yield* repo.code.defaultBranch).toBe("main");
    expect(yield* repo.code.branchExists("main")).toBe(true);
    expect(yield* repo.code.branchExists("feat/does-not-exist")).toBe(false);
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter": () => json({ default_branch: "main" }),
        "/repos/callajd/sprinter/branches/main": () => json({ name: "main" }),
      }),
    ),
  ),
);

it.effect("surfaces an unexpected branch status as RepositoryError", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const error = yield* repo.code.branchExists("main").pipe(Effect.flip);
    expect(error).toBeInstanceOf(RepositoryError);
    expect(error.operation).toBe("branchExists");
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/branches/main": () => json({ message: "boom" }, 500),
      }),
    ),
  ),
);

// ============================================================================
// PullRequestOps
// ============================================================================

it.effect("reads a PR and reports whether it merged", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const pr = yield* repo.pullRequests.getPullRequest(num(42));
    expect(pr).toStrictEqual({
      number: 42,
      url: "https://github.com/callajd/sprinter/pull/42",
      merged: true,
    });
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/pulls/42": () =>
          json({
            number: 42,
            html_url: "https://github.com/callajd/sprinter/pull/42",
            merged: true,
            state: "closed",
          }),
      }),
    ),
  ),
);

/** One `closedByPullRequestsReferences` node — a closing PR and whether it merged. */
interface ClosingNode {
  readonly number: number;
  readonly merged: boolean;
}

/** A GraphQL `closedByPullRequestsReferences` response with the given nodes (in order). */
const gqlNodes =
  (...nodes: ReadonlyArray<ClosingNode>): Route =>
  () =>
    json({
      data: {
        repository: { issue: { closedByPullRequestsReferences: { nodes } } },
      },
    });

/** A GraphQL response whose (single) closing PR is MERGED — the common landed case. */
const gqlClosing =
  (...numbers: ReadonlyArray<number>): Route =>
  () =>
    gqlNodes(...numbers.map((number) => ({ number, merged: true })))();

it.effect("detects the PR that CLOSED an Issue via GraphQL closedByPullRequestsReferences", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(42));
  }).pipe(Effect.provide(backend({ "/graphql": gqlClosing(42) }))),
);

it.effect(
  "does NOT attribute an unrelated PR that merely mentions a hand-closed Issue as the closer",
  () =>
    // The authoritative signal (`closedByPullRequestsReferences`) reports NO closing
    // PR for an Issue that was hand-closed while a different merged PR only mentions
    // it — where the retired timeline `cross-referenced` heuristic false-positived,
    // GraphQL returns an empty node set, so the reconciler never mis-lands (D18/CE1.3).
    Effect.gen(function* () {
      const repo = yield* Repository;
      const found = yield* repo.pullRequests.closingPullRequest(num(25));
      expect(found).toStrictEqual(Option.none());
    }).pipe(Effect.provide(backend({ "/graphql": gqlClosing() }))),
);

it.effect("reports no closing PR when the host has no record of the Issue (null issue)", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.none());
  }).pipe(
    Effect.provide(backend({ "/graphql": () => json({ data: { repository: { issue: null } } }) })),
  ),
);

it.effect("surfaces a GraphQL errors[] response as RepositoryError", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const error = yield* repo.pullRequests.closingPullRequest(num(25)).pipe(Effect.flip);
    expect(error).toBeInstanceOf(RepositoryError);
    expect(error.operation).toBe("closingPullRequest");
    expect(error.detail).toContain("rate limited");
  }).pipe(
    Effect.provide(backend({ "/graphql": () => json({ errors: [{ message: "rate limited" }] }) })),
  ),
);

it.effect("addresses the GraphQL endpoint for a GitHub Enterprise base URL", () =>
  // GHE exposes REST at `/api/v3` and GraphQL at `/api/graphql`; the adapter must
  // POST the closing-PR query to the latter, not `baseUrl + /graphql`.
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(7));
  }).pipe(
    Effect.provide(
      layer({ ...config, baseUrl: "https://ghe.example/api/v3" }).pipe(
        Layer.provide(
          FetchHttpClient.layer.pipe(
            Layer.provide(
              Layer.succeed(FetchHttpClient.Fetch, makeFetch({ "/api/graphql": gqlClosing(7) })),
            ),
          ),
        ),
      ),
    ),
  ),
);

it.effect(
  "selects the MERGED closing PR when a closed-but-unmerged reference precedes it (NB2)",
  () =>
    // An Issue closed by an unmerged PR, reopened, then closed/merged by a later PR:
    // the connection holds the UNMERGED reference ahead of the MERGED one. Taking
    // `nodes[0]` + a merged-gate would wrongly report not-landed; the adapter must
    // select the merged reference among the page.
    Effect.gen(function* () {
      const repo = yield* Repository;
      const found = yield* repo.pullRequests.closingPullRequest(num(25));
      expect(found).toStrictEqual(Option.some(99));
    }).pipe(
      Effect.provide(
        backend({
          "/graphql": gqlNodes({ number: 41, merged: false }, { number: 99, merged: true }),
        }),
      ),
    ),
);

it.effect("reports no closing PR when every closing reference is unmerged (NB2)", () =>
  // Only closed-but-unmerged references exist — nothing landed, so `Option.none`.
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.none());
  }).pipe(Effect.provide(backend({ "/graphql": gqlNodes({ number: 41, merged: false }) }))),
);

it.effect("normalizes a trailing slash on the base URL so the endpoint is /graphql (NB3)", () =>
  // A base URL ending in `/` must yield `.../graphql`, never `...//graphql`.
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(42));
  }).pipe(
    Effect.provide(
      layer({ ...config, baseUrl: "https://api.github.com/" }).pipe(
        Layer.provide(
          FetchHttpClient.layer.pipe(
            Layer.provide(
              Layer.succeed(FetchHttpClient.Fetch, makeFetch({ "/graphql": gqlClosing(42) })),
            ),
          ),
        ),
      ),
    ),
  ),
);

it.effect(
  "surfaces a 401 from the GraphQL endpoint as a loud RepositoryError, not Option.none (B1)",
  () =>
    // GitHub 401s an unauthenticated/invalid-token GraphQL request. That must be a
    // DISTINCT, loud failure the reconciler can see — never masked as "no closing PR".
    Effect.gen(function* () {
      const repo = yield* Repository;
      const error = yield* repo.pullRequests.closingPullRequest(num(25)).pipe(Effect.flip);
      expect(error).toBeInstanceOf(RepositoryError);
      expect(error.operation).toBe("closingPullRequest");
    }).pipe(
      Effect.provide(
        backend({
          "/graphql": () => json({ message: "Requires authentication" }, 401),
        }),
      ),
    ),
);

it.effect("rejects a token-less adapter with a distinct, loud error, not Option.none (B1)", () =>
  // Belt-and-suspenders: an empty token can never observe a closing PR (GitHub 401s
  // it), so `closingPullRequest` refuses loudly rather than silently reporting none.
  Effect.gen(function* () {
    const repo = yield* Repository;
    const error = yield* repo.pullRequests.closingPullRequest(num(25)).pipe(Effect.flip);
    expect(error).toBeInstanceOf(RepositoryError);
    expect(error.operation).toBe("closingPullRequest");
    expect(error.detail).toContain("token is required");
  }).pipe(
    Effect.provide(
      layer({ ...config, token: "" }).pipe(
        Layer.provide(
          FetchHttpClient.layer.pipe(
            Layer.provide(Layer.succeed(FetchHttpClient.Fetch, makeFetch({}))),
          ),
        ),
      ),
    ),
  ),
);

// ============================================================================
// Error translation (INV-PORT) & production wiring
// ============================================================================

it.effect("translates a non-2xx host response into RepositoryError", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const error = yield* repo.issues.getIssue(num(25)).pipe(Effect.flip);
    expect(error).toBeInstanceOf(RepositoryError);
    expect(error.operation).toBe("getIssue");
    expect(error.detail.length).toBeGreaterThan(0);
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25": () => json({ message: "server error" }, 500),
      }),
    ),
  ),
);

it.effect("layerFetch builds a self-contained adapter (Bun-native fetch transport)", () =>
  Effect.gen(function* () {
    // Building the production layer exercises `layerFetch`; it needs no ambient
    // HttpClient (the transport is sealed inside), which we assert by type: the
    // layer is provided with nothing else and yields the port.
    const authed = layerFetch({ ...config, token: "t0ken", baseUrl: "https://ghe.example/api/v3" });
    const repo = yield* Repository.pipe(Effect.provide(authed));
    expect(typeof repo.issues.getIssue).toBe("function");
  }),
);

it.effect("the transport seam is satisfiable by a fake HttpClient", () =>
  // A pure type/coverage witness that `layer` requires only HttpClient — provided
  // here by the fetch transport over a fake backend — no live network.
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    expect(typeof client.execute).toBe("function");
  }).pipe(
    Effect.provide(
      FetchHttpClient.layer.pipe(
        Layer.provide(Layer.succeed(FetchHttpClient.Fetch, makeFetch({}))),
      ),
    ),
  ),
);
