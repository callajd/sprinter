/**
 * GitHub adapter coverage (AE3.2) — the {@link CodeHost} port exercised THROUGH
 * the GitHub adapter against a FAKE HTTP backend (a fake `Fetch` under
 * {@link FetchHttpClient}). Deterministic and OFFLINE — no live GitHub call in the
 * gate (INV-GATE): every request is answered by a canned in-memory route table.
 *
 * The suite proves the adapter issues the right GitHub REST paths AND the GraphQL
 * closing-PR query (`closedByPullRequestsReferences`), decodes each wire response
 * through `Schema` into the OWNED port types (never a cast, INV-NOCAST), and
 * translates every host failure — an HTTP error, a decode error, a GraphQL
 * `errors[]` — into the owned {@link CodeHostError} (INV-PORT). The tests depend
 * ONLY on the package's public surface plus the standard `FetchHttpClient` transport
 * seam — never a concrete GitHub client.
 */
import { it } from "@effect/vitest";
import { Effect, Layer, Option, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { expect } from "vitest";
import { PositiveInt } from "@sprinter/domain";
import { CLOSING_PR_QUERY } from "./github.ts";
import {
  CodeHost,
  CodeHostError,
  hostInstant,
  layer,
  layerFetch,
  repositoryIdFor,
} from "./index.ts";

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

const backend = (routes: Record<string, Route>): Layer.Layer<CodeHost, CodeHostError> =>
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
    const repo = yield* CodeHost;
    const issue = yield* repo.issues.getIssue(num(25));
    expect(issue).toStrictEqual({
      number: 25,
      title: "AE3.2 — CodeHost port + GitHub adapter",
      state: "closed",
    });
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25": () =>
          json({
            number: 25,
            title: "AE3.2 — CodeHost port + GitHub adapter",
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
    const repo = yield* CodeHost;
    expect(yield* repo.code.defaultBranch).toBe("main");
    expect(yield* repo.code.branchExists("main")).toBe(true);
    expect(yield* repo.code.branchExists("feat/does-not-exist")).toBe(false);
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter": () =>
          json({ default_branch: "main", name: "sprinter", owner: { login: "callajd" } }),
        "/repos/callajd/sprinter/branches/main": () => json({ name: "main" }),
      }),
    ),
  ),
);

it.effect("surfaces an unexpected branch status as CodeHostError", () =>
  Effect.gen(function* () {
    const repo = yield* CodeHost;
    const error = yield* repo.code.branchExists("main").pipe(Effect.flip);
    expect(error).toBeInstanceOf(CodeHostError);
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
    const repo = yield* CodeHost;
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
    const repo = yield* CodeHost;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(42));
  }).pipe(Effect.provide(backend({ "/graphql": gqlClosing(42) }))),
);

/**
 * The GraphQL request body the adapter must send for {@link CLOSING_PR_QUERY} —
 * decoded straight from the captured JSON string (no cast, INV-NOCAST): the query
 * text plus the owner/repo/issue-number variables.
 */
const GraphqlRequestBody = Schema.fromJsonString(
  Schema.Struct({
    query: Schema.String,
    variables: Schema.Struct({
      owner: Schema.String,
      repo: Schema.String,
      number: Schema.Number,
    }),
  }),
);

/** What a single captured outbound `fetch` call carried (method / auth / body). */
interface CapturedRequest {
  readonly method: string;
  readonly authorization: string | null;
  readonly body: string | undefined;
}

/**
 * Normalize a `fetch` body to its JSON text. The transport encodes a JSON body as a
 * UTF-8 `Uint8Array`; a raw string is decoded as-is. Anything else yields `undefined`
 * (a missing body) — no cast, INV-NOCAST.
 */
const bodyText = (body: RequestInit["body"]): string | undefined =>
  typeof body === "string"
    ? body
    : body instanceof Uint8Array
      ? new TextDecoder().decode(body)
      : undefined;

it.effect(
  "sends the closing-PR query as an authenticated POST with the expected variables (NB2)",
  () =>
    // Locks the HEADLINE security behavior: the GraphQL closing-PR request must be a
    // POST that carries `Authorization: Bearer <token>` and the exact CLOSING_PR_QUERY
    // with the owner/repo/issue variables. Assert method + header + body — not merely
    // the pathname — so a regression that drops the token or garbles the query is caught.
    Effect.gen(function* () {
      let seen: CapturedRequest | undefined;
      const capturingFetch: typeof globalThis.fetch = Object.assign(
        (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
          seen = {
            method: init?.method ?? "GET",
            authorization: new Headers(init?.headers).get("authorization"),
            body: bodyText(init?.body),
          };
          return Promise.resolve(gqlClosing(42)());
        },
        { preconnect: globalThis.fetch.preconnect },
      );
      const repo = yield* CodeHost.pipe(
        Effect.provide(
          layer(config).pipe(
            Layer.provide(
              FetchHttpClient.layer.pipe(
                Layer.provide(Layer.succeed(FetchHttpClient.Fetch, capturingFetch)),
              ),
            ),
          ),
        ),
      );
      const found = yield* repo.pullRequests.closingPullRequest(num(25));
      expect(found).toStrictEqual(Option.some(42));
      expect(seen).toBeDefined();
      expect(seen?.method).toBe("POST");
      expect(seen?.authorization).toBe("Bearer test-token");
      const decoded = Schema.decodeUnknownSync(GraphqlRequestBody)(seen?.body);
      expect(decoded.query).toBe(CLOSING_PR_QUERY);
      expect(decoded.variables).toStrictEqual({ owner: "callajd", repo: "sprinter", number: 25 });
    }),
);

it.effect(
  "does NOT attribute an unrelated PR that merely mentions a hand-closed Issue as the closer",
  () =>
    // The authoritative signal (`closedByPullRequestsReferences`) reports NO closing
    // PR for an Issue that was hand-closed while a different merged PR only mentions
    // it — where the retired timeline `cross-referenced` heuristic false-positived,
    // GraphQL returns an empty node set, so the reconciler never mis-lands (D18/CE1.3).
    Effect.gen(function* () {
      const repo = yield* CodeHost;
      const found = yield* repo.pullRequests.closingPullRequest(num(25));
      expect(found).toStrictEqual(Option.none());
    }).pipe(Effect.provide(backend({ "/graphql": gqlClosing() }))),
);

it.effect("reports no closing PR when the host has no record of the Issue (null issue)", () =>
  Effect.gen(function* () {
    const repo = yield* CodeHost;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.none());
  }).pipe(
    Effect.provide(backend({ "/graphql": () => json({ data: { repository: { issue: null } } }) })),
  ),
);

it.effect("surfaces a GraphQL errors[] response as CodeHostError", () =>
  Effect.gen(function* () {
    const repo = yield* CodeHost;
    const error = yield* repo.pullRequests.closingPullRequest(num(25)).pipe(Effect.flip);
    expect(error).toBeInstanceOf(CodeHostError);
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
    const repo = yield* CodeHost;
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
      const repo = yield* CodeHost;
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
    const repo = yield* CodeHost;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.none());
  }).pipe(Effect.provide(backend({ "/graphql": gqlNodes({ number: 41, merged: false }) }))),
);

it.effect("normalizes a trailing slash on the base URL so the endpoint is /graphql (NB3)", () =>
  // A base URL ending in `/` must yield `.../graphql`, never `...//graphql`.
  Effect.gen(function* () {
    const repo = yield* CodeHost;
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
  "surfaces a 401 from the GraphQL endpoint as a loud CodeHostError, not Option.none (B1)",
  () =>
    // GitHub 401s an unauthenticated/invalid-token GraphQL request. That must be a
    // DISTINCT, loud failure the reconciler can see — never masked as "no closing PR".
    Effect.gen(function* () {
      const repo = yield* CodeHost;
      const error = yield* repo.pullRequests.closingPullRequest(num(25)).pipe(Effect.flip);
      expect(error).toBeInstanceOf(CodeHostError);
      expect(error.operation).toBe("closingPullRequest");
    }).pipe(
      Effect.provide(
        backend({
          "/graphql": () => json({ message: "Requires authentication" }, 401),
        }),
      ),
    ),
);

it.effect("refuses to construct a token-less adapter with a distinct, loud error (NB3)", () =>
  // Universal guard (NB3): an empty/blank token can never authenticate ANY operation
  // (GitHub 401s it), and the REST ops must never carry an empty `Bearer`. So the
  // adapter refuses to build at all — one construction-time fail-fast covering every
  // op — rather than a per-op guard, and never silently reports "no closing PR".
  Effect.gen(function* () {
    const error = yield* CodeHost.pipe(
      Effect.provide(
        layer({ ...config, token: "" }).pipe(
          Layer.provide(
            FetchHttpClient.layer.pipe(
              Layer.provide(Layer.succeed(FetchHttpClient.Fetch, makeFetch({}))),
            ),
          ),
        ),
      ),
      Effect.flip,
    );
    expect(error).toBeInstanceOf(CodeHostError);
    expect(error.detail).toContain("token is required");
  }),
);

// ============================================================================
// Error translation (INV-PORT) & production wiring
// ============================================================================

it.effect("translates a non-2xx host response into CodeHostError", () =>
  Effect.gen(function* () {
    const repo = yield* CodeHost;
    const error = yield* repo.issues.getIssue(num(25)).pipe(Effect.flip);
    expect(error).toBeInstanceOf(CodeHostError);
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
    const repo = yield* CodeHost.pipe(Effect.provide(authed));
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

// ============================================================================
// RepositoryOps — observing the owned `Repository` entity (DE1.2)
// ============================================================================

const KEY = { host: "github", owner: "callajd", name: "sprinter" } as const;
const SHA_MAIN = "0123456789abcdef0123456789abcdef01234567";
const SHA_FEAT = "89abcdef0123456789abcdef0123456789abcdef";

/**
 * A canned repository route pair (the repo itself + its branches), optionally under a
 * host `Date` response header — the instant the adapter reads `observedAt` from.
 *
 * The repo body carries the CANONICAL identity (`owner.login` + `name`) because that is
 * what the adapter builds the record from — GitHub's lookup is case-insensitive and
 * follows renames, so the caller's spelling is only ever the question.
 */
const repoRoutes = (date?: string): Record<string, Route> => ({
  "/repos/callajd/sprinter": () =>
    new Response(
      JSON.stringify({ default_branch: "main", name: "sprinter", owner: { login: "callajd" } }),
      {
        status: 200,
        headers: {
          "content-type": "application/json",
          ...(date === undefined ? {} : { date }),
        },
      },
    ),
  "/repos/callajd/sprinter/branches": () =>
    json([
      { name: "main", commit: { sha: SHA_MAIN } },
      { name: "feat/x-1", commit: { sha: SHA_FEAT } },
    ]),
});

it.effect("resolves a repository into the owned entity, refs and all", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const resolved = Option.getOrThrow(yield* host.repositories.resolve(KEY));
    expect(resolved.host).toBe("github");
    expect(resolved.owner).toBe("callajd");
    expect(resolved.name).toBe("sprinter");
    // The observed refs are ORDERED BY NAME, whatever order the host paginated them
    // in, so one observation has one spelling.
    expect(resolved.refs).toStrictEqual([
      { name: "feat/x-1", sha: SHA_FEAT },
      { name: "main", sha: SHA_MAIN },
    ]);
    // The id is a FUNCTION of the natural key — a refresh must land on the same row,
    // not collide with it (the store holds the triple UNIQUE).
    expect(resolved.id).toBe(yield* repositoryIdFor(KEY));
    expect(resolved.observedAt).toBe("2026-07-20T12:00:00.000Z");
  }).pipe(Effect.provide(backend(repoRoutes("Mon, 20 Jul 2026 12:00:00 GMT")))),
);

// B3 — the natural key stored is the HOST's spelling, never the caller's. GitHub's
// lookup is case-INSENSITIVE, so `CallaJD/Sprinter` and `callajd/sprinter` both answer
// 200 for ONE repository. Building the record from the caller's key would mint one id
// and one row per spelling — two anchors for one repository, invisible to the store's
// `UNIQUE (host, owner, name)` because the triples genuinely differ. That is verbatim
// the failure the entity exists to eliminate.
it.effect("canonicalises the natural key — two spellings converge on ONE id and ONE record", () =>
  Effect.gen(function* () {
    const resolve = (key: { host: "github"; owner: string; name: string }) =>
      Effect.gen(function* () {
        const host = yield* CodeHost;
        return Option.getOrThrow(yield* host.repositories.resolve(key));
      }).pipe(
        Effect.provide(
          backend({
            // The host answers the MIS-CASED path too, with its own spelling in the body.
            "/repos/CallaJD/Sprinter": () =>
              json({ default_branch: "main", name: "sprinter", owner: { login: "callajd" } }),
            ...repoRoutes("Mon, 20 Jul 2026 12:00:00 GMT"),
          }),
        ),
      );

    const asked = yield* resolve({ host: "github", owner: "CallaJD", name: "Sprinter" });
    const canonical = yield* resolve(KEY);
    // ONE id, and ONE natural key — so ONE row, and the UNIQUE constraint can see it.
    expect(asked.id).toBe(canonical.id);
    expect(asked.owner).toBe("callajd");
    expect(asked.name).toBe("sprinter");
    expect(asked.refs).toStrictEqual(canonical.refs);
  }),
);

// The same mechanism handles a RENAME for free: GitHub 301-redirects an old path to the
// repository's current one and `fetch` follows it, so the 200 that comes back describes
// a repository with a DIFFERENT name than the one asked for. Only the body can say so.
it.effect("follows a RENAME to the canonical name (the redirect target's spelling)", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const resolved = Option.getOrThrow(
      yield* host.repositories.resolve({ host: "github", owner: "callajd", name: "old-name" }),
    );
    expect(resolved.name).toBe("sprinter");
    expect(resolved.id).toBe(yield* repositoryIdFor(KEY));
    // The refs were read from the CANONICAL path, so they are the renamed repository's.
    expect(resolved.refs.map((ref) => ref.name)).toStrictEqual(["feat/x-1", "main"]);
  }).pipe(
    Effect.provide(
      backend({
        // What a followed 301 looks like to the client: a 200 whose body names the
        // repository's CURRENT path.
        "/repos/callajd/old-name": () =>
          json({ default_branch: "main", name: "sprinter", owner: { login: "callajd" } }),
        ...repoRoutes("Mon, 20 Jul 2026 12:00:00 GMT"),
      }),
    ),
  ),
);

// N2 — `resolve`'s non-200/non-404 branch. 404 is INFORMATION (below); every other
// unexpected status is the host failing to answer, which is the owned `CodeHostError`.
it.effect("surfaces an unexpected resolve status as CodeHostError", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const error = yield* host.repositories.resolve(KEY).pipe(Effect.flip);
    expect(error).toBeInstanceOf(CodeHostError);
    expect(error.operation).toBe("resolve");
    expect(error.detail).toContain("500");
  }).pipe(
    Effect.provide(backend({ "/repos/callajd/sprinter": () => json({ message: "boom" }, 500) })),
  ),
);

// N2 — the `Date`-header fallback. A host that omits the optional header must not cost
// us the observation: `observedAt` falls back to THIS process's clock, which is
// canonical by construction (`toISOString`), so the record still carries a real instant.
it.effect("falls back to this process's clock when the host sends no Date header", () =>
  Effect.gen(function* () {
    const before = new Date().toISOString();
    const host = yield* CodeHost;
    const resolved = Option.getOrThrow(yield* host.repositories.resolve(KEY));
    const after = new Date().toISOString();
    expect(resolved.observedAt >= before && resolved.observedAt <= after).toBe(true);
  }).pipe(Effect.provide(backend(repoRoutes()))),
);

// `Option.none`, not a failure: "no such repository" is INFORMATION the host has,
// where a `CodeHostError` means the host could not be asked. The daemon turns the
// former into a `PlanRejected` that writes nothing and the latter into a defect.
it.effect("answers Option.none for a repository the host does not know", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    expect(yield* host.repositories.resolve(KEY)).toStrictEqual(Option.none());
  }).pipe(Effect.provide(backend({}))),
);

// D7 — a second observation REPLACES the record and ADVANCES `observedAt`. The port
// has no partial/merge variant: a refresh is just another resolve.
it.effect("a REFRESH is a new complete observation with a later observedAt (D7)", () =>
  Effect.gen(function* () {
    const first = yield* Effect.gen(function* () {
      const host = yield* CodeHost;
      return Option.getOrThrow(yield* host.repositories.resolve(KEY));
    }).pipe(Effect.provide(backend(repoRoutes("Mon, 20 Jul 2026 12:00:00 GMT"))));

    const second = yield* Effect.gen(function* () {
      const host = yield* CodeHost;
      return Option.getOrThrow(yield* host.repositories.resolve(KEY));
    }).pipe(
      Effect.provide(
        backend({
          ...repoRoutes("Tue, 21 Jul 2026 09:30:00 GMT"),
          // The tip of `main` moved and `feat/x-1` was deleted upstream.
          "/repos/callajd/sprinter/branches": () =>
            json([{ name: "main", commit: { sha: SHA_FEAT } }]),
        }),
      ),
    );

    // SAME id (so the store upserts the same row) …
    expect(second.id).toBe(first.id);
    // … NEW observedAt, and a record describing ONE later moment: the deleted branch
    // is simply absent, never merged in from the earlier read.
    expect(second.observedAt).toBe("2026-07-21T09:30:00.000Z");
    expect(second.observedAt > first.observedAt).toBe(true);
    expect(second.refs).toStrictEqual([{ name: "main", sha: SHA_FEAT }]);
  }),
);

// D5 — the LEAP SECOND is translated at the ADAPTER boundary, not in `Timestamp`.
// `Timestamp` rejects `:60` outright by design; a code host can still emit one, so
// this module translates it to the instant it denotes BEFORE the value reaches the
// domain. The whole observation must survive — a decode failure here would drop a
// repository because its host restated a UTC broadcast.
it.effect("translates a host LEAP SECOND to the following second (D5)", () =>
  Effect.forEach(
    [
      // The canonical case: the end of a UTC day.
      ["2012-06-30T23:59:60Z", "2012-07-01T00:00:00.000Z"],
      // Mid-year, mid-month — the roll is the platform's date arithmetic, not string
      // surgery, so every boundary behaves.
      ["2026-07-20T12:00:60Z", "2026-07-20T12:01:00.000Z"],
      // The zero-offset spelling of the same thing.
      ["2015-06-30T23:59:60+00:00", "2015-07-01T00:00:00.000Z"],
      // KNOWN LOSS, pinned so it stays a decision rather than drifting into a surprise:
      // a FRACTIONAL leap second loses its sub-second part. The translation is `:59`
      // plus one second, and the fraction is not re-attached — see `hostInstant`.
      ["2012-06-30T23:59:60.500Z", "2012-07-01T00:00:00.000Z"],
    ],
    ([raw = "", expected = ""]) =>
      Effect.gen(function* () {
        expect(yield* hostInstant(raw, "resolve")).toBe(expected);
      }),
  ),
);

it.effect("the leap-second translation does NOT weaken Timestamp for anything else", () =>
  Effect.forEach(
    // Every other refusal still stands: an impossible field value, a rolled-over date,
    // a non-zero offset, and a non-instant string all fail at this boundary rather
    // than being smuggled past it.
    //
    // The last two are the INTERSECTION the earlier inputs miss and the one that
    // actually broke: a string that is impossible AND carries a `:60` seconds field
    // takes the leap-second TRANSLATION branch, where `Date.parse` answers `NaN` and
    // `new Date(NaN).toISOString()` throws inside an `Effect.sync`. Without the
    // non-finite guard these were a DIE — a defect killing the calling fiber — rather
    // than the declared `CodeHostError`. `Effect.flip` is what pins that: a defect does
    // not flip, it propagates, so this test fails rather than passes if it regresses.
    [
      "2026-13-45T99:99:99Z",
      "2026-02-30T00:00:00.000Z",
      "2026-07-20T12:00:00+02:00",
      "nope",
      "2026-13-45T23:59:60Z",
      "2026-02-30T23:59:60Z",
    ],
    (raw) =>
      Effect.gen(function* () {
        const error = yield* hostInstant(raw, "resolve").pipe(Effect.flip);
        expect(error).toBeInstanceOf(CodeHostError);
      }),
  ),
);

it.effect("carries a host leap second all the way through resolve, as a real Timestamp", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const resolved = Option.getOrThrow(yield* host.repositories.resolve(KEY));
    // End-to-end: the host's `Date` header said `23:59:60`, and the observation
    // survived — stamped with the instant that spelling denotes.
    expect(resolved.observedAt).toBe("2012-07-01T00:00:00.000Z");
  }).pipe(Effect.provide(backend(repoRoutes("Sat, 30 Jun 2012 23:59:60 GMT")))),
);

// …and the same path with an IMPOSSIBLE date behind the leap second FAILS rather than
// DIES. `httpDateToIso` deliberately does not validate the day (it re-spells only), so
// `45 Jun` reaches `hostInstant` together with `:60` — the exact intersection that used
// to format `new Date(NaN)` and throw. `Effect.flip` proves it is the declared failure:
// a defect would propagate and fail this test instead of satisfying it.
it.effect("a malformed host Date WITH a leap second FAILS the observation, never dies", () =>
  Effect.gen(function* () {
    const host = yield* CodeHost;
    const error = yield* host.repositories.resolve(KEY).pipe(Effect.flip);
    expect(error).toBeInstanceOf(CodeHostError);
    expect(error.operation).toBe("resolve");
  }).pipe(Effect.provide(backend(repoRoutes("Sat, 45 Jun 2026 23:59:60 GMT")))),
);

// The id encoding must be INJECTIVE, or two different repositories would share one id
// and the store's id-keyed upsert would let either silently overwrite the other's row.
// `a-b/c` and `a/b-c` are the pair a `-`-joined encoding would collapse.
it.effect("mints DISTINCT ids for repositories a naive encoding would collide", () =>
  Effect.gen(function* () {
    const left = yield* repositoryIdFor({ host: "github", owner: "a-b", name: "c" });
    const right = yield* repositoryIdFor({ host: "github", owner: "a", name: "b-c" });
    expect(left).not.toBe(right);
  }),
);
