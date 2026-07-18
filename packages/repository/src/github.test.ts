/**
 * GitHub adapter coverage (AE3.2) — the {@link Repository} port exercised THROUGH
 * the GitHub adapter against a FAKE HTTP backend (a fake `Fetch` under
 * {@link FetchHttpClient}). Deterministic and OFFLINE — no live GitHub call in the
 * gate (INV-GATE): every request is answered by a canned in-memory route table.
 *
 * The suite proves the adapter issues the right GitHub REST paths, decodes each
 * wire response through `Schema` into the OWNED port types (never a cast,
 * INV-NOCAST), and translates every host failure into the owned
 * {@link RepositoryError} (INV-PORT). The tests depend ONLY on the package's public
 * surface plus the standard `FetchHttpClient` transport seam — never a concrete
 * GitHub client.
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

const config = { owner: "callajd", repo: "sprinter" };

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

it.effect("detects the PR that closes an Issue from its timeline", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(42));
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25/timeline": () =>
          json([
            { event: "labeled" },
            { event: "cross-referenced", source: { issue: { number: 7 } } },
            {
              event: "cross-referenced",
              source: { issue: { number: 42, pull_request: { url: "x" } } },
            },
          ]),
      }),
    ),
  ),
);

it.effect("follows timeline pagination — finds a closing PR referenced on a later page", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    // The closing PR's cross-reference is on page 2; the adapter must follow the
    // `Link: rel="next"` header rather than stop at the first page.
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.some(99));
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25/timeline": (() => {
          let call = 0;
          return () => {
            call += 1;
            if (call === 1) {
              // Page 1: only noise, with a `Link` header advertising page 2.
              return new Response(JSON.stringify([{ event: "labeled" }, { event: "commented" }]), {
                status: 200,
                headers: {
                  "content-type": "application/json",
                  link: '<https://api.github.com/repos/callajd/sprinter/issues/25/timeline?per_page=100&page=2>; rel="next"',
                },
              });
            }
            // Page 2: the closing PR reference, and no further `Link` → stop.
            return json([
              {
                event: "cross-referenced",
                source: { issue: { number: 99, pull_request: { url: "x" } } },
              },
            ]);
          };
        })(),
      }),
    ),
  ),
);

it.effect("reports no closing PR when nothing references the Issue", () =>
  Effect.gen(function* () {
    const repo = yield* Repository;
    const found = yield* repo.pullRequests.closingPullRequest(num(25));
    expect(found).toStrictEqual(Option.none());
  }).pipe(
    Effect.provide(
      backend({
        "/repos/callajd/sprinter/issues/25/timeline": () => json([{ event: "labeled" }]),
      }),
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
