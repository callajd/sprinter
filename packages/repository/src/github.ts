/**
 * The GitHub ADAPTER behind the {@link CodeHost} port (Track A, task AE3.2).
 *
 * This is the ONLY module in the codebase permitted to reference the code host
 * concretely (INV-PORT / D5 / D14): GitHub's REST + GraphQL endpoints, its resource
 * paths, and its wire JSON. Nothing above it — no consumer, no reconciler — imports
 * a GitHub client, a URL, `gh`, or learns that a closing PR is found via GraphQL;
 * they build to the port ({@link ./code-host.ts}) and this instance is provided
 * behind a `Layer`.
 *
 * ## The instant boundary — a LEAP SECOND is translated HERE (DE1.2 D5)
 *
 * `Timestamp` (`@sprinter/domain`) rejects `…T23:59:60Z` as a hard decode failure,
 * deliberately: normalising it would silently move which second a stamp denotes and
 * collapse two instants onto one string. But a real code host CAN emit one — its
 * clock may be NTP-derived and restating a UTC broadcast — so something has to absorb
 * the difference between the host's representation and the domain's canonical one.
 * That something is THIS module, which is exactly what `INV-PORT` is for: the adapter
 * owns host-specific representation, and the domain stays canonical and strict.
 *
 * So every externally-sourced instant enters through {@link hostInstant}, which
 * translates a leap second to the instant it denotes (the following second) BEFORE the
 * value reaches `Timestamp`. Nothing here weakens `Timestamp`, and no raw host string
 * reaches it unvalidated — `hostInstant` translates and then DECODES, so a host string
 * that is malformed for any other reason still fails at the boundary.
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
 * into the owned {@link CodeHostError}, so no consumer sees an HTTP or decode type.
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
import {
  compareBranchNames,
  PositiveInt,
  type PullRequestRef,
  type Repository,
  RepositoryId,
  RepositoryKey,
  RepositoryRef,
  Timestamp,
} from "@sprinter/domain";
import {
  CodeHost,
  CodeHostError,
  type CodeOps,
  type IssueOps,
  type PullRequestOps,
  type RepositoryOps,
  RepositoryIssue,
} from "./code-host.ts";

// ============================================================================
// Error mapping — host failures → the owned CodeHostError (INV-PORT)
// ============================================================================

/**
 * Translate any host failure (an `HttpClientError` from the transport / status
 * filter, a `Schema.SchemaError` from decoding) into the owned
 * {@link CodeHostError}. Every such error carries a `message`; that is the only
 * surface this adapter leaks upward, so no consumer ever sees an HTTP or decode
 * type (INV-PORT).
 */
const fail =
  (operation: string) =>
  (error: { readonly message: string }): CodeHostError =>
    new CodeHostError({ operation, detail: error.message });

// ============================================================================
// The instant boundary — host spelling → the domain's canonical Timestamp
// ============================================================================

/**
 * An ISO-8601 UTC instant as a code host spells it, split into the fields the
 * leap-second translation needs. Deliberately SHAPE-only and permissive about the
 * seconds field (`\d{2}`, so `60` matches): its job is to RECOGNISE a leap second, not
 * to validate the instant — validation is `Timestamp`'s, and it still runs.
 */
const HOST_INSTANT = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d+)?(Z|\+00:00)$/;

/**
 * Translate a code host's instant spelling into one the domain's `Timestamp` accepts,
 * then DECODE it (DE1.2 D5).
 *
 * The ONE translation performed is the LEAP SECOND: `:60` in the seconds field denotes
 * the instant that, on the ECMAScript time line (which has no leap second), IS the
 * following `:00` — so `2012-06-30T23:59:60Z` becomes `2012-07-01T00:00:00.000Z`. It
 * is computed by taking the instant one second EARLIER (`:59`, which always exists)
 * and adding a second, so the roll across a minute, hour, day, month or year boundary
 * falls out of the platform's own date arithmetic rather than out of string surgery.
 *
 * Doing it HERE rather than in `Timestamp` is the whole point (INV-PORT): the choice
 * is VISIBLE, attributable to the host that sent it, and confined to the one module
 * that knows about GitHub. `Timestamp` stays strict for every other caller, and its
 * refusal keeps meaning "an upstream sent something that is not a UTC instant".
 *
 * Everything else is left to `Timestamp`: sub-millisecond precision, the `+00:00`
 * zero offset, and every impossible field value (month `13`, `2026-02-30`, hour `24`)
 * reach it untouched, so a host string that is malformed for any reason OTHER than a
 * leap second still fails at this boundary rather than being smuggled past it.
 *
 * That deferral needs a guard, because {@link HOST_INSTANT} is shape-only: a string
 * that is BOTH a leap second AND otherwise impossible (`2026-13-45T23:59:60Z`,
 * `2026-02-30T23:59:60Z`) takes the translation branch, where the arithmetic either
 * throws (`new Date(NaN).toISOString()`, a DEFECT that would kill the calling fiber) or
 * silently invents an instant (`Date.parse` rolls `2026-02-30` to 2 March). So the
 * translation validates its own input through `Timestamp` FIRST — the domain's rule,
 * not a second copy of it — and an input that fails is handed on RAW, to be refused
 * below exactly as it would have been. The failure is always the declared
 * `CodeHostError`.
 *
 * KNOWN LOSS: a FRACTIONAL leap second loses its sub-second part. The translation is
 * computed from `:59` of the same minute plus one second, and the fraction (captured by
 * the regex, then deliberately unused) is not re-attached — so `2012-06-30T23:59:60.5Z`
 * becomes `2012-07-01T00:00:00.000Z` rather than `…:00.500Z`. It is accepted rather
 * than fixed: it is confined to the one instant per leap event, no code host Sprinter
 * reads emits a fractional leap second, and the value it feeds — `observedAt`, rendered
 * as staleness (DE4.4) — is not sensitive at half a second. Re-attaching it would mean
 * doing the arithmetic in milliseconds, which buys nothing real here.
 */
export const hostInstant = (
  raw: string,
  operation: string,
): Effect.Effect<Timestamp, CodeHostError> =>
  Effect.gen(function* () {
    const match = HOST_INSTANT.exec(raw);
    const translated =
      match === null || match[4] !== "60"
        ? raw
        : yield* Effect.gen(function* () {
            const [, date = "", hour = "", minute = ""] = match;
            // `:59` of the same minute always exists (there is no leap 59th second) — but
            // ONLY if the rest of the instant is real, which {@link HOST_INSTANT} does not
            // check: it is shape-only, so `2026-13-45T23:59:60Z` and `2026-02-30T23:59:60Z`
            // reach here too. So the `:59` spelling is validated by the DOMAIN's own rule
            // before anything is translated, rather than by a second copy of it: `Timestamp`
            // round-trips the parse, which is what catches BOTH the unparseable month `13`
            // and the silently ROLLED-OVER `2026-02-30` (`Date.parse` answers 2 March for
            // that one, so a bare `Number.isNaN` guard would let it through and the
            // translation would invent an instant the host never sent).
            const oneSecondEarlier = `${date}T${hour}:${minute}:59Z`;
            const real = yield* Schema.decodeUnknownEffect(Timestamp)(oneSecondEarlier).pipe(
              Effect.option,
            );
            // Not translatable: hand the RAW string on and let `Timestamp` refuse it below,
            // so the failure is the declared `CodeHostError` — never a defect from
            // formatting `new Date(NaN)`, which would kill the calling fiber.
            if (Option.isNone(real)) return raw;
            // Adding 1000 ms lands on the instant `:60` denotes, with every boundary roll
            // (minute, hour, day, month, year) handled by the platform's date arithmetic.
            return new Date(Date.parse(real.value) + 1000).toISOString();
          });
    return yield* Schema.decodeUnknownEffect(Timestamp)(translated).pipe(
      Effect.mapError(() =>
        fail(operation)({
          message: `the code host reported "${raw}", which is not a UTC instant this domain can represent`,
        }),
      ),
    );
  });

// ============================================================================
// Wire schemas — GitHub REST JSON (decoded, then mapped to owned port types)
// ============================================================================

/**
 * The `GET /repos/{owner}/{repo}` fields we read: the default branch, and the
 * repository's CANONICAL identity as the host itself spells it.
 *
 * `owner.login` + `name` rather than `full_name`: the two carry the same information,
 * but the split form is already the natural key's shape, so it needs no parser and no
 * decision about what a `full_name` with more than one `/` in it would mean.
 *
 * The identity is read because GitHub's repository lookup is NOT
 * spelling-preserving — it matches case-insensitively, and it 301-redirects a renamed
 * repository to its current path (a redirect `fetch` follows transparently). So a 200
 * confirms "a repository is here", never "it is spelled the way you asked", and the
 * only spelling that can be stored is the one in the body.
 */
const GhRepo = Schema.Struct({
  default_branch: Schema.NonEmptyString,
  name: Schema.String,
  owner: Schema.Struct({ login: Schema.String }),
});

/**
 * The `GET /repos/{owner}/{repo}/branches` fields we read: each branch's name and the
 * commit its tip points at. GitHub's wire nests the sha under `commit`; the port's
 * `RepositoryRef` is flat, and this boundary is where that shape difference stops.
 */
const GhBranch = Schema.Struct({
  name: Schema.String,
  commit: Schema.Struct({ sha: Schema.String }),
});

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
 * {@link CodeHostError}); otherwise the MERGED `closedByPullRequestsReferences`
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
 * Mint the opaque {@link RepositoryId} for a repository's NATURAL key.
 *
 * It is a pure FUNCTION of the key, and it has to be: the store upserts a refresh on
 * the id while holding `(host, owner, name)` UNIQUE, so an id that varied per
 * observation would make the second observation of one repository collide with the
 * first instead of replacing it (D7).
 *
 * The encoding is INJECTIVE, which is the property that actually matters here and the
 * one a naive `repo-${host}-${owner}-${name}` would NOT have: `owner: "a-b", name: "c"`
 * and `owner: "a", name: "b-c"` are two different repositories that would receive one
 * id, and the store's id-keyed upsert would then let either silently overwrite the
 * other's row. The separators are chosen so no segment can contain them — `host` is a
 * closed literal, and an owner/name segment may not contain `/` (the domain rejects it)
 * and cannot contain `:` on any host Sprinter reads — so the three parts can never
 * re-split into a different triple.
 *
 * The key it is handed must be the HOST's canonical spelling, not a caller's: the id is
 * a function of the key, so two spellings of one repository would otherwise mint two
 * ids. `resolve` reads that spelling off the `GET /repos/…` body before calling here.
 *
 * Nothing above this module may parse the result: it is opaque, and equality is its
 * only defined operation.
 */
export const repositoryIdFor = (key: RepositoryKey): Effect.Effect<RepositoryId, CodeHostError> =>
  Schema.decodeUnknownEffect(RepositoryId)(`repo:${key.host}:${key.owner}/${key.name}`).pipe(
    Effect.mapError(() =>
      fail("resolve")({ message: `cannot derive a repository id for ${key.owner}/${key.name}` }),
    ),
  );

/** The months of an HTTP `Date` header, in the order RFC 7231's IMF-fixdate spells them. */
const HTTP_DATE_MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

/** RFC 7231's IMF-fixdate, the one `Date` header form a conforming host must send. */
const IMF_FIXDATE = /^[A-Za-z]{3}, (\d{2}) ([A-Za-z]{3}) (\d{4}) (\d{2}:\d{2}:\d{2}) GMT$/;

/**
 * Rewrite an HTTP `Date` header (RFC 7231 IMF-fixdate) into the ISO-8601 UTC spelling
 * {@link hostInstant} takes, or `undefined` when the header is absent or not in that
 * form.
 *
 * It is a pure RE-SPELLING — no parsing of the instant, no rollover, no validation —
 * and that is deliberate: handing the string to `Date.parse` here would let the
 * platform silently roll a LEAP SECOND forward to the next `:00`, which is precisely
 * the translation `hostInstant` exists to make VISIBLE and attributable. Keeping the
 * seconds field verbatim means a `:60` header reaches the one place that decides what
 * to do with it. Every other verdict (an impossible date, an out-of-range field) is
 * likewise left to `Timestamp` behind `hostInstant`.
 */
const httpDateToIso = (header: string | undefined): string | undefined => {
  if (header === undefined) return undefined;
  const match = IMF_FIXDATE.exec(header);
  if (match === null) return undefined;
  const [, day = "", month = "", year = "", time = ""] = match;
  const monthIndex = HTTP_DATE_MONTHS.indexOf(month);
  if (monthIndex < 0) return undefined;
  return `${year}-${String(monthIndex + 1).padStart(2, "0")}-${day}T${time}Z`;
};

/**
 * The instant an observation was made, as the CODE HOST reports it.
 *
 * The source is the response's HTTP `Date` header — the moment the host itself says it
 * produced this view, which is what `observedAt` means for a record read off that host
 * (INV-OBSERVED), and the reading DE4.4 renders staleness from. It arrives in the
 * host's own spelling and goes through {@link hostInstant}, so a leap second is
 * translated at this boundary (D5) rather than failing the whole observation.
 *
 * A missing or non-conforming header falls back to THIS process's clock. That is a
 * deliberate degradation, not a silent one: the fallback is by construction canonical
 * (`toISOString` emits exactly the `Timestamp` form and can never produce a leap
 * second), so the observation still carries a real instant, one whose only cost is that
 * it measures our clock rather than the host's. Failing the observation instead would
 * make Sprinter unable to read a repository from a host that merely omits an optional
 * header.
 */
const observedAtFrom = (
  header: string | undefined,
  operation: string,
): Effect.Effect<Timestamp, CodeHostError> => {
  const iso = httpDateToIso(header);
  return iso === undefined
    ? Effect.sync(() => new Date().toISOString()).pipe(
        Effect.flatMap((now) => hostInstant(now, operation)),
      )
    : hostInstant(iso, operation);
};

/**
 * Build the {@link CodeHost} implementation over the ambient {@link HttpClient}.
 * Each method issues a parameterised GitHub REST request (base URL + auth/accept
 * headers pre-applied) and decodes the response through `Schema` into the owned
 * port types; all host failures are mapped to {@link CodeHostError} at the
 * boundary.
 */
const make = (config: RepositoryConfig) =>
  Effect.gen(function* () {
    // The token is REQUIRED for EVERY operation (B1, NB3). GitHub's GraphQL endpoint
    // 401s every unauthenticated request, and the REST ops must never carry an empty
    // `Bearer`. So refuse to construct the adapter with an empty/blank token: a single,
    // universal fail-fast with a distinct, loud CodeHostError (never a silent empty
    // credential or `Option.none`), backing up the boot-time fail-fast in the daemon
    // composition root. The token itself is never logged.
    if (config.token.trim() === "") {
      return yield* Effect.fail(
        new CodeHostError({
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

    // Repository OBSERVATION addresses the repository the CALLER names, not the
    // adapter's bound `owner/repo`: its whole job is to turn a natural key a client
    // supplied (a `WorkstreamPlan`, D6) into an identity, and that key need not be the
    // one this adapter reconciles Issues against.
    const repositories: RepositoryOps = {
      resolve: (key) =>
        Effect.gen(function* () {
          const path = `/repos/${encodeURIComponent(key.owner)}/${encodeURIComponent(key.name)}`;
          const response = yield* client
            .execute(HttpClientRequest.get(path))
            .pipe(Effect.mapError(fail("resolve")));
          // 404 is INFORMATION, not a failure: the host is telling us it has no such
          // repository. It becomes `Option.none`, which the caller turns into a
          // rejection that writes nothing — never into an unobserved row.
          if (response.status === 404) return Option.none<Repository>();
          if (response.status !== 200) {
            return yield* Effect.fail(
              new CodeHostError({
                operation: "resolve",
                detail: `unexpected status ${response.status}`,
              }),
            );
          }
          // The CANONICAL natural key — the host's spelling, not the caller's.
          //
          // GitHub resolves `/repos/{owner}/{name}` case-INSENSITIVELY and 301-redirects
          // a renamed repository, so `CallaJD/Sprinter`, `callajd/sprinter` and a former
          // name all answer 200 for ONE repository. Building the record from `key` would
          // mint one `RepositoryId` and one row PER SPELLING — two anchors for one
          // repository, invisible to the store's `UNIQUE (host, owner, name)` because the
          // triples genuinely differ. That is verbatim the failure the `Repository` entity
          // exists to eliminate, so the caller's spelling is used to ASK and the host's
          // to ANSWER, and the rename case falls out for free.
          //
          // It is re-DECODED through the owned `RepositoryKey`, not trusted: the host's
          // strings are externally sourced like any other, and this is the boundary they
          // cross (INV-ENFORCE).
          const repo = yield* response.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(GhRepo)),
            Effect.mapError(fail("resolve")),
          );
          const canonical = yield* Schema.decodeUnknownEffect(RepositoryKey)({
            host: key.host,
            owner: repo.owner.login,
            name: repo.name,
          }).pipe(Effect.mapError(fail("resolve")));
          const id = yield* repositoryIdFor(canonical);
          const observedAt = yield* observedAtFrom(response.headers["date"], "resolve");
          // The refs are read in the SAME resolve, so the returned record describes ONE
          // moment (D7: a refresh replaces the record wholesale, never merges into it),
          // and from the CANONICAL path, so a renamed repository's refs are read from
          // where it lives now rather than through a second redirect.
          // `per_page=100` is GitHub's maximum page; a repository with more branches is
          // observed partially, which the model already admits — `refs` is what WAS
          // observed, and an absent branch reads as "not observed", never as "does not
          // exist" (see `tipOf`).
          const canonicalPath = `/repos/${encodeURIComponent(canonical.owner)}/${encodeURIComponent(canonical.name)}`;
          const branchRows = yield* client
            .execute(HttpClientRequest.get(`${canonicalPath}/branches?per_page=100`))
            .pipe(
              Effect.flatMap(HttpClientResponse.filterStatusOk),
              Effect.flatMap((branches) => branches.json),
              Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(GhBranch))),
              Effect.mapError(fail("resolve")),
            );
          // Decoded through the OWNED `RepositoryRef`, so the host's flat strings are
          // CHECKED here: a malformed branch name or a non-canonical sha fails the
          // observation at the boundary instead of entering the domain (INV-ENFORCE).
          const refs = yield* Schema.decodeUnknownEffect(Schema.Array(RepositoryRef))(
            branchRows.map((branch) => ({ name: branch.name, sha: branch.commit.sha })),
          ).pipe(Effect.mapError(fail("resolve")));
          const repository: Repository = {
            id,
            host: canonical.host,
            owner: canonical.owner,
            name: canonical.name,
            // Ordered by name so one observation has ONE spelling, whatever order the
            // host paginated them in — in the domain's `compareBranchNames` (Unicode code
            // point) order, which is also the order the store reads them back in, so a
            // round-trip cannot reorder them. JS `<` here would be UTF-16 code-unit order
            // and would disagree with the store for a non-BMP branch name.
            refs: [...refs].sort((left, right) => compareBranchNames(left.name, right.name)),
            observedAt,
          };
          return Option.some(repository);
        }),
    };

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
                    new CodeHostError({
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

    return CodeHost.of({ repositories, code, issues, pullRequests });
  });

// ============================================================================
// Adapter layers
// ============================================================================

/**
 * The GitHub adapter for {@link CodeHost}, requiring an ambient
 * {@link HttpClient} — the seam a test provides a fake HTTP backend into (a fake
 * `Fetch` under {@link FetchHttpClient}), and production satisfies with
 * {@link layerFetch}. Exposes no HTTP type on its public shape beyond the standard
 * `HttpClient` requirement (INV-PORT).
 */
export const layer = (
  config: RepositoryConfig,
): Layer.Layer<CodeHost, CodeHostError, HttpClient.HttpClient> =>
  Layer.effect(CodeHost, make(config));

/**
 * The self-contained GitHub adapter: {@link layer} with the Bun-native fetch
 * transport ({@link FetchHttpClient.layer}, `globalThis.fetch`) provided. This is
 * the production wiring — a `Layer<CodeHost>` with the transport sealed inside,
 * exposing no HTTP type (INV-PORT).
 */
export const layerFetch = (config: RepositoryConfig): Layer.Layer<CodeHost, CodeHostError> =>
  layer(config).pipe(Layer.provide(FetchHttpClient.layer));
