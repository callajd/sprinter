import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { BranchName } from "./ids.ts";
import {
  compareBranchNames,
  Repository,
  RepositoryKey,
  repositoryKey,
  RepositoryRefs,
  tipOf,
} from "./repository.ts";

const shaMain = "0123456789abcdef0123456789abcdef01234567";
const shaFeat = "89abcdef0123456789abcdef0123456789abcdef";

const wire = {
  id: "repo-github-callajd-sprinter",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [
    { name: "feat/x-1", sha: shaFeat },
    { name: "main", sha: shaMain },
  ],
  observedAt: "2026-07-20T12:00:00.000Z",
};

const decode = Schema.decodeUnknownEffect(Repository);

it.effect("decodes a repository observation and round-trips it byte-for-byte", () =>
  Effect.gen(function* () {
    const repository = yield* decode(wire);
    expect(yield* Schema.encodeUnknownEffect(Repository)(repository)).toStrictEqual(wire);
  }),
);

// An EMPTY ref set is VALID — "nothing observed yet" is a real state of a real
// repository, not a malformed observation (D4). It must decode, or the very first
// observation of a repository whose branches have not been read would be rejected.
it.effect("accepts an EMPTY ref set — nothing observed yet is a valid observation", () =>
  Effect.gen(function* () {
    const repository = yield* decode({ ...wire, refs: [] });
    expect(repository.refs).toStrictEqual([]);
  }),
);

// The natural key `(host, owner, name)` is what identifies a repository, and a `/` in
// either segment would make it AMBIGUOUS: `owner: "a/b", name: "c"` and
// `owner: "a", name: "b/c"` denote the same path while being different triples, so
// the store's UNIQUE constraint could not see the collision. Rejected at the schema.
it.effect("rejects an owner or name containing `/` — the natural key must be unambiguous", () =>
  Effect.forEach(
    [
      { ...wire, owner: "a/b" },
      { ...wire, name: "a/b" },
      { ...wire, owner: "" },
      { ...wire, name: "" },
    ],
    (value) =>
      Effect.exit(decode(value)).pipe(
        Effect.map((exit) =>
          expect(Exit.isFailure(exit), `expected ${JSON.stringify(value)} to be rejected`).toBe(
            true,
          ),
        ),
      ),
  ),
);

// A segment that is a relative PATH segment escapes the request URL a code-host
// adapter interpolates it into: `encodeURIComponent` does NOT escape `.`, and URL
// normalisation resolves `..`, so `name: ".."` walks OUT of `/repos/{owner}/{name}` and
// lets a caller who supplied only a repository key steer an AUTHENTICATED request at an
// unrelated endpoint (`owner: "..", name: "user"` → `GET /user`). The key is fully
// client-supplied — it arrives on `WorkstreamPlan` off the RPC surface — so this is
// rejected at DECODE, at the one boundary every key crosses, and no adapter is ever
// handed the value (INV-ENFORCE).
it.effect("rejects `.` and `..` — a relative path segment is not a repository name", () =>
  Effect.forEach(
    [
      // The plain cases.
      { ...wire, owner: "." },
      { ...wire, name: "." },
      { ...wire, owner: ".." },
      { ...wire, name: ".." },
      // The exact vectors that reached `api.github.com` before this check existed:
      // `GET /repos/` and `GET /..%252f..%252fx`.
      { ...wire, owner: "a", name: ".." },
      { ...wire, owner: "..", name: "..%2f..%2fx" },
      // …and the escalation: an authenticated `GET /user` steered by a plan.
      { ...wire, owner: "..", name: "user" },
    ],
    (value) =>
      Effect.exit(decode(value)).pipe(
        Effect.map((exit) =>
          expect(Exit.isFailure(exit), `expected ${JSON.stringify(value)} to be rejected`).toBe(
            true,
          ),
        ),
      ),
  ),
);

// Everything outside `[A-Za-z0-9._-]` is refused, not just the characters that were
// dangerous last time: each of these carries a syntax to SOME transport the segment is
// interpolated into, and an allow-list is the only form of this rule nobody has to keep
// complete. It is a superset of what a code host permits, so nothing real is excluded —
// the accepted spellings below all decode.
it.effect("accepts only [A-Za-z0-9._-] in an owner or name", () =>
  Effect.gen(function* () {
    yield* Effect.forEach(
      ["a%2fb", "a?b", "a#b", "a b", "a\\b", "a:b", "a*b", "café", "a\nb"],
      (segment) =>
        Effect.exit(decode({ ...wire, name: segment })).pipe(
          Effect.map((exit) =>
            expect(Exit.isFailure(exit), `expected ${JSON.stringify(segment)} to be rejected`).toBe(
              true,
            ),
          ),
        ),
    );
    yield* Effect.forEach(
      ["sprinter", "Sprinter", "dot.net", "a_b", "a-b", ".github", "x1"],
      (segment) =>
        Effect.map(decode({ ...wire, name: segment }), (repo) => expect(repo.name).toBe(segment)),
    );
  }),
);

// `host` is a CLOSED literal set (D2): a host with no adapter behind it names a
// repository nothing can read, resolve or refresh.
it.effect("rejects a host outside the closed literal set", () =>
  Effect.gen(function* () {
    expect(Exit.isFailure(yield* Effect.exit(decode({ ...wire, host: "gitlab" })))).toBe(true);
  }),
);

// The ref values are CHECKED, not merely strings: an observation carrying a malformed
// branch name or an abbreviated/uppercased sha fails to decode as a WHOLE rather than
// landing a bad value in the store. (Modelling `refs` as a keyed record would have
// SELECTED the matching keys and silently dropped the bad one — see `repository.ts`.)
it.effect("rejects an observation whose refs carry a malformed name or sha", () =>
  Effect.forEach(
    [
      { ...wire, refs: [{ name: "a..b", sha: shaMain }] },
      { ...wire, refs: [{ name: "main", sha: "0123456" }] },
      { ...wire, refs: [{ name: "main", sha: shaMain.toUpperCase() }] },
    ],
    (value) =>
      Effect.exit(decode(value)).pipe(
        Effect.map((exit) =>
          expect(Exit.isFailure(exit), `expected ${JSON.stringify(value)} to be rejected`).toBe(
            true,
          ),
        ),
      ),
  ),
);

// A branch appears AT MOST ONCE in one repository's observed refs — and that falls out
// of the ORDER rule rather than being a second, independent filter: the order is
// STRICTLY ascending, so a repeated name is an adjacent pair comparing equal, which
// strictness already rejects. (The store makes it unconstructible independently, with a
// composite PRIMARY KEY that holds against a writer which never saw this schema.)
it.effect("rejects a ref list repeating a branch name (subsumed by the strict order)", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      decode({
        ...wire,
        refs: [
          { name: "main", sha: shaMain },
          { name: "main", sha: shaFeat },
        ],
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

// `refs` is ORDERED, and the order is CHECKED rather than merely documented. An
// asserted-but-unenforced property is the worst kind: the two producers (the code-host
// adapter, and the store's `ORDER BY name`) can drift apart and nothing notices. The
// order is `compareBranchNames` — Unicode code point, which is what SQLite's default
// BINARY collation yields over UTF-8, so a record cannot be reordered by a round-trip.
//
// The check runs on DECODE, and `RepositoryRefs` is BRANDED so decode is the only way
// to obtain the field's type — which is what makes "a mis-sorted producer fails to
// BUILD the record" true rather than "fails to encode it, inside somebody else's
// `Snapshot` response". The type-level half of that is enforced by the gate's
// `check:types`; this is its runtime half, asserted on `RepositoryRefs` directly as well
// as through `Repository`, since the brand is what a producer must go through.
it.effect("rejects a ref list that is not ordered by branch name", () =>
  Effect.gen(function* () {
    const misSorted = [
      { name: "main", sha: shaMain },
      { name: "feat/x-1", sha: shaFeat },
    ];
    expect(Exit.isFailure(yield* Effect.exit(decode({ ...wire, refs: misSorted })))).toBe(true);
    expect(
      Exit.isFailure(yield* Effect.exit(Schema.decodeUnknownEffect(RepositoryRefs)(misSorted))),
    ).toBe(true);
    // …and the positive control: the same pair, sorted, decodes.
    expect(
      (yield* Schema.decodeUnknownEffect(RepositoryRefs)([...misSorted].reverse())).map(
        (ref) => ref.name,
      ),
    ).toStrictEqual(["feat/x-1", "main"]);
  }),
);

// The two orders disagree exactly for NON-BMP names: JS `<` compares UTF-16 code
// UNITS, so a surrogate pair (`0xD83D…`) sorts BEFORE `U+FFFD`, while its code point is
// far above it. `compareBranchNames` is the code-point order both producers use, so
// this list is the one the store also returns.
it.effect("orders a NON-BMP branch name by code point, where JS `<` would disagree", () =>
  Effect.gen(function* () {
    // `\uFFFD` (U+FFFD) < `\u{1F680}` (U+1F680) by code point; the opposite under JS `<`,
    // which sees the rocket's leading surrogate `\uD83D` and puts it first.
    const utf16CodeUnitOrder = (left: string, right: string): boolean => left < right;
    // A PREFIX sorts first and equality is 0 — the same verdicts SQLite's `memcmp`
    // reaches, since UTF-8 encodes no name as a prefix of a different one by accident.
    expect(compareBranchNames("main", "main-2")).toBeLessThan(0);
    expect(compareBranchNames("main-2", "main")).toBeGreaterThan(0);
    expect(compareBranchNames("main", "main")).toBe(0);
    expect(compareBranchNames("\uFFFD", "\u{1F680}")).toBeLessThan(0);
    expect(utf16CodeUnitOrder("\uFFFD", "\u{1F680}")).toBe(false);
    const repository = yield* decode({
      ...wire,
      refs: [
        { name: "\uFFFD", sha: shaMain },
        { name: "\u{1F680}", sha: shaFeat },
      ],
    });
    expect(repository.refs.map((ref) => ref.name)).toStrictEqual(["\uFFFD", "\u{1F680}"]);
  }),
);

// `observedAt` is the shared owned `Timestamp` — NOT a second time type — so it
// inherits its canonicalisation and its refusals (a leap second among them; the code
// host adapter translates one at its boundary, D5).
it.effect("carries observedAt as the owned Timestamp — canonicalised, and strict", () =>
  Effect.gen(function* () {
    const canonicalised = yield* decode({ ...wire, observedAt: "2026-07-20T12:00:00+00:00" });
    expect(canonicalised.observedAt).toBe("2026-07-20T12:00:00.000Z");
    const leap = yield* Effect.exit(decode({ ...wire, observedAt: "2026-06-30T23:59:60Z" }));
    expect(Exit.isFailure(leap)).toBe(true);
  }),
);

it.effect("extracts the natural key, and resolves a branch tip from the observed refs", () =>
  Effect.gen(function* () {
    const repository = yield* decode(wire);
    expect(repositoryKey(repository)).toStrictEqual(
      yield* Schema.decodeUnknownEffect(RepositoryKey)({
        host: "github",
        owner: "callajd",
        name: "sprinter",
      }),
    );
    const main = yield* Schema.decodeUnknownEffect(BranchName)("main");
    const absent = yield* Schema.decodeUnknownEffect(BranchName)("nope");
    expect(tipOf(repository, main)).toBe(shaMain);
    // Absent means "NOT OBSERVED" — never "the branch does not exist", which only the
    // code host can say.
    expect(tipOf(repository, absent)).toBeUndefined();
  }),
);
