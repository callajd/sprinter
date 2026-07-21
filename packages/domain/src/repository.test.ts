import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { BranchName } from "./ids.ts";
import { Repository, RepositoryKey, repositoryKey, tipOf } from "./repository.ts";

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

// A branch appears AT MOST ONCE in one repository's observed refs. The store makes
// this unconstructible with a composite PRIMARY KEY; the schema states it too, so a
// value that never reaches the store (a wire payload, a fixture) cannot carry it
// either.
it.effect("rejects a ref list repeating a branch name", () =>
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
