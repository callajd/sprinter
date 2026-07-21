import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import {
  AgentId,
  BranchName,
  CommitSha,
  EpicId,
  IssueId,
  JobId,
  RepositoryId,
  SessionId,
  WorkstreamId,
} from "./ids.ts";

// Each brand paired with ONE well-formed sample. The sample is carried explicitly
// rather than reusing the label as its own value because not every id is a bare
// non-empty string: `RepositoryId` checks a SHAPE (`repo:<host>:<host-id>`), and a
// table that fed each schema its own name would have quietly stopped covering it.
const brands = [
  [WorkstreamId, "ws-alpha"],
  [EpicId, "epic-alpha"],
  [IssueId, "issue-alpha"],
  [JobId, "job-alpha"],
  [SessionId, "session-alpha"],
  [AgentId, "agent-alpha"],
  [RepositoryId, "repo:github:1296269"],
] as const;

it.effect("decodes a well-formed value for each branded id and round-trips", () =>
  Effect.forEach(brands, ([schema, sample]) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(schema)(sample);
      const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
      expect(encoded).toBe(sample);
    }),
  ),
);

it.effect("rejects the empty string for every branded id", () =>
  Effect.forEach(brands, ([schema]) =>
    Effect.exit(Schema.decodeUnknownEffect(schema)("")).pipe(
      Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
    ),
  ),
);

// ── RepositoryId — the SHAPE is in the schema, so a malformed id is unconstructible ──
//
// The value is opaque to READERS, but the encoding `repo:<host>:<host-id>` is what the
// injectivity argument rests on (`packages/repository/src/github.ts`'s
// `repositoryIdFor`), and these are the cases a branded `NonEmptyString` would have
// waved through. `repo:github:callajd/sprinter` is the one that matters most: it is a
// NATURAL-KEY-derived id, the exact shape that forks identity on a rename, and it is
// now unconstructible rather than merely discouraged.
it.effect("REJECTS a repository id that is not `repo:<host>:<host-id>`", () =>
  Effect.forEach(
    [
      "",
      "1296269",
      "repo:github:",
      "repo::1296269",
      "repo:github:callajd/sprinter",
      "repo:GitHub:1296269",
      "repo:github:1e+21",
      "repo:github:1296269 ",
      "workstream:github:1296269",
    ],
    (raw) =>
      Effect.exit(Schema.decodeUnknownEffect(RepositoryId)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
  ),
);

// The host-id segment is the URL-UNRESERVED set, which is what every identifier form a
// code host actually issues fits inside — so the check constrains the shape without
// committing the domain to GitHub's decimal ids.
it.effect("ACCEPTS every host-id spelling a second code host could plausibly issue", () =>
  Effect.forEach(
    [
      "repo:github:1296269",
      "repo:gitlab:278964",
      "repo:bitbucket:b4d0f6e2-0000-4000-8000-000000000001",
      "repo:some-host:A_z0.9~-",
    ],
    (raw) =>
      Schema.decodeUnknownEffect(RepositoryId)(raw).pipe(
        Effect.map((decoded) => expect(decoded).toBe(raw)),
      ),
  ),
);

it.effect("rejects non-string input for a branded id", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(Schema.decodeUnknownEffect(WorkstreamId)(42));
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

// ── CommitSha — the check is IN the schema, so a malformed sha is unconstructible ──
//
// These are the cases a branded `NonEmptyString` would have ACCEPTED, which is the
// whole reason `CommitSha` carries a real filter: `"zzz"` is not a sha, an uppercase
// spelling is a second spelling of one value (so equality would stop working), a
// 7-character abbreviation is a PREFIX QUERY whose referent can change, and a
// 41-character string is not a git object name at all.

const validSha = "0123456789abcdef0123456789abcdef01234567";

it.effect("decodes a full 40-character lowercase hex commit sha and round-trips", () =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknownEffect(CommitSha)(validSha);
    expect(yield* Schema.encodeUnknownEffect(CommitSha)(decoded)).toBe(validSha);
  }),
);

it.effect("rejects every non-sha a branded NonEmptyString would have accepted", () =>
  Effect.forEach(
    [
      "zzz",
      // Uppercase — the same commit, a second spelling.
      validSha.toUpperCase(),
      // A 7-character abbreviation.
      "0123456",
      // 41 characters — one too many.
      `${validSha}0`,
      // 39 characters — one too few.
      validSha.slice(0, -1),
      "",
      // Correct length, but `g` is not hex.
      `${validSha.slice(0, -1)}g`,
    ],
    (value) =>
      Effect.exit(Schema.decodeUnknownEffect(CommitSha)(value)).pipe(
        Effect.map((exit) =>
          expect(Exit.isFailure(exit), `expected ${value} to be rejected`).toBe(true),
        ),
      ),
  ),
);

// ── BranchName — one rejected example per enforced rule ──────────────────────

it.effect("accepts ordinary git branch names", () =>
  Effect.forEach(
    [
      "main",
      "feat/x-1",
      "release/2.0",
      "fix/86-repository-entity",
      "a",
      "user/feat/deep/name",
      // A PAIRED surrogate is a real code point and stays accepted — the rule rejects
      // lone surrogates, not non-BMP names (the store holds this one fine, and
      // `compareBranchNames` exists precisely to order it correctly).
      "feat/\u{1F680}",
    ],
    (value) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(BranchName)(value);
        expect(yield* Schema.encodeUnknownEffect(BranchName)(decoded)).toBe(value);
      }),
  ),
);

it.effect("rejects one example of every branch-name rule it enforces", () =>
  Effect.forEach(
    [
      // non-empty
      "",
      // no ASCII whitespace …
      "feat x",
      // … and no control characters
      "feat\tx",
      "feat\nx",
      // no `..` (git's range operator)
      "a..b",
      // no leading `/`
      "/main",
      // no trailing `/`
      "main/",
      // no `//`
      "a//b",
      // does not end in `.lock`
      "main.lock",
      // is not the reserved bare `@`
      "@",
      // no UNPAIRED SURROGATE: a lone `U+D800…U+DFFF` code unit is a well-formed JS
      // string but is NOT encodable as UTF-8, so it cannot survive the `TEXT` column
      // the store keeps ref names in — a name the store round-trip would silently
      // mangle to `U+FFFD` must not enter the domain in the first place.
      "\uD83D",
      "feat/\uDE80",
      "\uDC00trailing",
    ],
    (value) =>
      Effect.exit(Schema.decodeUnknownEffect(BranchName)(value)).pipe(
        Effect.map((exit) =>
          expect(Exit.isFailure(exit), `expected ${JSON.stringify(value)} to be rejected`).toBe(
            true,
          ),
        ),
      ),
  ),
);
