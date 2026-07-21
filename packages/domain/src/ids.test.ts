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

const brands = [
  ["WorkstreamId", WorkstreamId],
  ["EpicId", EpicId],
  ["IssueId", IssueId],
  ["JobId", JobId],
  ["SessionId", SessionId],
  ["AgentId", AgentId],
  ["RepositoryId", RepositoryId],
] as const;

it.effect("decodes non-empty strings into each branded id and round-trips", () =>
  Effect.forEach(brands, ([label, schema]) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(schema)(label);
      const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
      expect(encoded).toBe(label);
    }),
  ),
);

it.effect("rejects the empty string for every branded id", () =>
  Effect.forEach(brands, ([, schema]) =>
    Effect.exit(Schema.decodeUnknownEffect(schema)("")).pipe(
      Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
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
