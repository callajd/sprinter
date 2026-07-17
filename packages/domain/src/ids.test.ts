import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { EpicId, IssueId, JobId, SessionId, WorkstreamId } from "./ids.ts";

const brands = [
  ["WorkstreamId", WorkstreamId],
  ["EpicId", EpicId],
  ["IssueId", IssueId],
  ["JobId", JobId],
  ["SessionId", SessionId],
] as const;

it.effect("decodes non-empty strings into each branded id and round-trips", () =>
  Effect.gen(function* () {
    for (const [label, schema] of brands) {
      const decoded = yield* Schema.decodeUnknownEffect(schema)(label);
      const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
      expect(encoded).toBe(label);
    }
  }),
);

it.effect("rejects the empty string for every branded id", () =>
  Effect.gen(function* () {
    for (const [, schema] of brands) {
      const exit = yield* Effect.exit(Schema.decodeUnknownEffect(schema)(""));
      expect(Exit.isFailure(exit)).toBe(true);
    }
  }),
);

it.effect("rejects non-string input for a branded id", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(Schema.decodeUnknownEffect(WorkstreamId)(42));
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);
