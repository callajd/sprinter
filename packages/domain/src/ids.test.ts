import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { AgentId, EpicId, IssueId, JobId, SessionId, WorkstreamId } from "./ids.ts";

const brands = [
  ["WorkstreamId", WorkstreamId],
  ["EpicId", EpicId],
  ["IssueId", IssueId],
  ["JobId", JobId],
  ["SessionId", SessionId],
  ["AgentId", AgentId],
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
