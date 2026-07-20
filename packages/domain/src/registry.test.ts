/**
 * The REGISTRY layer (DE1.1) — `Agent` and its append-only semantics.
 *
 * The suite pins the shape invariants the rest of the domain remodel builds on:
 * the two optional keys are genuinely optional and ELIDED when absent; retired-ness
 * is `retiredAt`'s presence and nothing else (INV-SUM); and the entity carries NO
 * repository/workstream scope and NO `observedAt` (INV-DERIVED / INV-OBSERVED) —
 * asserted structurally, so re-adding one of those fields fails a test rather than
 * only a review.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { Agent, isOriginalRevision, isRetired } from "./registry.ts";

const decode = (raw: (typeof Agent)["Encoded"]) =>
  Schema.decodeUnknownEffect(Agent)(raw).pipe(Effect.orDie);

const original = {
  id: "agt-1",
  name: "implementer",
  model: "opus",
  version: "1.0.0",
  tools: ["r"],
};

it.effect("round-trips an agent with both optional keys absent, eliding them", () =>
  Effect.gen(function* () {
    const agent = yield* decode(original);
    expect("supersedes" in agent).toBe(false);
    expect("retiredAt" in agent).toBe(false);
    expect(yield* Schema.encodeUnknownEffect(Agent)(agent)).toStrictEqual(original);
  }),
);

it.effect("round-trips a revision that supersedes another and a retired agent", () =>
  Effect.gen(function* () {
    const revised = yield* decode({ ...original, id: "agt-2", supersedes: "agt-1" });
    expect(revised.supersedes).toBe("agt-1");

    const retired = yield* decode({ ...original, retiredAt: "2026-07-20T12:00:00.000Z" });
    expect(retired.retiredAt).toBe("2026-07-20T12:00:00.000Z");
  }),
);

it.effect("reads retired-ness off `retiredAt` presence — there is no status enum", () =>
  Effect.gen(function* () {
    expect(isRetired(yield* decode(original))).toBe(false);
    expect(isRetired(yield* decode({ ...original, retiredAt: "2026-01-01T00:00:00Z" }))).toBe(true);
    // The shape carries no `status`/`state` field to contradict the stamp (INV-SUM).
    const keys = Object.keys(yield* decode({ ...original, retiredAt: "2026-01-01T00:00:00Z" }));
    expect(keys).not.toContain("status");
  }),
);

it.effect("identifies the first revision of a lineage by an absent `supersedes`", () =>
  Effect.gen(function* () {
    expect(isOriginalRevision(yield* decode(original))).toBe(true);
    expect(isOriginalRevision(yield* decode({ ...original, supersedes: "agt-0" }))).toBe(false);
  }),
);

it.effect("carries no repository/workstream scope and no observedAt", () =>
  Effect.gen(function* () {
    const agent = yield* decode({
      ...original,
      supersedes: "agt-0",
      retiredAt: "2026-01-01T00:00:00Z",
    });
    // Every field of a FULLY populated agent — an exact set, so adding a repository
    // scope (INV-DERIVED) or an `observedAt` (INV-OBSERVED) breaks this test.
    expect(Object.keys(agent).toSorted()).toStrictEqual([
      "id",
      "model",
      "name",
      "retiredAt",
      "supersedes",
      "tools",
      "version",
    ]);
  }),
);

it.effect("rejects an agent whose required fields are empty or mistyped", () =>
  Effect.gen(function* () {
    const rejects = [
      { ...original, id: "" },
      { ...original, name: "" },
      { ...original, model: "" },
      { ...original, version: "" },
      { ...original, tools: [""] },
      { ...original, retiredAt: "yesterday" },
    ];
    yield* Effect.forEach(rejects, (raw) =>
      Effect.exit(Schema.decodeUnknownEffect(Agent)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
    );
  }),
);
