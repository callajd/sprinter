/**
 * The REGISTRY layer (DE1.1) — `Agent` and its append-only semantics.
 *
 * The suite pins the shape invariants the rest of the domain remodel builds on:
 * the two optional keys are genuinely optional and ELIDED when absent; retired-ness
 * is `retiredAt`'s presence and nothing else (INV-SUM); and the entity carries NO
 * repository/workstream scope and NO `observedAt` (INV-DERIVED / INV-OBSERVED) —
 * asserted structurally, so re-adding one of those fields fails a test rather than
 * only a review.
 *
 * It also pins the LINEAGE-level read `isLineageRetired`, which is a genuinely
 * different question from the per-record `isRetired` under append-only semantics —
 * retiring a revision stamps a NEW record, never the one being retired.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { Agent, isLineageRetired, isOriginalRevision, isRetired } from "./registry.ts";

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

it.effect("answers lineage-level retirement, which per-record `isRetired` cannot", () =>
  Effect.gen(function* () {
    // A three-revision lineage: agt-1 --superseded by--> agt-2 --retired by--> agt-3.
    // Only agt-3 carries the stamp; agt-1 and agt-2 are immutable and un-stamped
    // forever, which is exactly why the per-record read is not the lineage read.
    const first = yield* decode(original);
    const second = yield* decode({ ...original, id: "agt-2", supersedes: "agt-1" });
    const retirement = yield* decode({
      ...original,
      id: "agt-3",
      supersedes: "agt-2",
      retiredAt: "2026-01-01T00:00:00Z",
    });
    const all = [first, second, retirement];

    // The distinction the helper exists for: the record says no, the lineage says yes.
    expect(isRetired(second)).toBe(false);
    expect(isLineageRetired(second, all)).toBe(true);
    // It walks FORWARD across an arbitrary number of hops, from any revision.
    expect(isLineageRetired(first, all)).toBe(true);
    // And a stamped revision is retired without needing any successor at all.
    expect(isLineageRetired(retirement, all)).toBe(true);

    // A live lineage (nothing retires it) is not retired at any revision …
    const live = [first, second];
    expect(isLineageRetired(first, live)).toBe(false);
    expect(isLineageRetired(second, live)).toBe(false);
    // … and a retirement in a DIFFERENT lineage does not leak across.
    const otherLineage = yield* decode({
      ...original,
      id: "agt-9",
      retiredAt: "2026-01-01T00:00:00Z",
    });
    expect(isLineageRetired(first, [first, second, otherLineage])).toBe(false);
  }),
);

it.effect("terminates on a malformed cyclic supersedes chain rather than looping", () =>
  Effect.gen(function* () {
    // The writer's acyclicity precondition forbids this, but a lineage read handed a
    // cycle must still RETURN — the walk visits each revision at most once.
    const a = yield* decode({ ...original, id: "agt-a", supersedes: "agt-b" });
    const b = yield* decode({ ...original, id: "agt-b", supersedes: "agt-a" });
    expect(isLineageRetired(a, [a, b])).toBe(false);
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
