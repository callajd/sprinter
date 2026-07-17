import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { isComplete, Workstream } from "./index.ts";

it.effect("decodes a valid workstream", () =>
  Effect.gen(function* () {
    const ws = yield* Schema.decodeUnknownEffect(Workstream)({
      id: "fdn",
      name: "Foundation",
      status: "active",
    });
    expect(ws.name).toBe("Foundation");
    expect(isComplete(ws)).toBe(false);
  }),
);

it.effect("rejects an invalid workstream", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      Schema.decodeUnknownEffect(Workstream)({ id: "", name: "x", status: "nope" }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

it.effect("isComplete is true for a done workstream", () =>
  Effect.gen(function* () {
    const ws = yield* Schema.decodeUnknownEffect(Workstream)({
      id: "a",
      name: "A",
      status: "done",
    });
    expect(isComplete(ws)).toBe(true);
  }),
);
