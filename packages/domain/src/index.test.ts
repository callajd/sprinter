import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import { isComplete, ExecutionEvent, Workstream } from "./index.ts";

it.effect("re-exports the read model and execution schemas from the barrel", () =>
  Effect.gen(function* () {
    const ws = yield* Schema.decodeUnknownEffect(Workstream)({
      id: "ws-fdn",
      name: "Foundation",
      repositoryId: "repo:github:1296269",
      status: "done",
      epics: [],
    });
    expect(isComplete(ws)).toBe(true);

    const event = yield* Schema.decodeUnknownEffect(ExecutionEvent)({ _tag: "ExecutionIdle" });
    expect(event._tag).toBe("ExecutionIdle");
  }),
);
