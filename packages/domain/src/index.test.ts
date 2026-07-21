import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import { isComplete, SessionEvent, Workstream } from "./index.ts";

it.effect("re-exports the read model and session schemas from the barrel", () =>
  Effect.gen(function* () {
    const ws = yield* Schema.decodeUnknownEffect(Workstream)({
      id: "ws-fdn",
      name: "Foundation",
      repositoryId: "repo:github:callajd/sprinter",
      status: "done",
      epics: [],
    });
    expect(isComplete(ws)).toBe(true);

    const event = yield* Schema.decodeUnknownEffect(SessionEvent)({ _tag: "SessionIdle" });
    expect(event._tag).toBe("SessionIdle");
  }),
);
