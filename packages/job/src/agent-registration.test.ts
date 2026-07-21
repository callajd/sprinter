/**
 * The `Agent` registry's first PRODUCTION writer (DE2.2 / D2) — content-addressed
 * identity, and what that buys: an idempotent re-registration, and a CHANGED agent
 * landing as a new revision instead of colliding with the old one under one id.
 */
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { expect } from "vitest";
import type { AgentContent } from "@sprinter/domain";
import { layerMemory, StateStore } from "@sprinter/state";
import { agentIdFor, registerAgent } from "./agent-registration.ts";

const content: AgentContent = {
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read", "edit"],
};

it.effect("derives one id per CONTENT — stable across calls, different for a changed agent", () =>
  Effect.gen(function* () {
    const id = yield* agentIdFor(content);
    expect(id).toMatch(/^agt-[0-9a-f]{32}$/);
    // Same content, derived again: the SAME id. This is what makes re-registration an
    // idempotent no-op rather than a collision.
    expect(yield* agentIdFor({ ...content, tools: ["read", "edit"] })).toBe(id);
    // Every field participates — a changed model, version, name or TOOL SET (order
    // included: tool order is part of an agent's content) derives a different id.
    expect(yield* agentIdFor({ ...content, model: "other" })).not.toBe(id);
    expect(yield* agentIdFor({ ...content, version: "1.0.1" })).not.toBe(id);
    expect(yield* agentIdFor({ ...content, name: "reviewer" })).not.toBe(id);
    expect(yield* agentIdFor({ ...content, tools: ["edit", "read"] })).not.toBe(id);
  }),
);

it.effect("registers the revision that ran, and re-registering the same agent is a no-op", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const first = yield* registerAgent(store, content);
    const again = yield* registerAgent(store, content);
    expect(again).toBe(first);
    // ONE row, not two: `putAgent` sees a byte-identical revision and appends nothing.
    const registry = yield* store.agents.listAgents;
    expect(registry.map((agent) => agent.id)).toStrictEqual([first]);
    expect(registry[0]).toStrictEqual({ id: first, ...content });
  }).pipe(Effect.provide(layerMemory)),
);

it.effect("an EDITED agent lands as a NEW revision — never a refused rewrite of the old id", () =>
  Effect.gen(function* () {
    const store = yield* StateStore;
    const before = yield* registerAgent(store, content);
    // The one failure a self-minted stable id would produce at dispatch time: an id
    // already stored with DIFFERENT content. A content-derived id cannot reach it.
    const after = yield* registerAgent(store, { ...content, version: "1.1.0" });
    expect(after).not.toBe(before);
    const registry = yield* store.agents.listAgents;
    expect(registry.length).toBe(2);
    // Both revisions survive: a past execution still resolves to the exact agent that
    // ran it (the registry is append-only, and this writer never rewrites).
    expect(registry.map((agent) => agent.version).sort()).toStrictEqual(["1.0.0", "1.1.0"]);
  }).pipe(Effect.provide(layerMemory)),
);
