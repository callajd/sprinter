/**
 * `@sprinter/domain` — the owned domain schemas (Effect `Schema`).
 *
 * Two surfaces, both owned and provider-neutral (INV-NAMING, INV-PORT):
 *
 * - the **read model** — `Workstream ⊃ Epic ⊃ Issue`, plus `Job` and `Session`,
 *   with branded ID types, status enums, and the `workstream → epic → Issue → PR`
 *   mapping ({@link ./read-model.ts}, {@link ./ids.ts}); and
 * - the neutral, maximally reactive **session model** — `SessionEvent`,
 *   `SessionInput`, `UiResponse` ({@link ./session.ts}).
 *
 * Alongside them sits the **registry** layer ({@link ./registry.ts}) — owned,
 * global, scoped to no repository — whose only member today is the append-only
 * `Agent`, and the shared owned instant type `Timestamp` ({@link ./time.ts}).
 */
export * from "./ids.ts";
export * from "./numeric.ts";
export * from "./read-model.ts";
export * from "./registry.ts";
export * from "./session.ts";
export * from "./time.ts";
