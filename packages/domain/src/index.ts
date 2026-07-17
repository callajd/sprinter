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
 */
export * from "./ids.ts";
export * from "./read-model.ts";
export * from "./session.ts";
