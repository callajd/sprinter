/**
 * Branded identifier types for the owned read model.
 *
 * Each ID is a non-empty string with a nominal {@link Schema.brand} so the
 * type checker rejects passing (say) an `EpicId` where a `WorkstreamId` is
 * required, even though both are structurally `string`. Branding adds no
 * runtime check beyond non-emptiness — it narrows the TypeScript type only
 * (INV-NAMING: owned types, plain names).
 */
import { Schema } from "effect";

/** Identifies a {@link Workstream} — the top of the `Workstream ⊃ Epic ⊃ Issue` hierarchy. */
export const WorkstreamId = Schema.NonEmptyString.pipe(Schema.brand("WorkstreamId"));
export type WorkstreamId = (typeof WorkstreamId)["Type"];

/** Identifies an {@link Epic} — a related set of Issues within a workstream. */
export const EpicId = Schema.NonEmptyString.pipe(Schema.brand("EpicId"));
export type EpicId = (typeof EpicId)["Type"];

/** Identifies an {@link Issue} — one ~PR-sized unit of code work. */
export const IssueId = Schema.NonEmptyString.pipe(Schema.brand("IssueId"));
export type IssueId = (typeof IssueId)["Type"];

/** Identifies a {@link Job} — one bounded cognitive task (1 Job = 1 session = 1 transcript = 1 PR). */
export const JobId = Schema.NonEmptyString.pipe(Schema.brand("JobId"));
export type JobId = (typeof JobId)["Type"];

/** Identifies a {@link Session} — one agent run executing a {@link Job}. */
export const SessionId = Schema.NonEmptyString.pipe(Schema.brand("SessionId"));
export type SessionId = (typeof SessionId)["Type"];

/**
 * Identifies an {@link Agent} — a member of the REGISTRY layer: owned, global,
 * and scoped to NO repository. Because the registry is append-only, an `AgentId`
 * identifies one immutable REVISION: editing an agent mints a NEW id whose record
 * points back at the previous one through `supersedes`.
 */
export const AgentId = Schema.NonEmptyString.pipe(Schema.brand("AgentId"));
export type AgentId = (typeof AgentId)["Type"];

/**
 * Identifies one STORE GENERATION — the lifetime of a single durable store, from
 * the moment its schema is created to the moment it is dropped and recreated.
 *
 * It is an OWNED, provider-neutral identifier (no SQL, no file path, no version
 * number leaks through it), which is what lets it appear on the daemon↔client
 * contract alongside the rest of the read model (INV-PORT). It is minted FRESH
 * every time the schema is created, so two generations never share one — that is
 * the whole point: durable offsets are only comparable WITHIN a generation, so a
 * cursor is meaningful only when paired with the generation it was minted in.
 *
 * A generation ends only by drop-and-recreate (`SCHEMA_VERSION`,
 * `packages/state/src/sqlite.ts` — INV-FRESH never migrates), and the schema is
 * applied at store construction, so a new generation is observable only across a
 * daemon RESTART. It is opaque: nothing may parse it, order it, or infer age from
 * it — the only defined operation is equality.
 */
export const StoreGenerationId = Schema.NonEmptyString.pipe(Schema.brand("StoreGenerationId"));
export type StoreGenerationId = (typeof StoreGenerationId)["Type"];
