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
