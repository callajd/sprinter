/**
 * `@sprinter/domain` — owned domain schemas.
 *
 * Scaffold-stage seed for the read model (`Workstream ⊃ Epic ⊃ Issue`, see
 * `conventions.md`). Kept intentionally small; `FE2.1` fleshes out the full
 * model. Present here so the `check` gate exercises a real Effect `Schema`
 * module rather than an empty package.
 */
import { Schema } from "effect";

/** Lifecycle status shared by work-graph nodes. */
export const WorkStatus = Schema.Literals(["pending", "active", "done"]);
export type WorkStatus = (typeof WorkStatus)["Type"];

/** A workstream: the top of the `Workstream ⊃ Epic ⊃ Issue` hierarchy. */
export const Workstream = Schema.Struct({
  id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  status: WorkStatus,
});
export type Workstream = (typeof Workstream)["Type"];

/** Decode untrusted input into a validated `Workstream`, throwing on failure. */
export const decodeWorkstream = Schema.decodeUnknownSync(Workstream);

/** True when a workstream has reached the terminal `done` status. */
export const isComplete = (workstream: Workstream): boolean => workstream.status === "done";
