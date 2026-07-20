/**
 * The owned REGISTRY layer — global entities scoped to NO repository.
 *
 * {@link Agent} is its only member today. The registry is what makes "the agents
 * used in this repo" a FOLD over that repository's executions rather than a
 * stored per-repo list (INV-DERIVED): an `Agent` has no repository and no
 * workstream, so `Agent 1:∗ Execution` holds ACROSS ALL REPOS and the per-repo
 * view is computed from the executions that reference it, never persisted.
 *
 * The registry is APPEND-ONLY. Editing an agent does not mutate its record: it
 * mints a NEW revision whose `supersedes` points at the one it replaces, so every
 * historical execution keeps resolving to the exact agent revision that ran it.
 * Retirement sets `retiredAt`; it does not remove the record. Consequently NO
 * delete is exposed anywhere on the path — not on the `StateStore` port, not on
 * the contract.
 *
 * Retired-ness is read off `retiredAt`'s PRESENCE ({@link isRetired}) — there is
 * deliberately no `AgentStatus` enum, because a status enum paired with an
 * optional field admits states the domain does not have (`retired` with no
 * `retiredAt`, or an `active` agent carrying one) — INV-SUM.
 *
 * `Agent` is OWNED, not observed from an external system, so it carries no
 * `observedAt` (INV-OBSERVED). Like the read model, this is a pure description of
 * shape: it references no backing store and no running instance (INV-PORT).
 */
import { Schema } from "effect";
import { AgentId } from "./ids.ts";
import { Timestamp } from "./time.ts";

/**
 * An agent in the registry: the identity that RUNS work. Global — it names no
 * repository and no workstream (INV-DERIVED / D14: repo-scoping lives on the
 * work, not on the runner).
 *
 * - `name` — the human-facing name of the agent (e.g. `"implementer"`).
 * - `model` / `version` — the model it drives and the revision of its definition.
 * - `tools` — the tool names it is permitted to use, in declaration order.
 * - `supersedes` — the PREVIOUS revision this record replaces, absent on the first
 *   revision. The append-only edit link (never a mutation in place).
 * - `retiredAt` — the instant the agent was retired, absent while it is in
 *   service. Retirement is a stamp, NOT a status enum (INV-SUM), and NOT a delete:
 *   a retired agent's record stays readable so past executions still resolve.
 */
export const Agent = Schema.Struct({
  id: AgentId,
  name: Schema.NonEmptyString,
  model: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  tools: Schema.Array(Schema.NonEmptyString),
  supersedes: Schema.optionalKey(AgentId),
  retiredAt: Schema.optionalKey(Timestamp),
});
export type Agent = (typeof Agent)["Type"];

/**
 * True when an agent has been retired — i.e. it carries a `retiredAt` stamp. This
 * is the ONLY way retired-ness is expressed; there is no `AgentStatus` to consult
 * and no second source of truth to keep in sync (INV-SUM).
 */
export const isRetired = (agent: Agent): boolean => agent.retiredAt !== undefined;

/**
 * True when `agent` is the FIRST revision of its lineage — it replaces nothing.
 * Later revisions carry `supersedes`, so the chain is walked backwards from any
 * revision until this returns true.
 */
export const isOriginalRevision = (agent: Agent): boolean => agent.supersedes === undefined;
