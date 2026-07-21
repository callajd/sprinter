/**
 * The owned REGISTRY layer — global entities scoped to NO repository.
 *
 * {@link Agent} is its only member today. The registry is what makes "the agents
 * used in this repo" a FOLD over that repository's executions rather than a
 * stored per-repo list (INV-DERIVED): an `Agent` has no repository and no
 * workstream, so `Agent 1:∗ Execution` holds ACROSS ALL REPOS and the per-repo
 * view is computed from the executions that reference it, never persisted.
 *
 * The registry is APPEND-ONLY, and a stored revision is IMMUTABLE: once written,
 * its columns are never rewritten. That single rule generates both mutating
 * operations, and there are only these two:
 *
 * - **Edit** ⇒ append a NEW revision, under a NEW id, whose `supersedes` names the
 *   current head of the lineage. The record being replaced is untouched, so every
 *   historical execution keeps resolving to the exact agent revision that ran it.
 *   An edit MAY change any of `name` / `model` / `version` / `tools` — that is what
 *   an edit IS.
 * - **Retire** ⇒ ALSO a NEW revision under a NEW id, carrying BOTH `supersedes`
 *   (naming the head it retires) AND a `retiredAt` stamp. Retirement is NEVER a
 *   same-id stamp on an existing row — that would be a mutation, and it would make
 *   the retirement indistinguishable from the revision it retired. A retiring
 *   revision without `supersedes` is not a retirement at all but a NEW lineage that
 *   was born retired.
 *
 * Nothing is ever removed, so NO delete is exposed anywhere on the path — not on
 * the `StateStore` port, not on the contract.
 *
 * ## A retirement is LIFECYCLE-ONLY — it may not rewrite content
 *
 * A retiring revision MUST carry the SAME `name`, `model`, `version` and `tools` as
 * the head it retires; ONLY `supersedes` and `retiredAt` differ. Retirement says
 * "this lineage is out of service", not "this lineage is out of service AND its
 * tools were always empty". A retirement that also changed content would be an
 * EDIT and a retirement fused into one indistinguishable append: a reader walking
 * back from the retiring revision could no longer tell which of the two things
 * happened, and the retired head's content would appear to have changed at the
 * moment it stopped being used. Do BOTH by appending the edit first and retiring
 * the edited revision.
 *
 * This is not a convention — the `StateStore` port ENFORCES it on `putAgent` (it
 * reads the superseded revision and rejects a retiring append whose non-lifecycle
 * fields differ). And it is enforced UNCONDITIONALLY, not only when the superseded
 * revision happens to be stored: `supersedes` is a real referential constraint in
 * the adapter, so a revision naming an ABSENT predecessor cannot be written at all.
 * That is what makes the rule order-independent — appending the retirement FIRST,
 * with nothing yet to compare it against, is not a way around it.
 *
 * ## Durability is scoped to a STORE GENERATION
 *
 * "Nothing is ever removed" is a property of the REGISTRY's operations, and it holds
 * for the life of a store generation: within one generation, revisions are immutable
 * and the whole history is preserved, so a past execution resolves to the exact
 * revision that ran it. It is NOT an eternal guarantee. `SCHEMA_VERSION`
 * (`packages/state/src/sqlite.ts`) is the store's ONLY evolution mechanism and it
 * never migrates: bumping it DROPS the database and recreates it, starting a NEW
 * generation and discarding the previous one entirely. `putAgent` is the sole source
 * of truth for registry content — there is no manifest and no config to re-seed from
 * — so that reset is real, permanent data loss, ACCEPTED because `DMR` treats the
 * store as greenfield pre-release.
 *
 * A DANGLING `Execution.agentId` was recorded here as the downstream consequence of
 * that reset. DE2.2 RESOLVED it, and not by asking anyone to be careful:
 * `execution.agentId` is a real FOREIGN KEY onto this table, and a reset drops EVERY
 * table in one sweep — so `agent` and `execution` go together and no execution can
 * survive referencing a discarded revision. Within a generation the key refuses the
 * write; across one there is nothing left on either side. A dangling `agentId` is
 * UNCONSTRUCTIBLE, not merely unlikely, so nothing downstream has to model a
 * possibly-absent agent (INV-ENFORCE).
 *
 * What the reset still costs is a PRODUCT question, not an integrity one: SHOULD the
 * registry's history survive a remodel? While the store is pre-release greenfield, no
 * — the history is worth exactly what the executions that reference it are worth, and
 * those go with it. That is the whole of the tension DE1.1 recorded; there is no
 * re-derivation source to invent, and inventing one (a manifest re-seeded on open)
 * would make `putAgent` stop being the sole source of truth for registry content.
 *
 * That generation boundary is also the ONLY bound on the registry's SIZE. Append-only
 * with no delete means every revision ever written stays — superseded and retired ones
 * are precisely what the model exists to keep — and there is no pruning, no compaction
 * and no retention window anywhere on the path: the store's `listAgents` reads the
 * whole table, the daemon's `snapshot` ships all of it on every connect, and every
 * client retains all of it. Growth is therefore unbounded within a generation, and a
 * `SCHEMA_VERSION` bump is the only thing that ever shrinks it (by discarding it
 * entirely). Accepted at pre-release scale — a registry grows by human edits, not by
 * execution volume — and recorded here so it is a known cost rather than a surprise.
 * If it ever needs bounding, the remedy is a narrower READ, never a delete.
 *
 * ## The `supersedes` chain is an ACYCLIC path, BY CONSTRUCTION
 *
 * Every `supersedes` chain is a finite path ending at an original revision: a
 * revision may not supersede itself, may not supersede a revision that
 * (transitively) supersedes it, and may not name a revision that does not exist.
 *
 * This used to be stated as a PRECONDITION the writer owed, with only the
 * self-reference case checked — and an unenforceable obligation is one that gets
 * violated. It is now structural, and both halves are needed:
 *
 * - `supersedes` is a REFERENTIAL constraint in the store: a revision can only name
 *   a predecessor that is ALREADY stored. Closing a cycle of length ≥ 2 requires an
 *   edge pointing at a revision that does not exist yet, so no write ORDER produces
 *   one — and a dangling `supersedes` is unstorable for the same reason.
 * - the self-reference (the one edge a referential constraint accepts, since a row
 *   satisfies a key against itself) is rejected by the `StateStore` port.
 *
 * So a consumer walking the chain backwards ({@link isOriginalRevision}) walks a
 * structure the STORE keeps well-formed: the walk terminates, and it terminates at a
 * revision that really is the original.
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
 *   revision. The append-only edit link (never a mutation in place). It names a
 *   DIFFERENT, ALREADY-STORED revision and cannot close a cycle — see the module
 *   docstring; that is enforced, not assumed.
 * - `retiredAt` — the instant the agent was retired, absent while it is in
 *   service. Retirement is a stamp, NOT a status enum (INV-SUM), and NOT a delete:
 *   a retired agent's record stays readable — for the life of the store generation
 *   — so past executions still resolve. A RETIRING revision carries `supersedes` as
 *   well, and carries the SAME `name`/`model`/`version`/`tools` as the head it
 *   retires: it is a new LIFECYCLE-ONLY revision, never a stamp applied to an
 *   existing row and never a content rewrite (see the module docstring).
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
 * The CONTENT of an agent revision — what an agent IS (`name` / `model` / `version` /
 * `tools`), with no identity and no lineage.
 *
 * It exists because a PRODUCTION WRITER declares content, not identity: the runner
 * adapter knows which agent it dispatches through, but the revision's `id` is not its
 * to choose — it is DERIVED from this content, so that "identical content is an
 * idempotent no-op, differing content is a new revision" holds by construction rather
 * than by a writer remembering to mint a new id (see `@sprinter/job`'s
 * `agent-registration.ts`, the registry's first production writer).
 *
 * These are the exact fields a RETIREMENT may not rewrite (see the module docstring),
 * which is the same partition read from the other side.
 */
export const AgentContent = Schema.Struct({
  name: Schema.NonEmptyString,
  model: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
  tools: Schema.Array(Schema.NonEmptyString),
});
export type AgentContent = (typeof AgentContent)["Type"];

/**
 * True when an agent has been retired — i.e. it carries a `retiredAt` stamp. This
 * is the ONLY way retired-ness is expressed; there is no `AgentStatus` to consult
 * and no second source of truth to keep in sync (INV-SUM).
 */
export const isRetired = (agent: Agent): boolean => agent.retiredAt !== undefined;

/**
 * True when `agent` is the FIRST revision of its lineage — it replaces nothing.
 * Later revisions carry `supersedes`, so the chain is walked backwards from any
 * revision until this returns true. This predicate reads ONE revision and cannot
 * observe the chain, but the walk built out of it terminates for every history the
 * store can hold: `supersedes` is acyclic BY CONSTRUCTION (see the module
 * docstring), not by a writer's promise.
 *
 * KNOWN CONSEQUENCE — in a live daemon this is `true` of nearly everything. The only
 * production writer (`agent-registration.ts`, DE2.2) appends ORIGINAL revisions only:
 * it never sets `supersedes`, because the runner knows the content it is about to run
 * but not whether a person meant that content to REPLACE something. Identity is derived
 * from content, so editing an agent definition (`LOCAL_PI_AGENT`, say) does not extend a
 * lineage — it starts a NEW one, unlinked to the old. `Snapshot.agents` therefore
 * accumulates unrelated single-revision lineages, and the helpers below are CORRECT
 * about them in a way that may read as surprising: {@link isLineageRetired} answers for
 * a lineage of one, and the pre-edit and post-edit revisions of "the same" agent are, to
 * every helper here, two different agents that happen to share a `name`. Linking them is
 * a human operation on the registry surface — nothing on the dispatch path can infer it,
 * and no helper here should pretend otherwise (`name` is not an identity).
 */
export const isOriginalRevision = (agent: Agent): boolean => agent.supersedes === undefined;

/**
 * True when `agent`'s LINEAGE has been retired — i.e. `agent` itself carries a
 * `retiredAt` stamp, or some revision in `all` retires it, directly or through a
 * chain of later revisions.
 *
 * {@link isRetired} answers a question about ONE RECORD, and under append-only
 * semantics that is deliberately NOT the same question as "is this agent still in
 * service". Retiring `agt-2` appends a NEW revision `agt-3` that carries
 * `supersedes: "agt-2"` and the stamp; `agt-2` itself is immutable and stays
 * un-stamped forever. So `isRetired(agt-2)` is `false` even after the lineage is
 * retired, and it is correct that it is: the record really was never retired — its
 * LINEAGE was.
 *
 * Answering the lineage question needs the REVERSE of the `supersedes` link, which
 * no single revision carries and no store read exposes. This helper derives that
 * reverse index from a `listAgents`-shaped collection — the whole registry,
 * superseded and retired revisions included, which is exactly what `listAgents` and
 * `Snapshot.agents` hand over — and walks FORWARD from `agent` along it until it
 * reaches a stamped revision or the head of the lineage.
 *
 * Revisions in `all` that belong to other lineages are ignored. The answer does NOT
 * depend on `all`'s order: each revision has at most one successor, so there is exactly
 * one forward path. That is not a convention this helper hopes its callers honour — a
 * revision superseded TWICE is unstorable (the adapter's `agent_supersedes` UNIQUE
 * index), so the forked history that would make the walk pick a branch, and the answer
 * follow the collection's order, cannot reach here from the store at all.
 *
 * COST: this rebuilds the reverse index on EVERY call — O(n) in `all` per agent, and
 * therefore O(n²) for a fold over the whole registry (e.g. a UI listing live agents by
 * calling this once per revision). That is fine for one lookup and fine at the scale
 * the registry is bounded to, and it is why the index is not cached here: caching it
 * would tie a pure predicate to a lifetime it cannot see. If a caller ever folds this
 * across the registry, the remedy is to build the successor index ONCE at the call
 * site and pass it in (a sibling taking a prepared index), not to memoize inside.
 *
 * Termination: the walk visits each revision at most once (it stops on re-visiting
 * one). Nothing from the STORE can be cyclic — `supersedes` is acyclic by
 * construction (see the module docstring) — but this takes an arbitrary `Iterable`,
 * so it terminates on a hand-built cyclic collection too rather than trusting its
 * caller to have come from a store.
 */
export const isLineageRetired = (agent: Agent, all: Iterable<Agent>): boolean => {
  const successors = new Map<AgentId, Agent>();
  for (const revision of all) {
    if (revision.supersedes !== undefined && !successors.has(revision.supersedes)) {
      successors.set(revision.supersedes, revision);
    }
  }
  const seen = new Set<AgentId>();
  let current: Agent | undefined = agent;
  while (current !== undefined && !seen.has(current.id)) {
    if (isRetired(current)) return true;
    seen.add(current.id);
    current = successors.get(current.id);
  }
  return false;
};
