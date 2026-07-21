/**
 * The owned domain read model — `Workstream ⊃ Epic ⊃ Issue`, plus `Job` and
 * `Execution`, as Effect `Schema` (architecture §2).
 *
 * These are OUR types with plain names (INV-NAMING). They encode the
 * `workstream → epic → Issue → PR` mapping structurally: a workstream lists its
 * epics, an epic names its parent workstream and lists its issues, an issue
 * names its parent epic and carries the one PR that closes it. The unit of work
 * is a {@link Job} — one bounded cognitive task, paired 1:1 with one PR — and it
 * is advanced by a TREE of {@link Execution}s, each one agent's continuous run
 * producing exactly one {@link Transcript}. (A job carried exactly ONE execution
 * until DE2.2; `parent` is what makes the relation a tree, and DE2.4 replaces
 * `Job` itself with the `Session` that owns the tree.)
 *
 * These schemas are pure descriptions of shape; they reference no backing store
 * or running instance (INV-PORT).
 */
import { Schema } from "effect";
import { AgentId, EpicId, ExecutionId, IssueId, JobId, RepositoryId, WorkstreamId } from "./ids.ts";
import { NonNegativeInt, PositiveInt } from "./numeric.ts";

/**
 * Lifecycle status shared by the planning nodes `Workstream` and `Epic`: work is
 * queued (`pending`), being driven (`active`), completed (`done`), waiting on an
 * unmet dependency (`blocked`), or deliberately stopped before completion
 * (`cancelled`). `done` and `cancelled` are BOTH terminal, but distinct: a
 * cancelled node was abandoned, not finished, so the board and roll-up render it
 * apart from a completed one ({@link isTerminal} / {@link isComplete}).
 */
export const WorkStatus = Schema.Literals(["pending", "active", "done", "blocked", "cancelled"]);
export type WorkStatus = (typeof WorkStatus)["Type"];

/**
 * Issue lifecycle: not yet dispatchable (`pending`), dependencies met and
 * dispatchable (`ready`), an agent is working it (`in_progress`), its PR is open
 * awaiting review (`in_review`), the PR merged and the issue closed (`done`), or
 * held on a failure/conflict (`blocked`).
 */
export const IssueStatus = Schema.Literals([
  "pending",
  "ready",
  "in_progress",
  "in_review",
  "done",
  "blocked",
]);
export type IssueStatus = (typeof IssueStatus)["Type"];

/**
 * Job kinds are an open set; the daemon core is agnostic to the kind (kind lives
 * in per-kind handlers). This enumerates the kinds modelled today (architecture
 * §2); it grows as handlers are added.
 */
export const JobKind = Schema.Literals([
  "implement",
  "review",
  "resolve-conflict",
  "address-findings",
  "plan",
]);
export type JobKind = (typeof JobKind)["Type"];

/** Job execution status. */
export const JobStatus = Schema.Literals(["queued", "running", "succeeded", "failed", "cancelled"]);
export type JobStatus = (typeof JobStatus)["Type"];

/**
 * Who holds the turn in an {@link Execution} — a CLOSED axis, and the ONLY axis
 * `mode` has (a string would admit a third, undefined character):
 *
 * - `interactive` — the HUMAN holds the turn; the agent yields at the end of each one.
 * - `autonomous` — the AGENT holds the turn and yields only when it is blocked.
 *
 * It lives on the EXECUTION and nowhere above it (INV-MODE): an interactive session
 * spawns autonomous subagents and an autonomous one escalates to a human, so a
 * session-level mode would be a second source of truth for a question only the
 * individual run can answer. A session's character is the mode of its ROOT execution
 * — DERIVED by reading that execution, never stored (INV-DERIVED).
 *
 * The wire tokens are lower-case, matching every other `Schema.Literals` set in this
 * module (`WorkStatus`, `JobStatus`, …); the spec spells them `Interactive` /
 * `Autonomous`, which is the same closed pair.
 *
 * NOT to be confused with `ExecutionInput.mode` (`{@link ./execution.ts}`), which is a
 * property of ONE message driven into a run — is this a fresh prompt, a mid-turn steer,
 * or a follow-up — and is never stored. This one is a property of the RUN, and it is the
 * only `mode` any table holds.
 */
export const ExecutionMode = Schema.Literals(["interactive", "autonomous"]);
export type ExecutionMode = (typeof ExecutionMode)["Type"];

/**
 * An execution's transcript, as TWO DISTINCT TYPES rather than one type with a flag
 * (D4 / INV-ENFORCE):
 *
 * - {@link LiveTranscript} — an OPEN offset range. The execution is still running, so
 *   the transcript has no last entry yet and a reader that wants to be current must
 *   TAIL it (subscribe to `executionEvents`).
 * - {@link SealedTranscript} — the run has ended and `[0, lastOffset]` is a settled,
 *   immutable PREFIX of its transcript, so a reader that has read it once may cache it
 *   forever. `lastOffset` is a LOWER BOUND on the extent, not a claim that no entry
 *   exists beyond it — see below.
 *
 * The difference is carried by the TYPE, and neither case carries a boolean saying
 * which it is. That is the whole point: a tail subscription must be UNREPRESENTABLE on
 * a sealed transcript rather than accepted and refused at runtime, so the affordances
 * DE4.2 builds (tail / cache / interrupt) take {@link LiveTranscript} or
 * {@link SealedTranscript} in their signature and a mismatch is a COMPILE error. A
 * single `Transcript { sealed: boolean }` would have made every one of those a runtime
 * check that a sibling code path can forget.
 *
 * Liveness is therefore read off the transcript ({@link isExecutionLive}) and is the
 * ONLY place it is written down: {@link Execution} carries no status enum beside it,
 * because two fields that must agree are one value (INV-ENFORCE / INV-SUM).
 *
 * `lastOffset` is a coordinate in the CURRENT store generation, exactly like every other
 * durable offset, so it is interpretable only alongside `Snapshot.generation`. It is `0`
 * for a run that produced no durable entry at all (durable offsets are strictly `> 0`),
 * which is a valid sealed transcript: an empty one.
 *
 * CONTRACT — `lastOffset` is a LOWER BOUND on the transcript's extent, not an upper one.
 * `[0, lastOffset]` is guaranteed COMPLETE and IMMUTABLE, which is the whole cacheability
 * claim and is unconditionally true (entries are immutable and per-execution offsets
 * never reset, so a cached prefix can never be invalidated). What it does NOT guarantee
 * is that the durable log holds nothing beyond it. Three ways it can understate, none of
 * them a defect a reader can be shielded from:
 *
 * - the seal reads the extent from the durable log, and a TRANSIENT read failure falls
 *   back to `0` rather than refusing to seal — an execution left LIVE because a read
 *   hiccuped would look forever-running to every liveness gate, which is worse than an
 *   understated extent (`packages/job/src/job-runner.ts`, and the same fallback in
 *   `startup-reconcile.ts`);
 * - the durable-append fold is bounded by the execution's terminal
 *   (`Stream.interruptWhen(handle.result)`), so an append still in flight when the
 *   terminal resolves can land AFTER the extent is read;
 * - a per-append `StateStoreError` is absorbed so one hiccup does not truncate the rest
 *   of the fold, so a dropped entry leaves a gap the extent never reflects.
 *
 * A reader must therefore treat "sealed" as "this prefix is final", never as "this is
 * the whole transcript" — i.e. it may cache what it has read and must still be willing
 * to be handed more. That is the SAME discipline the re-dispatch consequence below
 * already demands, which is why one rule covers both rather than two.
 *
 * KNOWN CONSEQUENCE — a seal is per-RUN, not per-id. A re-dispatch of the same Job
 * re-attaches the SAME execution id (issue #77's merged-transcript decision: retries
 * APPEND to one durable log rather than segmenting it), so a sealed transcript can
 * become live again with a strictly greater extent. What a reader cached is never
 * invalidated by that — entries are immutable and offsets never reset, so the cached
 * `[0, lastOffset]` prefix stays exactly correct; only the extent grows. DE2.4 is where
 * a run gains its own identity under a `Session`, at which point a seal becomes final.
 */
export const Transcript = Schema.TaggedUnion({
  LiveTranscript: {},
  SealedTranscript: { lastOffset: NonNegativeInt },
});
export type Transcript = (typeof Transcript)["Type"];

/**
 * The OPEN transcript of a RUNNING execution — see {@link Transcript}. A distinct
 * TYPE, not a flagged variant: it is what a tail subscription takes, so a sealed
 * transcript cannot be handed to one.
 */
export const LiveTranscript = Transcript.cases.LiveTranscript;
export type LiveTranscript = (typeof LiveTranscript)["Type"];

/**
 * The transcript of a SETTLED execution — see {@link Transcript}. `[0, lastOffset]` is
 * a final, immutable prefix and is therefore cacheable; `lastOffset` is a LOWER BOUND on
 * the extent, so a reader may cache what it has and must still accept more. It has no
 * tail, which is expressed by it not being a {@link LiveTranscript} rather than by a
 * runtime refusal.
 */
export const SealedTranscript = Transcript.cases.SealedTranscript;
export type SealedTranscript = (typeof SealedTranscript)["Type"];

/**
 * A reference to the pull request that closes an {@link Issue} (and is produced
 * by its {@link Job}) — the leaf of the `workstream → epic → Issue → PR` mapping.
 * GitHub holds only Issues and the PRs that close them (D13).
 */
export const PullRequestRef = Schema.Struct({
  number: PositiveInt,
  url: Schema.NonEmptyString,
  merged: Schema.Boolean,
});
export type PullRequestRef = (typeof PullRequestRef)["Type"];

/**
 * An Issue: one ~PR-sized feature. Names its parent epic, lists the issues it
 * depends on (the dependency DAG edges), and — once opened — carries the one PR
 * that closes it.
 */
export const Issue = Schema.Struct({
  id: IssueId,
  epicId: EpicId,
  number: PositiveInt,
  title: Schema.NonEmptyString,
  status: IssueStatus,
  dependsOn: Schema.Array(IssueId),
  pr: Schema.optionalKey(PullRequestRef),
});
export type Issue = (typeof Issue)["Type"];

/** An Epic: a related set of Issues. Names its parent workstream and lists its issues. */
export const Epic = Schema.Struct({
  id: EpicId,
  workstreamId: WorkstreamId,
  name: Schema.NonEmptyString,
  status: WorkStatus,
  issues: Schema.Array(IssueId),
});
export type Epic = (typeof Epic)["Type"];

/**
 * A Workstream: a related set of Epics with one spec and one repository — the
 * top-level unit driven to done. Repo-scoped per D14 (cross-repo work is many
 * workstreams).
 *
 * `repositoryId` REFERENCES a stored {@link Repository} (DE1.2). It replaced the bare
 * `repo: string` this node used to carry, and the difference is not cosmetic: a string
 * is not an identity, so two spellings of one repository were two anchors, nothing
 * could be referenced from it, and no constraint could be stated about it. The
 * reference is a real `FOREIGN KEY` in the store, so a workstream naming a repository
 * that was never observed is REJECTED at the write rather than stored and reconciled
 * later (INV-ENFORCE).
 */
export const Workstream = Schema.Struct({
  id: WorkstreamId,
  name: Schema.NonEmptyString,
  repositoryId: RepositoryId,
  status: WorkStatus,
  epics: Schema.Array(EpicId),
});
export type Workstream = (typeof Workstream)["Type"];

/**
 * A Job: one bounded cognitive task, run as one {@link Execution}, producing one
 * durable transcript, paired 1:1 with one PR. Names the issue it advances, and —
 * as it runs — carries the execution running it, its transcript reference, and
 * its PR.
 */
export const Job = Schema.Struct({
  id: JobId,
  issueId: IssueId,
  kind: JobKind,
  status: JobStatus,
  executionId: Schema.optionalKey(ExecutionId),
  transcriptRef: Schema.optionalKey(Schema.NonEmptyString),
  pr: Schema.optionalKey(PullRequestRef),
});
export type Job = (typeof Job)["Type"];

/**
 * An Execution: ONE AGENT'S CONTINUOUS RUN, producing exactly one transcript.
 *
 * - `jobId` — the work this run advances. **Only until DE2.4** (D1): the target model
 *   links an execution to the `Session` that owns it, but `Session` (the unit of work)
 *   does not exist until DE2.4, which depends on THIS task. Rather than ship a dangling
 *   or nullable link, the execution keeps the `jobId` it already had, as a real
 *   `FOREIGN KEY` into `job (id)`, and DE2.4 re-points it at `sessionId` when `Session`
 *   replaces `Job`. Referential integrity therefore holds at EVERY intermediate state,
 *   which is what INV-ENFORCE is for.
 * - `agentId` — the exact {@link Agent} REVISION that ran, a `FOREIGN KEY` into the
 *   append-only registry. Because the registry never rewrites a revision, a historical
 *   execution always resolves to the agent that actually ran it; because the link is a
 *   real key, an execution naming an agent that was never registered is UNSTORABLE, and
 *   a `SCHEMA_VERSION` reset drops `agent` and `execution` TOGETHER, so a DANGLING
 *   `agentId` is unconstructible rather than merely unlikely (D2 / D3).
 * - `parent` — the execution that SPAWNED this one, absent on a root. Executions form a
 *   TREE: a `FOREIGN KEY` onto this same table means a child cannot name a parent that
 *   is not already stored, so a dangling parent is unstorable and no write ORDER closes
 *   a cycle of length ≥ 2; the one edge a referential constraint accepts — a
 *   self-parent — is rejected by the `StateStore` port. That is the `supersedes` lesson
 *   from issue #85: three lineage rules were each defeated by a reordered write until a
 *   foreign key made the precondition unconstructible.
 * - `mode` — who holds the turn ({@link ExecutionMode}). Per-execution, never above it
 *   (INV-MODE).
 * - `transcript` — {@link Transcript}: the LIVE or SEALED transcript this run produces,
 *   as two distinct types. It is also where LIVENESS lives ({@link isExecutionLive});
 *   there is deliberately NO status enum beside it (INV-SUM / INV-LIFECYCLE — an
 *   execution stores no lifecycle its transcript already determines, and a settled run's
 *   OUTCOME belongs to the work it advanced, which is the `Job`'s `status`).
 *
 * `Execution` is OWNED, not observed from an external system, so it carries no
 * `observedAt` (INV-OBSERVED).
 */
export const Execution = Schema.Struct({
  id: ExecutionId,
  jobId: JobId,
  agentId: AgentId,
  parent: Schema.optionalKey(ExecutionId),
  mode: ExecutionMode,
  transcript: Transcript,
});
export type Execution = (typeof Execution)["Type"];

/**
 * True while an execution is still RUNNING — i.e. its transcript is open
 * ({@link LiveTranscript}). This is the ONLY expression of liveness in the model:
 * there is no `ExecutionStatus` to consult and no second field to keep in agreement
 * with the transcript (INV-SUM).
 */
export const isExecutionLive = (execution: Execution): boolean =>
  execution.transcript._tag === "LiveTranscript";

/**
 * The terminal result of a {@link Job}, captured when its {@link Execution} settles
 * — the minimal, OPEN result envelope (D6). The daemon core treats it as opaque;
 * per-kind handlers own and interpret the `payload`. It carries only:
 *
 * - `status` — the terminal outcome, reusing {@link JobStatus}. A settled job is
 *   always one of its terminal values (`succeeded` / `failed`); the envelope
 *   imposes no tighter constraint (D6: minimal constraints).
 * - `payload` — optional, open, JSON-serialisable data whose shape the producing
 *   Job kind owns and narrows (`unknown` here; the core never inspects it).
 * - `error` — optional, neutral, human-readable failure detail on a failed job.
 *
 * This is a pure description of shape; it references no runner, execution instance,
 * or backing store (INV-PORT). The Job runner (AE3) maps a settled execution's
 * outcome onto this envelope.
 */
export const JobResult = Schema.Struct({
  status: JobStatus,
  payload: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.String),
});
export type JobResult = (typeof JobResult)["Type"];

/** True when a planning node (`Workstream`/`Epic`) has reached the terminal `done` status. */
export const isComplete = (node: Workstream | Epic): boolean => node.status === "done";

/**
 * True when a planning node has reached ANY terminal status — completed (`done`)
 * OR cancelled. A terminal node is settled: the status roll-up never resurrects or
 * overwrites it (a cancelled node stays cancelled even once its Issues land). This
 * is broader than {@link isComplete}, which is `done`-only — `cancelled` is
 * terminal-but-not-`done`.
 */
export const isTerminal = (node: Workstream | Epic): boolean =>
  node.status === "done" || node.status === "cancelled";

/** True when an issue is closed by a merged PR — i.e. its work is fully landed. */
export const isIssueLanded = (issue: Issue): boolean =>
  issue.status === "done" && issue.pr?.merged === true;
