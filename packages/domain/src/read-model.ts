/**
 * The owned domain read model — `Workstream ⊃ Epic ⊃ Issue`, plus `Job` and
 * `Session`, as Effect `Schema` (architecture §2).
 *
 * These are OUR types with plain names (INV-NAMING). They encode the
 * `workstream → epic → Issue → PR` mapping structurally: a workstream lists its
 * epics, an epic names its parent workstream and lists its issues, an issue
 * names its parent epic and carries the one PR that closes it. The execution
 * unit is a {@link Job} — 1 Job = 1 session = 1 transcript = 1 PR — so a job
 * carries the session running it, its transcript reference, and its PR.
 *
 * These schemas are pure descriptions of shape; they reference no backing store
 * or running instance (INV-PORT).
 */
import { Schema } from "effect";
import { EpicId, IssueId, JobId, SessionId, WorkstreamId } from "./ids.ts";
import { PositiveInt } from "./numeric.ts";

/**
 * Lifecycle status shared by the planning nodes `Workstream` and `Epic`: work is
 * queued (`pending`), being driven (`active`), reached its terminal state
 * (`done`), or waiting on an unmet dependency (`blocked`).
 */
export const WorkStatus = Schema.Literals(["pending", "active", "done", "blocked"]);
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

/** Session lifecycle status. */
export const SessionStatus = Schema.Literals([
  "starting",
  "active",
  "idle",
  "interrupted",
  "completed",
  "failed",
]);
export type SessionStatus = (typeof SessionStatus)["Type"];

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
 * A Workstream: a related set of Epics with one spec and one repo — the
 * top-level unit driven to done. Repo-scoped per D14 (`repo` is the bound
 * repository; cross-repo work is many workstreams).
 */
export const Workstream = Schema.Struct({
  id: WorkstreamId,
  name: Schema.NonEmptyString,
  repo: Schema.NonEmptyString,
  status: WorkStatus,
  epics: Schema.Array(EpicId),
});
export type Workstream = (typeof Workstream)["Type"];

/**
 * A Job: one bounded cognitive task, run as one {@link Session}, producing one
 * durable transcript, paired 1:1 with one PR. Names the issue it advances, and —
 * as it runs — carries the session executing it, its transcript reference, and
 * its PR.
 */
export const Job = Schema.Struct({
  id: JobId,
  issueId: IssueId,
  kind: JobKind,
  status: JobStatus,
  sessionId: Schema.optionalKey(SessionId),
  transcriptRef: Schema.optionalKey(Schema.NonEmptyString),
  pr: Schema.optionalKey(PullRequestRef),
});
export type Job = (typeof Job)["Type"];

/** A Session: one agent run executing a {@link Job}. */
export const Session = Schema.Struct({
  id: SessionId,
  jobId: JobId,
  status: SessionStatus,
});
export type Session = (typeof Session)["Type"];

/**
 * The terminal result of a {@link Job}, captured when its {@link Session} settles
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
 * This is a pure description of shape; it references no runner, session instance,
 * or backing store (INV-PORT). The Job runner (AE3) maps a settled session's
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

/** True when an issue is closed by a merged PR — i.e. its work is fully landed. */
export const isIssueLanded = (issue: Issue): boolean =>
  issue.status === "done" && issue.pr?.merged === true;
