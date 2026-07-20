# Workstream `DMR` — Domain remodel

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** replace the process-shaped read model with a model of *agentic work*
> — a `Session` of `Execution`s against a target — and the state / transition /
> plan layers around it, across `packages/*` and the Swift mirror.
>
> **Prerequisites:** none. The store is treated as **greenfield** — no data is
> preserved, no migration is written.
>
> **Exit:** every state in [Must become unrepresentable](#must-become-unrepresentable)
> fails to typecheck or has no schema to express it.
>
> This spec is self-contained. No other document is required to execute it.

## The model in one paragraph

A **Repository** and its open **Issues** are a *state*. A **PullRequest** is a
*transition* over that state; only a merge advances it. A **Session** is one
bounded unit of agentic work against a *target* — an Issue, a Spec, later others
— and it owns a tree of **Execution**s, each being one agent's continuous run
producing exactly one **Transcript**. An Execution is `Interactive` (the human
holds the turn) or `Autonomous` (the agent holds it and yields only when
blocked). **Spec → Workstream → Epic → Issue** is the *plan*: what Sprinter
intends, as opposed to what the code host currently reports.

## Point A — what exists today

`packages/domain/src/read-model.ts`:

| Type | Shape |
|------|-------|
| `Workstream` | `{ id, name, repo: string, status: WorkStatus, epics: EpicId[] }` |
| `Epic` | `{ id, workstreamId, name, status: WorkStatus, issues: IssueId[] }` |
| `Issue` | `{ id, epicId, number, title, status: IssueStatus, dependsOn: IssueId[], pr?: PullRequestRef }` |
| `Job` | `{ id, issueId, kind: JobKind, status: JobStatus, sessionId?, transcriptRef?: string, pr?: PullRequestRef }` |
| `Session` | `{ id, jobId, status: SessionStatus }` — one OS process, 1:1 with `Job` |
| `PullRequestRef` | `{ number, url, merged: boolean }` — value object, no id |

Enums: `WorkStatus`, `IssueStatus`, `JobKind`, `JobStatus`, `SessionStatus`.
Branded ids (`ids.ts`): `WorkstreamId`, `EpicId`, `IssueId`, `JobId`, `SessionId`.

`packages/state/src/sqlite.ts` tables: `workstream`, `epic`, `issue`,
`issue_dependency`, `job`, `session` (with `UNIQUE session_job(jobId)`),
`event_log`, `session_event_log`. Foreign keys are conventional — indexes only.

`packages/contract/src/rpc.ts`: `Snapshot { workstreams, epics, issues, jobs,
sessions }`; `WorkGraphEvent` tagged union, upsert-only; `WorkstreamPlan { name,
repo, spec: string }`.

Two facts that drive most of this workstream:

- **`Repository` is a port, not an entity** (`packages/repository/src/repository.ts`).
  Repo identity in the domain is the `repo: string` field on `Workstream`.
- **`WorkstreamPlan.spec` is discarded.** `materialize` in
  `packages/daemon/src/rpc-handlers.ts` checks it is non-empty and never reads it
  again; no column stores it, nothing on the Swift side references it.

`Session` today means *one OS process*. Under Point B that concept is renamed
`Execution` and `Session` is reused for the unit of work — see
[DE2.1](#de21--rename-session--execution-process-level).

## Point B — target model

```
── AGENTIC WORK ── owned by Sprinter

Execution = { id, sessionId, agentId, parent?: ExecutionId, mode, transcript }
              // one agent's continuous run; exactly one transcript; forms a tree

mode      = Interactive     // the human holds the turn; the agent yields each turn
          | Autonomous      // the agent holds the turn; yields only when blocked

transcript = LiveTranscript      // open offset range, needs a tail subscription
           | SealedTranscript    // [0, N], immutable, cacheable

Session =                       // one bounded unit of agentic work
  | Running   { id, target, base: CommitSha, root: ExecutionId,
                workspace: Workspace, pullRequest?: PullRequestId }
  | Abandoned { id, target, base, root, reason }
  | Completed { id, target, base, root, product: Product }

target  = OnIssue { issueId } | OnSpec { specId }          // OPEN union — grows
product = MergedPullRequest { pullRequestId }              // OnIssue
        | SpecRevised { specRevisionId }                   // OnSpec

Workspace = { id, sessionId, base: CommitSha,              // owned, ephemeral
              path, branch, diff, processes, ports, tasks }

── STATE ── referenced from the code host; carries observedAt

Repository = { id, host, owner, name, refs: BranchName → CommitSha, observedAt }

Issue =
  | Open      { id, repositoryId, epicId, number, title, dependsOn: IssueId[],
                sessions: SessionId[], observedAt }
  | Fixed     { …, by: PullRequestId }                     // PR observed Merged
  | Dismissed { …, reason: WontFix | Duplicate | Invalid }

── TRANSITION ── referenced; only a merge advances the state

PullRequest = {
  id, repositoryId, number, url,
  target: BranchName,        // where it applies
  base:   CommitSha,         // state it was computed from
  head:   CommitSha,         // state it proposes
  state:  Open | Merged { at: CommitSha } | Closed,
  closes: IssueId[],
  mergeability: Mergeability,
  observedAt
}

Mergeability = {
  applies:  Clean | Conflicted { paths },   // git decides — cheap
  verified: Passed | Failed | NotRun,       // checks decide — expensive
  against:  CommitSha                       // state both were computed on
}

── PLAN ── owned by Sprinter

Spec         = { id, name, head: SpecRevisionId }
SpecRevision = { id, specId, content, supersedes?: SpecRevisionId,
                 boundTo?: { repositoryId, path, commit: CommitSha } }

Workstream = { id, name, repositoryId, derivedFrom: SpecRevisionId,
               epics: EpicId[] }                           // status derived
Epic       = { id, workstreamId, name, issues: IssueId[],  // status derived
               milestone?: Ref<Milestone> }

── REGISTRY ── owned, global, scoped to no repository

Agent = { id, name, model, version, tools,
          supersedes?: AgentId, retiredAt? }               // append-only
```

New branded ids: `RepositoryId`, `PullRequestId`, `SessionId` (re-pointed),
`ExecutionId`, `WorkspaceId`, `AgentId`, `SpecId`, `SpecRevisionId`, `CommitSha`,
`BranchName`.

### The work matrix

`mode` is a **closed** axis; `target` is an **open** one. Every cell is valid; the
diagonal is the common workflow.

|  | `OnSpec` | `OnIssue` |
|---|---|---|
| **Interactive** | collaborative planning | pairing on an issue |
| **Autonomous** | drafting a spec unattended | implementation |

`mode` lives on the **Execution**, not the Session — an interactive session spawns
autonomous subagents, and an autonomous session escalates to a human. A session's
character is the mode of its **root** execution, derived rather than stored.

### Derived, not stored

| Value | Derivation |
|---|---|
| `Issue` open/fixed | membership of the current open set; `Fixed` by a `Merged` PR whose `closes` contains it |
| `Epic` / `Workstream` completion | fold over the issue set |
| pull-request staleness | `tipOf(pr.target) ≠ pr.base` |
| spec drift | `spec.head ≠ workstream.derivedFrom` |
| session character | mode of the root execution |
| **inbox** | outstanding `UiRequestRaised` where `execution.mode = Autonomous` |
| session cost | sum of `Usage` across the execution tree |
| "agents used in this repo" | fold over that repo's executions |

An `Interactive` execution awaiting its turn is **not** an inbox entry — the human
is already in that session. Only a blocked `Autonomous` execution is.
`SessionIdle` likewise reads as *resting* under `Interactive` and *blocked or
stalled* under `Autonomous`; the event stays single and the meaning is read off
the mode.

### Cardinality

`Repository 1:∗ Issue` · `Epic 1:∗ Issue` · `Issue ∗:∗ Issue` (`dependsOn`, a DAG)
· `Issue 1:∗ Session` · `Spec 1:∗ Workstream` (usually 1:1, by convention) ·
`Session 1:∗ Execution` (a tree via `parent`) · `Execution 1:1 Transcript` ·
`Agent 1:∗ Execution` (across all repos) · `PullRequest 1:∗ Issue` (`closes`).

## Repository map

| Path | Role |
|------|------|
| `packages/domain` | Schemas + branded ids. Depends only on `effect`. |
| `packages/contract` | Daemon↔client `RpcGroup` over domain types. |
| `packages/state` | `StateStore` port + SQLite adapter (`sqlite.ts`). |
| `packages/repository` | Code-host port + GitHub adapter + reconciler. |
| `packages/runner` | Pi process/transport adapter. Keeps Pi's own "session" vocabulary — adapter-internal, excluded from the contract. |
| `packages/job` | Execution runner, spawn router. |
| `packages/daemon` | Wiring: rpc-handlers, registries, event journal. |
| `apple/Sprinter/Sources/SprinterContract` | Hand-written Swift mirror of the contract. |
| `apple/Sprinter/Sources/Sprinter{MissionControl,Session,Inspector}` | View models. |

**The path every task takes:** `domain` → `state` → `repository` → `contract` →
`daemon` → `SprinterContract` (+ goldens) → view models. A task is not done until
the Swift side compiles and its goldens pass.

**Gates.** TS: `bun run check` at the repo root (oxfmt, oxlint, `tsc --noEmit`,
vitest + coverage). Swift: `make check` in `apple/Sprinter/`.
**Goldens.** Fixtures in `apple/Sprinter/Tests/SprinterContractTests/Goldens`,
regenerated by `apple/Sprinter/scripts/generate-goldens.ts`.

## Cross-cutting invariants

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | Both gates green — `bun run check` and `make check` | CI |
| `INV-COV` | ≥ 75% line **and** function coverage on the task's modules | coverage threshold inside `check` |
| `INV-NOCAST` | No `as` / non-null `!` / `any` (TS); no force unwrap/cast/try (Swift) | `oxlint` / `SwiftLint --strict` |
| `INV-PIN` | Deps exact-pinned; lockfile committed | `check` |
| `INV-NAMING` | Follows [`conventions.md`](../conventions.md) | review |
| `INV-PORT` | External systems sit behind ports | review |
| `INV-MIRROR` | Every contract change lands with its Swift mirror **and** golden test in the same PR | `check` + goldens |
| `INV-SUM` | No status enum paired with an optional field. Terminal variants carry their payload as required | review + typecheck |
| `INV-DERIVED` | Values in [Derived, not stored](#derived-not-stored) are computed, never persisted | review |
| `INV-OBSERVED` | Every referenced entity carries `observedAt`; owned entities do not | review |
| `INV-FRESH` | No store migration. Bump the schema version; drop and recreate | `sqlite.ts` version constant |
| `INV-TARGET` | Session machinery is target-agnostic. A new `target` variant adds a reference and a `product` — nothing else | review |
| `INV-MODE` | `mode` is per-execution. Nothing stores a session-level mode | review |

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `DE1` | Identity — registry & state anchors | — |
| `DE2` | Agentic work — Execution & Session | `DE1` |
| `DE3` | State, spec & plan | `DE2` |
| `DE4` | Projections & client affordances | `DE3` |

```
DE1 ──► DE2 ──► DE3 ──► DE4
```

---

## Epic `DE1` — Identity: registry & state anchors

### DE1.1 — `Agent` registry
- **Done:** `AgentId` branded; `Agent { id, name, model, version, tools,
  supersedes?, retiredAt? }`; append-only — editing writes a new revision, no
  delete is exposed; no repository or workstream scope; carried in `Snapshot` +
  `WorkGraphEvent`; Swift mirror + golden.
- **Depends on:** —

### DE1.2 — `Repository` entity
- **Done:** `RepositoryId`, `BranchName`, `CommitSha` branded; `Repository { id,
  host, owner, name, refs, observedAt }`; `packages/repository` resolves and
  refreshes it behind the existing port; `Workstream.repo: string` replaced by
  `repositoryId`; Swift mirror + golden.
- **Depends on:** —

### DE1.3 — Retire `SprinterCore/Workstream.swift`
- **Done:** the pre-`SprinterContract` duplicate `Workstream` (unbranded `id:
  String`, three-case `WorkStatus`) is deleted; no references remain.
- **Depends on:** `DE1.2`

---

## Epic `DE2` — Agentic work: Execution & Session

### DE2.1 — Rename `Session` → `Execution` (process level)
- **Done:** every process-level `Session*` identifier in the domain and contract
  becomes `Execution*` — `SessionEvent`→`ExecutionEvent`,
  `SessionInput`→`ExecutionInput`, `SessionNotFound`→`ExecutionNotFound`,
  `sessionEvents`→`executionEvents`, `SessionHandle`/`SessionResult`/
  `SessionRegistry`→`Execution*`, table `session_event_log`→`execution_event_log`;
  `packages/runner` and `pi/wire.ts` keep Pi's "session" wording as adapter-internal
  vocabulary; the name `Session` is left free for DE2.3; Swift mirror + goldens.
- **Depends on:** `DE1.1`

### DE2.2 — `Execution` entity + `mode`
- **Done:** `ExecutionId` branded; `Execution { id, sessionId, agentId, parent?,
  mode, transcript }`; `mode = Interactive | Autonomous`; `LiveTranscript` and
  `SealedTranscript` are distinct types, not one type with a flag;
  `execution_event_log` keyed by `ExecutionId`; `SessionId` (Pi's) confined to
  `packages/runner` and absent from `packages/contract`; `UNIQUE
  session_job(jobId)` dropped; Swift mirror + golden.
- **Depends on:** `DE2.1`

### DE2.3 — `PullRequest` as an entity
- **Done:** `PullRequestId` branded; `PullRequest` and `Mergeability` per Point B;
  `PullRequestRef` and `merged: boolean` deleted; staleness computed as
  `tipOf(target) ≠ base`, never stored; a `Mergeability` whose `against` is behind
  `tipOf(target)` is reported unknown; Swift mirror + golden.
- **Depends on:** `DE1.2`

### DE2.4 — `Session` replaces `Job`
- **Done:** `SessionId` re-pointed at the work unit; `Session` per Point B with
  `target`, `base`, `root`, and the three-variant lifecycle; `Product` union;
  `Completed` carries a product, never an optional pull request; `Job`, the old
  `Session`, `JobStatus`, `SessionStatus`, `JobKind`, `transcriptRef` deleted from
  `packages/*` and `apple/*`; Swift mirror + golden.
- **Depends on:** `DE2.2`, `DE2.3`

### DE2.5 — `Workspace` entity
- **Done:** `WorkspaceId` branded; `Workspace` per Point B, held only by
  `Session.Running`; release is one operation covering worktree, branches,
  processes, ports and background tasks; the contract exposes a narrowed
  projection (`branch`, `base`, diff summary) — process ids and ports do not cross
  to the app; Swift mirror + golden for the projection.
- **Depends on:** `DE2.4`

---

## Epic `DE3` — State, spec & plan


### DE3.1 — `Issue` as a sum
- **Done:** `Issue` per Point B; `Fixed.by` resolves only to a `Merged` pull
  request; `IssueStatus` and `Issue.pr` deleted; Swift mirror + golden.
- **Depends on:** `DE2.4`

### DE3.2 — `Spec` + `SpecRevision`
- **Done:** `SpecId`, `SpecRevisionId` branded; `Spec { id, name, head }` and
  `SpecRevision { id, specId, content, supersedes?, boundTo? }`; revisions are
  append-only; `boundTo` is optional — a spec need not be committed to a repository;
  `WorkstreamPlan.spec: string` replaced by a `SpecRevisionId`; Swift mirror +
  golden.
- **Depends on:** `DE1.2`

### DE3.3 — `Workstream.derivedFrom` + spec drift
- **Done:** `Workstream { id, name, repositoryId, derivedFrom: SpecRevisionId,
  epics }`; `materialize` records the revision it decomposed; drift computed as
  `spec.head ≠ workstream.derivedFrom`, never stored; Swift mirror + golden.
- **Depends on:** `DE3.2`

### DE3.4 — Derived plan status
- **Done:** `Epic` and `Workstream` completion is a computed fold over the issue
  set; `WorkStatus` removed from the persisted schema and from `Snapshot`; board
  projections read the fold; Swift mirror + golden.
- **Depends on:** `DE3.1`

### DE3.5 — Deterministic sequencing
- **Done:** *sequencing* — topological sort over `dependsOn`, dispatch of the next
  `Session` — runs in `packages/daemon` with no `Execution`, no `AgentId` and no
  transcript; decisions appended to `event_log`. *Authoring* a plan from a spec is
  agent work and is out of scope here: it is an `OnSpec` session.
- **Depends on:** `DE3.1`

---

## Epic `DE4` — Projections & client affordances

### DE4.1 — `SessionRecord` projection
- **Done:** a contract procedure returning a `Completed` session joined to its
  execution tree, sealed transcripts and product; composed daemon-side in one
  call; addressed by `SessionId`; not persisted; Swift mirror + golden.
- **Depends on:** `DE2.4`

### DE4.2 — Live vs sealed transcript affordances
- **Done:** `TranscriptProjection` consumes `LiveTranscript` and
  `SealedTranscript` as distinct types; a tail subscription is unrepresentable on
  a sealed transcript; sealed transcripts cached indefinitely; interrupt / answer
  / send exposed only on live.
- **Depends on:** `DE2.2`

### DE4.3 — Inbox by mode
- **Done:** the inbox lists outstanding `UiRequestRaised` from `Autonomous`
  executions only; an `Interactive` execution awaiting its turn produces no inbox
  entry; `SessionIdle` renders as *resting* under `Interactive` and *blocked*
  under `Autonomous`; `waitingSince` ranks blocked autonomous work.
- **Depends on:** `DE2.2`

### DE4.4 — Staleness rendering
- **Done:** referenced entities render `observedAt`; an unknown `Mergeability`
  renders as unknown, never as its last value; divergence alone is not surfaced as
  a warning; spec drift is surfaced on the workstream.
- **Depends on:** `DE2.3`, `DE3.3`

### DE4.5 — Execution tree in the session view
- **Done:** a session renders its root transcript with spawn tool-calls expanded
  into child executions; per-session usage is a fold across the tree.
- **Depends on:** `DE2.2`, `DE4.2`

---

## Must become unrepresentable

The workstream is done when none of these can be constructed:

1. An issue in a fixed state with no pull request that fixed it.
2. A completed unit of agent work that produced no transcript.
3. Two lifecycles for one thing disagreeing (`Job.status` vs `Session.status`).
4. Two records disagreeing about the same issue's pull request.
5. An issue closed as *wontfix* indistinguishable from one closed by a merge.
6. A pull request with no base — i.e. no way to express that it is stale.
7. A referenced value with no `observedAt`.
8. One agent per unit of work (`UNIQUE session_job(jobId)`).
9. A subagent transcript with no parent to hang from.
10. A worktree, port or process outliving the session that opened it.
11. A merge performed outside Sprinter with nowhere to land.
12. A workstream with no spec revision it was derived from.
13. A blocked autonomous escalation indistinguishable, in the inbox, from an
    interactive session merely awaiting its turn.
14. A completed session whose product does not match its target.

## Done (workstream)

- All fourteen states above are unconstructible.
- `Job`, `PullRequestRef`, `IssueStatus`, `JobStatus`, `SessionStatus`, `JobKind`
  appear nowhere in `packages/*` or `apple/*`.
- `Session` names the unit of work; `Execution` names one agent's run. No
  identifier in `packages/domain` or `packages/contract` uses "session" for a
  process.
- `bun run check` and `make check` green; goldens cover every mirrored type.
