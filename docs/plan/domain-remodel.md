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
blocked). A session stores no lifecycle of its own — running, abandoned and
completed are read off its root execution. **Spec → Workstream → Epic → Issue** is
the *plan*: what Sprinter intends, as opposed to what the code host currently
reports; the decomposition from spec to plan is itself a session.

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

Session = { id, target, base: CommitSha, root: ExecutionId,   // STORED record
            workspace?: Workspace, product?: Product }
            // no stored lifecycle — see SessionState below

SessionState =                  // PROJECTED into the contract; never persisted
  | Running   { workspace }                    // root execution Running
  | Abandoned { reason }                       // root Finished, no product
  | Completed { product }                      // root Finished, product present

target  = OnIssue { issueId }                              // OPEN union — grows
        | OnSpec  { specId }                               // author the document
        | OnPlan  { specRevisionId }                       // decompose into a plan

product = MergedPullRequest { pullRequestId }              // OnIssue
        | SpecRevised       { specRevisionId }             // OnSpec
        | PlanDerived       { workstreamId }               // OnPlan

// Totality is a compile error, not a convention: adding a `target` variant
// fails to typecheck until its `product` exists.
PRODUCT_FOR = { OnIssue: "MergedPullRequest",
                OnSpec:  "SpecRevised",
                OnPlan:  "PlanDerived" }
              satisfies Record<Target["_tag"], Product["_tag"]>

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

|  | `OnSpec` | `OnPlan` | `OnIssue` |
|---|---|---|---|
| **Interactive** | collaborative planning | walking the decomposition together | pairing on an issue |
| **Autonomous** | drafting a spec unattended | decomposing a revision unattended | implementation |

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
| **session lifecycle** | root execution state + presence of `product` — never stored |
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

## Landed foundations (DE1.1) — inherit these, do not rebuild them

DE1.1 (issue #85, PR #88) established machinery the rest of the workstream builds on.
Later tasks must reuse it rather than reinvent or rediscover it.

| Foundation | Where | What later tasks owe it |
|---|---|---|
| `SCHEMA_VERSION` + `SCHEMA_LEDGER` | `packages/state/src/sqlite.ts`, digest ledger in `schema-version.test.ts` | Any schema change **bumps the constant and adds a ledger row**. There is no migration ladder; reusing a version with different DDL fails the gate. This is `INV-FRESH`'s guard. |
| `PRAGMA foreign_keys = ON`, read back at connection setup | `packages/state/src/sqlite.ts` | Foreign keys are a **real modelling tool** — `INV-ENFORCE` expects them, not read-then-check. |
| Store **generation** id, minted per drop-and-recreate | `store_meta`, on `Snapshot` and both stream cursors | Any new cursor-bearing stream carries the generation in the **same value** as its offset, and refuses on mismatch. |
| `ResumeContext { sinceOffset, generation }` | `packages/contract/src/rpc.ts` + Swift mirror | The pattern for "two fields that must agree" — one optional value, never two optionals. |
| `check:goldens` stage | `bun run check` | `INV-MIRROR` is now gate-enforced; golden drift fails CI rather than relying on manual regeneration. |
| `Timestamp` (canonical `…THH:MM:SS.sssZ`) | `packages/domain/src/time.ts` | The single owned instant type. Reuse it for every `observedAt`; **a leap second is a hard decode failure**, so an adapter reading externally-sourced instants translates at its boundary (`INV-PORT`). |

### Landed by DE1.2 (issue #86) — also inherit these

| Foundation | Where | What later tasks owe it |
|---|---|---|
| `CodeHost` port (was `Repository`) | `packages/repository/src/code-host.ts` | The role-noun names the external SYSTEM; the plain name `Repository` belongs to the owned entity. Tag `sprinter/repository/CodeHost`; its error is `CodeHostError`. |
| `Repository` entity + `RepositoryKey` | `packages/domain/src/repository.ts` | The `── STATE ──` anchor. `(host, owner, name)` is the natural key and is `UNIQUE`, but it is **mutable** (rename/transfer). The `RepositoryId` an adapter mints is therefore a function of the **host's own stable identifier** (GitHub's numeric repo id), never of the key: a refresh lands on the same row, and a **rename updates that row's key in place** instead of forking identity. A second host adapter owes the same property. |
| `RepositorySegment` (branded, allow-list) | `packages/domain/src/repository.ts` | Deliberately a **superset** of any one host's grammar, so it survives a second host adapter. It owes only: no transport syntax, no ambiguity in the natural key, no relative path segment. Whether a spelling is a legal owner is the **host's** verdict (a 404), not the domain's. Do not tighten per-host. |
| `CommitSha` / `BranchName` | `packages/domain/src/ids.ts` | Real checks IN the schema, not branded `NonEmptyString`s. Reuse them wherever a sha or a ref name enters the domain. |
| `hostInstant` (leap-second translation) | `packages/repository/src/github.ts` | The one place a host's instant spelling becomes a `Timestamp`. Any new host-sourced instant goes through it — never a raw host string into `Timestamp`. |

### A known tension — `INV-FRESH` vs. the registry's durability

`INV-FRESH` drops and recreates the store on every schema bump. The `Agent` registry is
**owned**, append-only, with no re-derivation source, and its stated value is that history
survives so a past execution resolves to the exact revision that ran it. Both cannot hold
unconditionally: a bump destroys that history, and DE2.2's `Execution.agentId` can then
dangle across one.

**RESOLVED at DE2.2 (issue #104) for referential integrity.** `Execution.agentId` is a
real `FOREIGN KEY` onto `agent`, and a version bump drops *every* table in one sweep — so
`agent` and `execution` go together and no execution can survive naming a discarded
revision. Within a generation the key refuses the write; across one there is nothing left
on either side. A dangling `agentId` is **unconstructible**, so no downstream task has to
model a possibly-absent agent.

What remains is not an integrity question but a product one — *should* agent history
survive the remodel? While the store is pre-release greenfield, no: the history is worth
what the executions referencing it are worth, and those go with it. No re-derivation
source was invented; `putAgent` stays the sole source of truth for registry content. The
reasoning lives where the durability claim does (`packages/domain/src/registry.ts`,
`packages/state/src/sqlite.ts`). Revisions remain immutable **within a store generation**,
and no task may assert durability wider than that.

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
| `INV-TARGET` | Session machinery is target-agnostic. A new `target` variant adds a reference and a `product` — nothing else | `PRODUCT_FOR` totality + review |
| `INV-LIFECYCLE` | No entity stores a lifecycle its children already determine | review + typecheck |
| `INV-OPAQUE` | No code in `packages/*` parses spec content. Structure exists only in the derived work graph | review |
| `INV-MODE` | `mode` is per-execution. Nothing stores a session-level mode | review |
| `INV-ENFORCE` | Every invariant above is enforced by a mechanism that makes violation **unconstructible** — a schema constraint (`UNIQUE`/`FOREIGN KEY`/`CHECK`) or a type — never by convention, and never by a runtime check a sibling code path can bypass. Two fields that must agree are **one value**. A surviving runtime guard states in its docstring why the state cannot instead be made unconstructible | review + the constraint itself |

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
  refreshes it behind the port; `Workstream.repo: string` replaced by
  `repositoryId`; Swift mirror + golden. The port is renamed to the role-noun
  **`CodeHost`** so the plain name `Repository` is free for the owned entity
  (`conventions.md` updated with it). `host` is a closed literal set; `(host, owner,
  name)` is `UNIQUE`; `refs` is a child table keyed `(repositoryId, name)`; and
  `workstream.repositoryId` is a `FOREIGN KEY` (`INV-ENFORCE`).
- **Out of scope, stated:** the refresh MECHANISM lands here; the refresh TRIGGER
  does not. One consequence is documented and pinned by a test rather than fixed: a
  stale row's natural key blocks a different repository renamed into it (it is
  REJECTED at the RPC, not died on). Branch listing is unpaginated. And the plan-NAME
  half of a workstream id is still a lossy slug — pre-existing, tracked as issue #95.
  See the constraints recorded against `DE4.4` and `DE2.3`.
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
  vocabulary; the name `Session` is left free for **DE2.4** (which re-points it at the
  unit of work); Swift mirror + goldens.
- **Depends on:** `DE1.1`

### DE2.2 — `Execution` entity + `mode`
- **Done:** `ExecutionId` branded; `Execution { id, sessionId, agentId, parent?,
  mode, transcript }`; `mode = Interactive | Autonomous`; `LiveTranscript` and
  `SealedTranscript` are distinct types, not one type with a flag;
  `execution_event_log` keyed by `ExecutionId`; `SessionId` (Pi's) confined to
  `packages/runner` and absent from `packages/contract`; `UNIQUE
  session_job(jobId)` dropped; Swift mirror + golden. `agentId` is a `FOREIGN KEY` into
  the registry (`INV-ENFORCE`) — and this task lands the registry's **first production
  writer**, since DE1.1 shipped `Agent` reachable only from tests, so `Snapshot.agents` is
  empty in a real daemon until here. The durability tension recorded under
  [Landed foundations](#a-known-tension--inv-fresh-vs-the-registrys-durability) is
  **resolved** by that foreign key: a reset drops the registry and the executions together,
  so a dangling `agentId` is unconstructible rather than merely unlikely.
- **Landed as:** `Execution { id, jobId, agentId, parent?, mode, transcript }` — the link
  is `jobId` **only until DE2.4** re-points it at `sessionId` (`Session` does not exist
  yet, and a real key now beats a nullable one later). `ExecutionStatus` is **gone**:
  liveness IS the transcript variant, because a status enum beside it would be a second
  field that must agree with the first (`INV-SUM` / `INV-ENFORCE`), and a settled run's
  OUTCOME belongs to the work it advanced. `execution_event_log."executionId"` became a
  foreign key too (1 Execution = 1 Transcript), and the registry's production writer
  derives an agent revision's id from its CONTENT, so re-running the same agent is an
  idempotent no-op and editing it is a new revision by construction.
- **Decided at cold review (issue #109):**
  - **The tree is acyclic only with `parent` FROZEN.** The `parent` foreign key
    constrains the row a write *references*, never the re-pointing of an existing one, so
    while `parent` sat in `putExecution`'s `ON CONFLICT … DO UPDATE SET` list a 2-cycle
    was constructible in three ordinary writes — after which the job had no rootless row,
    `getExecutionForJob` answered `None`, and the job's execution vanished from the
    snapshot *and* from `startup-reconcile`'s seal, re-opening the CE4.1-R4 live-orphan
    stall. `parent` is now INSERT-ONLY: **re-parenting is not an operation an execution
    has**, and a write attempting one is refused by the engine inside the same statement
    (`INV-ENFORCE` — unconstructible, not walked for).
  - **A job has exactly ONE root**, enforced by a partial `UNIQUE (jobId) WHERE parent IS
    NULL` index. `getExecutionForJob`'s "the root" was previously undefined for two
    parentless rows and picked between them by id collation.
  - **ONE JOB OWNS ONE TREE**, enforced by a COMPOSITE `parent` edge — `UNIQUE (id,
    "jobId")` plus `FOREIGN KEY (parent, "jobId") REFERENCES execution (id, "jobId")`
    (`SCHEMA_VERSION` 7). The single-column key constrained only that the parent ROW
    exists; nothing required it to belong to the same job, so a CROSS-JOB edge was an
    ordinary write and it recreated the very state the single-root index closed — a job
    owning an execution while holding no rootless row, hence `getExecutionForJob` → `None`
    and a `startup-reconcile.settle` that silently skipped sealing. With the edge scoped to
    the job a job's executions are closed under `parent`, so `None` now genuinely means
    "this job owns no executions" (`INV-ENFORCE`).
  - **`putExecution`'s update set is a RULE, not a list.** Four review rounds each found a
    freeze that covered the column someone had named while the invariant needed a
    different one (`supersedes` write order → `parent` re-point → `parent` cross-job →
    `jobId` migration: an ordinary re-put moved a leaf to another job, leaving the
    original owning nothing and `settle` sealing nothing). The rule that replaces the
    patching: **an `Execution` is a historical record of a run, so every column is FROZEN
    unless there is a named reason it changes.** That yields `id` (key), `jobId`/`parent`
    (STRUCTURAL — frozen, refused inside the upsert statement), `agentId`/`mode` (PER-RUN
    FACTS — rewritten by a re-dispatch), `transcript` (the one field that legitimately
    changes, live → sealed). **A new column's default is FROZEN.** Stated on
    `putExecution` in `store.ts` and beside the DDL.
  - **`agentId` is the MOST RECENT run's revision, not "the agent that ran".** #77's
    merged-transcript model reuses ONE execution id across re-dispatches, so a retry
    after an agent upgrade must be able to re-attribute the row — freezing it would break
    the retry. The docstrings claiming an unconditional "always resolves to the agent that
    actually ran it" were narrowed on both sides of the wire instead: a merged transcript
    is attributed to the latest revision, and *"a retry is a new execution"* is DE2.4 work.
  - **The dispatch seal is EXACT; only the reconcile seal is a bound.** `job-runner`'s
    fold is the sole writer of the transcript and now KEEPS the offsets `append` returns,
    sealing at `max(localHighWater, maxOffset ?? 0)`. The "no local high-water mark to
    prefer" argument is true only of `startup-reconcile`'s settle, whose run's process is
    already gone — which is the path the lower-bound contract is written for.
  - **`settle` seals EVERY execution a job owns**, iterating `listExecutionsForJob` rather
    than sealing `getExecutionForJob`'s root. A job owns a tree; a sibling left with a
    `LiveTranscript` kept `isExecutionLive` true and made `resolveLive` bounded-WAIT on it
    while the job was still `queued`/`running` — the CE4.1-R4 stall moved off the root
    rather than closed. Each is sealed at ITS OWN `executionLog.maxOffset`.
  - **The snapshot carries the whole TREE**, via a new `listExecutionsForJob` read.
    `putExecution` journals `ExecutionChanged` for *every* execution, so a snapshot built
    from the root alone shipped strictly less than the deltas that follow it and a
    reconnecting client converged differently from one that never dropped.
  - **`SealedTranscript.lastOffset` is a LOWER BOUND**, stated on both sides of the wire.
    `[0, lastOffset]` is complete and immutable — the entire cacheability claim, and
    unconditionally true — but it is not a claim that nothing exists beyond it: the seal
    falls back to `0` on a transient extent read (better than an execution left live
    forever), the durable-append fold is bounded by the execution's terminal so an
    in-flight append can land after the extent is read, and a per-append failure is
    absorbed. Nothing consumes `lastOffset` yet — `resyncExecutionEvents` reads
    `executionLog.maxOffset` directly — so the bound is documented before it has a reader.
  - **`BoardProjection.liveActivity`'s widening is INTENDED.** The old
    `execution?.status == .active` clause was dead; `isLive` makes the execution clause
    decide for the first time, and a non-terminal non-`running` job with an open
    transcript now reads live. Narrowing back would put liveness on the job's status enum
    and make the transcript decorative — the second source of truth `INV-SUM` removed —
    and the state it newly surfaces is exactly the live orphan the startup seal clears.
  - **`LOCAL_PI_AGENT.version` is a placeholder that does NOT track the `pi` binary**, so
    an upgrade re-derives the same registry id and the registry records that "the same
    agent" ran across it. Documented alongside the `model` caveat, with what would have to
    change (the resolved version reaching the adapter as data at spawn time).
  - **`registerAgent` appends ORIGINAL revisions only**, so editing an agent definition
    starts a new unlinked lineage rather than extending one; `Snapshot.agents` accumulates
    single-revision lineages the helpers correctly treat as unrelated agents.
  - **A RETIREMENT is a no-op on the dispatch path**, a consequence of content-addressed
    ids. Retiring a lineage appends a new revision with `supersedes` + `retiredAt`, but the
    local `pi` content is unchanged, so the next dispatch re-derives the ORIGINAL id,
    `putAgent` answers "unchanged", and the execution is attributed to a retired lineage's
    head. Dispatch never consults `isLineageRetired`. Recorded, not fixed: honouring a
    retirement is a decision about what one MEANS operationally (refuse? run the
    successor's content? warn?) and no task on this workstream takes it.
  - **`Job.executionId` is deliberately NOT a foreign key** while `execution."jobId"` is.
    The tables reference each other, so a plain key is unwritable in either order without
    deferred constraints; the surviving consequence — a job may name an execution never
    stored — is stated in the DDL rather than hidden. Every read that matters resolves the
    enforced direction.
  - **`indexedPreferringLive` is a PROJECTION, not an index.** Now that the snapshot ships
    a job's whole tree, it reduces N executions per job to one. Correct for the board's
    boolean question; anything that needs to say something *about* the tree must read
    `snapshot.executions` directly.
- **Depends on:** `DE2.1`

### DE2.3 — `PullRequest` as an entity
- **Done:** `PullRequestId` branded; `PullRequest` and `Mergeability` per Point B;
  `PullRequestRef` and `merged: boolean` deleted; staleness computed as
  `tipOf(target) ≠ base`, never stored; a `Mergeability` whose `against` is behind
  `tipOf(target)` is reported unknown; Swift mirror + golden.
- **Constraint (recorded by DE1.2, issue #86):** the observed `refs` a `tipOf` reads
  are **UNPAGINATED**. The GitHub adapter lists branches at `per_page=100` — GitHub's
  maximum page, not its maximum repository — and follows no `Link: rel="next"`, so on a
  repository with more than 100 branches the observation is PARTIAL, and which 100 is
  GitHub's alphabetical ordering rather than a choice the adapter makes. `Repository`
  already admits partial observation (`refs` is what WAS observed, and an absent branch
  means "not observed", never "does not exist"), so nothing is corrupt — but
  `tipOf(pr.target)` then answers `undefined` for a REAL branch that simply fell off the
  page. DE2.3 must therefore treat "not observed" as **unknown** staleness rather than
  as stale (the same treatment it already gives an out-of-date `Mergeability`), **or**
  land pagination in the adapter first. Paginating was out of scope for DE1.2, whose
  own readers — the natural key, the id, `observedAt` — do not depend on `refs`.
- **Depends on:** `DE1.2`

### DE2.4 — `Session` replaces `Job`
- **Done:** `SessionId` re-pointed at the work unit; `Session` stored as the flat
  record in Point B — **no persisted lifecycle field**; `SessionState` derived from
  the root execution plus presence of `product` and projected into the contract, so
  no consumer sees `workspace?`/`product?`; `target` and `product` unions with the
  `PRODUCT_FOR` map declared `satisfies Record<Target["_tag"], Product["_tag"]>`,
  plus a schema refinement rejecting a `product` whose tag is not
  `PRODUCT_FOR[target._tag]`; `Job`, the old `Session`, `JobStatus`, `JobKind`,
  `transcriptRef` deleted from `packages/*` and `apple/*` (`SessionStatus`, i.e.
  DE2.1's `ExecutionStatus`, is already gone — DE2.2 replaced it with the transcript
  union); Swift mirror + golden.
- **Depends on:** `DE2.2`, `DE2.3`

### DE2.5 — `Workspace` entity
- **Done:** `WorkspaceId` branded; `Workspace` per Point B, present exactly while the
  root execution is `Running`; release is one operation covering worktree, branches,
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
  `content` is opaque to the domain — nothing in `packages/*` parses it
  (`INV-OPAQUE`); `WorkstreamPlan.spec: string` replaced by a `SpecRevisionId`;
  Swift mirror + golden.
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
  transcript; decisions appended to `event_log`. *Authoring* a spec and *decomposing*
  one into a plan are agent work and are out of scope here — see `DE3.6`.
- **Depends on:** `DE3.1`

### DE3.6 — `OnPlan` sessions write the work graph
- **Done:** an `OnPlan` session decomposes a `SpecRevision` into a `Workstream`,
  its `Epic`s and `Issue`s; its product is `PlanDerived { workstreamId }`;
  `Workstream.derivedFrom` is written by that session and by nothing else;
  `createWorkstreamFromPlan` is replaced by starting an `OnPlan` session; Swift
  mirror + golden.
- **Depends on:** `DE2.4`, `DE3.3`

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
- **Constraint (recorded by DE1.2, issue #86):** DE1.2 landed the refresh
  **mechanism** — `CodeHost.resolve` re-observes and `putRepository` replaces the
  record wholesale under a new `observedAt` (D7) — but **no refresh TRIGGER**. The
  only production caller of `putRepository` is new-plan materialisation, so every
  `Repository` freezes at first sighting and would render as monotonically ageing.
  DE4.4 therefore **must land a trigger** (a poll, an invalidation, or a refresh on
  read) before it can render staleness honestly; rendering `observedAt` against a
  record nothing refreshes measures when Sprinter first saw the repository, not how
  stale the data is. A trigger also buys a **write** fix, not just a rendering one: a
  never-refreshed record keeps holding a `(host, owner, name)` the host has already
  freed, so a *different* repository renamed into that key collides with the stale row
  on the `UNIQUE` natural key and fails with `StateStoreError` — permanently, though it
  is entirely valid on the host. DE1.2 pins that behaviour with a test rather than
  guessing which observation is current; the trigger is what makes the guess
  unnecessary. DE1.2 does, however, keep that collision off the DEFECT channel: at the
  RPC it is delivered as a `PlanRejected` naming the conflicting key, because it is
  host-caused and permanent, not a broken store.
- **Constraint (recorded by DE1.2, issue #86):** a repository re-observation that
  changed nothing is **not journaled**. The daemon's journaling decorator compares the
  new observation against the stored row and emits `RepositoryChanged` only when they
  differ in something other than `observedAt` — without which a client retry-looping on
  a rejected plan would grow the untrimmed durable event log without bound and
  re-broadcast an identical delta on every attempt. The cost lands on DE4.4: a client's
  mirror keeps the `observedAt` it was last told about while the durable row moves
  ahead, so a suppressed refresh renders as *staler than it is* until the next genuine
  change. A trigger that wants the stamp fanned out regardless needs a delta carrying
  the observation TIME as its own event, not a whole-record change.
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
14. A completed session whose product does not match its target — rejected by the
    `PRODUCT_FOR` refinement, and unaddable by the map's totality.
15. A session whose stored lifecycle disagrees with its root execution — there is
    no stored lifecycle to disagree.
16. A `Workstream` produced by anything other than an `OnPlan` session.
17. A stored reference to an entity that was never stored — every id-valued column
    naming another table is a `FOREIGN KEY`, so a dangling reference cannot be written
    and no rule may depend on write ordering to hold.
18. A resume cursor separated from the store generation it belongs to — the pair is one
    value, so "a cursor without its generation" has no representation.
19. Two records of the same referenced entity — an entity with a natural key
    (`Repository` is `host`/`owner`/`name`) carries a `UNIQUE` constraint over it.
20. A malformed value in a validated position — a `CommitSha` that is not 40 hex
    characters, a `BranchName` that is not a legal git ref, an instant that is not
    canonical. Branding a `NonEmptyString` does not satisfy this.

> **17-20 were learned, not designed.** They come from DE1.1's implementation
> (issue #85 / PR #88), where each was initially a runtime check that some other path
> bypassed. They are stated here so the remaining epics inherit them as design
> constraints rather than rediscovering them in review. See `INV-ENFORCE`.

## Done (workstream)

- All sixteen states above are unconstructible.
- `Job`, `PullRequestRef`, `IssueStatus`, `JobStatus`, `SessionStatus`, `JobKind`
  appear nowhere in `packages/*` or `apple/*`. (`SessionStatus` — the process-level
  status enum, renamed `ExecutionStatus` by DE2.1 — was deleted early, at **DE2.2**:
  giving `Execution` its real shape replaced it with the `LiveTranscript`/
  `SealedTranscript` distinction, and keeping both would have been two fields that must
  agree.)
- `Session` names the unit of work; `Execution` names one agent's run. No
  identifier in `packages/domain` or `packages/contract` uses "session" for a
  process.
- `bun run check` and `make check` green; goldens cover every mirrored type.
