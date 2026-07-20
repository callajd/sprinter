# Workstream `DMR` — Domain remodel

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** replace the process-shaped read model with a state / transition / plan
> model, across `packages/*` and the Swift mirror.
>
> **Prerequisites:** none. The store is treated as **greenfield** — no data is
> preserved, no migration is written.
>
> **Exit:** every state in [Must become unrepresentable](#must-become-unrepresentable)
> fails to typecheck or has no schema to express it, and the app renders live and
> sealed transcripts through distinct types.
>
> Optional deeper reference: [`../design/domain-graph.html`](../design/domain-graph.html)
> (open in a browser). This spec is self-contained; the reference adds rationale only.

## Point A — what exists today

`packages/domain/src/read-model.ts` (Effect `Schema.Struct`, not classes):

| Type | Shape |
|------|-------|
| `Workstream` | `{ id, name, repo: string, status: WorkStatus, epics: EpicId[] }` |
| `Epic` | `{ id, workstreamId, name, status: WorkStatus, issues: IssueId[] }` |
| `Issue` | `{ id, epicId, number, title, status: IssueStatus, dependsOn: IssueId[], pr?: PullRequestRef }` |
| `Job` | `{ id, issueId, kind: JobKind, status: JobStatus, sessionId?, transcriptRef?: string, pr?: PullRequestRef }` |
| `Session` | `{ id, jobId, status: SessionStatus }` |
| `PullRequestRef` | `{ number, url, merged: boolean }` — value object, no id |

Enums: `WorkStatus`, `IssueStatus`, `JobKind`, `JobStatus`, `SessionStatus`.
Branded ids in `packages/domain/src/ids.ts`: `WorkstreamId`, `EpicId`, `IssueId`,
`JobId`, `SessionId`.

`packages/state/src/sqlite.ts` tables: `workstream`, `epic`, `issue`,
`issue_dependency`, `job`, `session` (with `UNIQUE session_job(jobId)`),
`event_log`, `session_event_log`. Foreign keys are conventional — indexes only.

`packages/contract/src/rpc.ts`: `Snapshot { workstreams, epics, issues, jobs,
sessions }`; `WorkGraphEvent` tagged union, upsert-only.

`packages/repository/src/repository.ts`: `Repository` is a `Context.Service`
(port) with `code` / `issues` / `pullRequests` capability groups — **not** an
entity. Repo identity in the domain is the `repo: string` field on `Workstream`.

## Point B — target model

```
── STATE ── referenced from the code host; carries observedAt

Repository = { id, host, owner, name, refs: BranchName → CommitSha, observedAt }

Issue =
  | Open      { id, repositoryId, epicId, number, title, dependsOn: IssueId[],
                attempts: AttemptId[], observedAt }
  | Fixed     { …, by: PullRequestId }                    // PR observed Merged
  | Dismissed { …, reason: WontFix | Duplicate | Invalid }

── TRANSITION ── proposal referenced, derivation owned

PullRequest = {                                            // referenced
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
  applies:  Clean | Conflicted { paths },   // git decides
  verified: Passed | Failed | NotRun,       // checks decide
  against:  CommitSha                       // state both were computed on
}

Attempt =                                                  // owned
  | Running   { id, issueId, base: CommitSha, root: ExecutionId,
                workspace: Workspace, pullRequest?: PullRequestId }
  | Abandoned { id, issueId, base, root, reason }
  | Landed    { id, issueId, base, root, pullRequest: PullRequestId }

Execution =                                                // owned
  | Running  { id, attemptId, agentId, parent?: ExecutionId,
               transcript: LiveTranscript }
  | Finished { id, attemptId, agentId, parent?: ExecutionId,
               transcript: SealedTranscript, outcome }

Workspace = { id, attemptId, base: CommitSha,               // owned, ephemeral
              path, branch, diff, processes, ports, tasks }

── PLAN ── owned by Sprinter

Workstream = { id, name, repositoryId, epics: EpicId[] }    // status derived
Epic       = { id, workstreamId, name, issues: IssueId[],   // status derived
               milestone?: Ref<Milestone> }

── REGISTRY ── owned, global, scoped to no repository

Agent = { id, name, model, version, tools,
          supersedes?: AgentId, retiredAt? }                // append-only
```

New branded ids: `RepositoryId`, `PullRequestId`, `AttemptId`, `ExecutionId`,
`WorkspaceId`, `AgentId`, `CommitSha`, `BranchName`.

Cardinality: `Repository 1:∗ Issue`; `Epic 1:∗ Issue`; `Issue ∗:∗ Issue`
(`dependsOn`, a DAG); `Issue 1:∗ Attempt`; `Attempt 1:∗ Execution` (a tree via
`parent`); `Execution 1:1 Transcript`; `Agent 1:∗ Execution` (across all repos);
`PullRequest 1:∗ Issue` (`closes`).

## Repository map

| Path | Role |
|------|------|
| `packages/domain` | Schemas + branded ids. Depends only on `effect`. |
| `packages/contract` | Daemon↔client `RpcGroup` over domain types. |
| `packages/state` | `StateStore` port + SQLite adapter (`sqlite.ts`). |
| `packages/repository` | Code-host port + GitHub adapter + reconciler. |
| `packages/runner` | Pi process/transport adapter → owned `SessionEvent`. |
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

Bind every epic; each task's **Done** must satisfy the applicable ones.

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | Both gates green — `bun run check` and `make check` | CI |
| `INV-COV` | ≥ 75% line **and** function coverage on the task's modules | coverage threshold inside `check` |
| `INV-NOCAST` | No `as` / non-null `!` / `any` (TS); no force unwrap/cast/try (Swift) | `oxlint` / `SwiftLint --strict` |
| `INV-PIN` | Deps exact-pinned; lockfile committed | `check` |
| `INV-NAMING` | Follows [`conventions.md`](../conventions.md) (ports/adapters, owned vs foreign, `*Store`/`*Ops`) | review |
| `INV-PORT` | External systems sit behind ports; no concrete backing depended on directly | review |
| `INV-MIRROR` | Every contract change lands with its Swift mirror **and** golden test in the same PR | `check` + goldens |
| `INV-SUM` | No status enum paired with an optional field. Terminal variants carry their payload as required | review + typecheck |
| `INV-DERIVED` | Values derivable from the graph are not stored | review |
| `INV-OBSERVED` | Every referenced entity carries `observedAt`; owned entities do not | review |
| `INV-FRESH` | No store migration. Bump the schema version; drop and recreate | `sqlite.ts` version constant |

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `DE1` | Identity — registry & state anchors | — |
| `DE2` | Transition layer | `DE1` |
| `DE3` | State & plan reshape | `DE2` |
| `DE4` | Projections & client affordances | `DE3` |

```
DE1 ──► DE2 ──► DE3 ──► DE4
```

---

## Epic `DE1` — Identity: registry & state anchors

### DE1.1 — `Agent` registry
- **Done:** `AgentId` branded; `Agent { id, name, model, version, tools,
  supersedes?, retiredAt? }` in `packages/domain`; store is append-only — editing
  writes a new revision, no delete is exposed; no repository or workstream scope;
  carried in `Snapshot` + `WorkGraphEvent`; Swift mirror + golden.
- **Depends on:** —

### DE1.2 — `Repository` entity
- **Done:** `RepositoryId`, `BranchName`, `CommitSha` branded; `Repository { id,
  host, owner, name, refs: BranchName → CommitSha, observedAt }`;
  `packages/repository` resolves and refreshes it behind the existing port;
  `Workstream.repo: string` replaced by `repositoryId`; Swift mirror + golden.
- **Depends on:** —

### DE1.3 — Retire `SprinterCore/Workstream.swift`
- **Done:** the pre-`SprinterContract` duplicate `Workstream` (unbranded `id:
  String`, three-case `WorkStatus`) is deleted; no references remain.
- **Depends on:** `DE1.2`

---

## Epic `DE2` — Transition layer

### DE2.1 — `PullRequest` as an entity
- **Done:** `PullRequestId` branded; `PullRequest` and `Mergeability` per
  [Point B](#point-b--target-model); `PullRequestRef` and `merged: boolean`
  deleted; staleness computed as `tipOf(target) ≠ base`, never stored; a
  `Mergeability` whose `against` is behind `tipOf(target)` is reported unknown;
  Swift mirror + golden.
- **Depends on:** `DE1.2`

### DE2.2 — `Execution` entity + transcript re-addressing
- **Done:** `ExecutionId` branded; `Execution` per Point B; `LiveTranscript` and
  `SealedTranscript` are distinct types, not one type with a flag;
  `session_event_log` keyed by `ExecutionId`; `SessionId` confined to
  `packages/runner` and absent from `packages/contract`; `UNIQUE
  session_job(jobId)` dropped; Swift mirror + golden.
- **Depends on:** `DE1.1`

### DE2.3 — `Attempt` replaces `Job`
- **Done:** `AttemptId` branded; `Attempt` per Point B; `Landed` reachable only
  when the referenced `PullRequest.state` is observed `Merged`; `Job`, `Session`,
  `JobStatus`, `SessionStatus`, `JobKind`, `transcriptRef` deleted from
  `packages/*` and `apple/*`; Swift mirror + golden.
- **Depends on:** `DE2.1`, `DE2.2`

### DE2.4 — `Workspace` entity
- **Done:** `WorkspaceId` branded; `Workspace` per Point B, held only by
  `Attempt.Running`; release is one operation on the entity and covers worktree,
  branches, processes, ports and background tasks; the contract exposes a narrowed
  projection (`branch`, `base`, diff summary) — process ids and ports do not cross
  to the app; Swift mirror + golden for the projection.
- **Depends on:** `DE2.3`

---

## Epic `DE3` — State & plan reshape

### DE3.1 — `Issue` as a sum
- **Done:** `Issue` per Point B; `Fixed.by` resolves only to a `Merged` pull
  request; `IssueStatus` and `Issue.pr` deleted; Swift mirror + golden.
- **Depends on:** `DE2.3`

### DE3.2 — Derived plan status
- **Done:** `Epic` and `Workstream` completion is a computed fold over the issue
  set; `WorkStatus` removed from the persisted schema and from `Snapshot`; board
  projections read the fold; Swift mirror + golden.
- **Depends on:** `DE3.1`

### DE3.3 — Deterministic dispatch
- **Done:** epic sequencing — topological sort over `dependsOn`, dispatch of the
  next `Attempt` — runs in `packages/daemon` with no `Execution`, no `AgentId` and
  no transcript; decisions appended to `event_log`.
- **Depends on:** `DE3.1`

---

## Epic `DE4` — Projections & client affordances

### DE4.1 — `Landing` projection
- **Done:** a contract procedure returning a `Landed` attempt joined to its
  execution tree, sealed transcripts and merged pull request; composed daemon-side
  in one call; addressed by `AttemptId`; not persisted; Swift mirror + golden.
- **Depends on:** `DE3.1`

### DE4.2 — Live vs sealed transcript affordances
- **Done:** `TranscriptProjection` consumes `LiveTranscript` and
  `SealedTranscript` as distinct types; a tail subscription is unrepresentable on
  a sealed transcript; sealed transcripts cached indefinitely; interrupt / answer
  / send exposed only on live.
- **Depends on:** `DE2.2`

### DE4.3 — Staleness rendering
- **Done:** referenced entities render `observedAt`; an unknown `Mergeability`
  renders as unknown, never as its last value; divergence alone is not surfaced as
  a warning.
- **Depends on:** `DE2.1`

### DE4.4 — Execution tree in the session view
- **Done:** an attempt renders its root transcript with spawn tool-calls expanded
  into child executions; per-attempt usage is a fold across the tree.
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
10. A worktree, port or process outliving the attempt that opened it.
11. A merge performed outside Sprinter with nowhere to land.

## Done (workstream)

- All eleven states above are unconstructible.
- `Job`, `Session`, `PullRequestRef`, `IssueStatus`, `JobStatus`,
  `SessionStatus`, `JobKind` appear nowhere in `packages/*` or `apple/*`.
- `bun run check` and `make check` green; goldens cover every mirrored type.
