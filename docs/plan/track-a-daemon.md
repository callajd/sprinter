# Workstream `TRK-A` — Daemon / execution

> Spec doc for the **`workstream`** skill. `/epic` cuts one issue per task, copies
> its **Done**, and threads each cross-cutting invariant into that acceptance
> (naming its guard).
>
> **Goal:** the real daemon behind the frozen contract — `LocalPiRunner`,
> `StateStore`, the Job runner + `implement-issue` skill + `Repository`, real
> `RpcServer` handlers, and restart safety.
>
> **Prerequisites (cross-workstream):** the **`FDN` (Foundation) workstream is
> landed** — the frozen RPC contract, the owned Pi protocol schema (`FE3`), the
> domain schemas (`FE2`), the stub, and the scaffold/`check` gate. `TRK-A` depends
> on the Foundation **only** — never on `TRK-B`.

## Cross-cutting invariants

Repo-wide set (from [`policy.md`](../policy.md) / [`conventions.md`](../conventions.md) /
[`decisions.md`](../decisions.md)); `/epic` threads each applicable one into every
task's **Done**.

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | `bun run check` green (format + lint + typecheck + tests) | `bun run check` + CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's modules | coverage threshold in `check` |
| `INV-NOCAST` | No `as`/non-null `!`/`any` | `oxlint` |
| `INV-PIN` | Deps exact-pinned; lockfile committed | `check` + committed lock |
| `INV-NAMING` | Follows `conventions.md` | cold `pr-review` |
| `INV-PORT` | No concrete backing/instance depended on directly; external systems + locations behind ports with adapters | cold `pr-review` |
| `INV-BOUNDARY` | Pi protocol types (`AgentSessionEvent`, `RpcCommand`, …) never cross past `LocalPiRunner`; everything above uses owned `SessionEvent` | cold `pr-review` + no Pi-type import outside the runner package |

`INV-GATE`/`INV-COV`/`INV-NOCAST`/`INV-PIN`/`INV-NAMING` apply to every task;
`INV-PORT`/`INV-BOUNDARY` are tagged per epic.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `AE1` | `LocalPiRunner` | — (needs `FDN`: `FE3`, `FE2.2`) |
| `AE2` | `StateStore` port + adapter | — (needs `FDN`: `FE2`) |
| `AE3` | Job runner · skill · `Repository` | `AE1`, `AE2` |
| `AE4` | Real `RpcServer` handlers | `AE1`, `AE2`, `AE3` |
| `AE5` | Restart safety | `AE2`, `AE4` |

```
AE1 ─┐
     ├─► AE3 ─► AE4 ─► AE5
AE2 ─┘                 ▲
     └─────────────────┘
```

---

## Epic `AE1` — `LocalPiRunner`  ·  tags: `INV-PORT`, `INV-BOUNDARY`

The Pi-specific heart; the `ExecutionRunner` local adapter.

### AE1.1 — Spawn + Scope lifecycle
- **Done:** spawn `pi --mode rpc` via `effect/unstable/process` with cwd/env,
  `Scope`-managed so the child is killed on scope close; startup/exit surfaced as
  typed results.
- **Depends on:** —

### AE1.2 — NDJSON stdio bridge
- **Done:** stdout decoded via `Ndjson` → owned Pi protocol schema; stdin encoded;
  pending-command correlation by `id` (a `Deferred` per in-flight command); mines
  Pi's `rpc-process.ts` as reference, imports nothing from it.
- **Depends on:** `AE1.1`

### AE1.3 — `SessionHandle` + event translation
- **Done:** `SessionHandle` (`events` `Stream` / `send` / `interrupt` / `result`);
  **Pi `AgentSessionEvent` → owned `SessionEvent`** translation so nothing above
  the runner sees Pi's type (`INV-BOUNDARY`).
- **Depends on:** `AE1.2`

### AE1.4 — Per-Job worktree lifecycle
- **Done:** a git worktree created per Job and torn down with the Job's `Scope`
  (create/teardown; no leaked worktrees/branches).
- **Depends on:** `AE1.1`

---

## Epic `AE2` — `StateStore` port + first adapter  ·  tag: `INV-PORT`

### AE2.1 — `StateStore` port
- **Done:** persistence-agnostic `StateStore` Service (`WorkGraphStore` /
  `JobStore` / `EventLogStore` interfaces) — work graph + Job model +
  Issue→Job→session→PR mapping + append-only event feed. No backing referenced.
- **Depends on:** —

### AE2.2 — SQL adapter
- **Done:** one adapter behind the port (SQL via `effect/unstable/sql`, SQLite
  instance): schema/migrations + the three stores implemented and tested; **nothing
  outside this adapter imports SQLite/SQL** (`INV-PORT`).
- **Depends on:** `AE2.1`

---

## Epic `AE3` — Job runner · skill · `Repository`  ·  tag: `INV-PORT`

### AE3.1 — Single-Issue Job runner
- **Done:** dispatch a Job → session (via `AE1`) → capture terminal `JobResult`;
  persists Job/session rows via `StateStore`.
- **Depends on:** `AE1.3`, `AE2.2`

### AE3.2 — `implement-issue` skill + result capture
- **Done:** the agent-side `implement-issue` Pi skill (prompt/tools) plus
  structured result capture (a `report` tool whose payload the runner records).
- **Depends on:** `AE3.1`

### AE3.3 — `Repository` port
- **Done:** repo-scoped `Repository` Service (`CodeOps` / `IssueOps` /
  `PullRequestOps`); no host/vendor terms leak into the interface.
- **Depends on:** —

### AE3.4 — GitHub adapter for `Repository`
- **Done:** GitHub adapter behind `Repository`: Issue read, PR
  detection/reconciliation (which PR closed which Issue); rolls up to Epic/Workstream
  status in `StateStore`.
- **Depends on:** `AE3.3`, `AE2.2`

---

## Epic `AE4` — Real `RpcServer` handlers  ·  tag: `INV-CONTRACT` (from `FDN`)

Swap the Foundation stub → real, per-handler; each must still satisfy the frozen
contract's golden-fixture tests.

### AE4.1 — `snapshot` + `events` handlers
- **Done:** `snapshot` served from `StateStore`; `events` fed by a real `PubSub`;
  contract tests green against the real handlers.
- **Depends on:** `AE2.2`

### AE4.2 — command handlers
- **Done:** `createWorkstream` / `controlWorkstream` (start/pause/resume/cancel/
  retry) drive the runner/scheduler; contract tests green.
- **Depends on:** `AE3.1`

### AE4.3 — session-channel handlers
- **Done:** `sessionEvents` / `sessionSend` / `interrupt` / `answerUiRequest`
  bridge a live `SessionHandle`, including the `extension_ui_request` round-trip.
- **Depends on:** `AE1.3`, `AE4.1`

---

## Epic `AE5` — Restart safety

### AE5.1 — Persist mapping + reconcile on startup
- **Done:** the Job↔session↔PR mapping is persisted; on startup the daemon
  reconciles against `Repository` (which Issues closed / PRs merged) and rolls up
  status before resuming.
- **Depends on:** `AE2.2`, `AE4.1`

### AE5.2 — Re-dispatchable / idempotent Jobs
- **Done:** an in-flight Job that was `running` at restart is either re-dispatched
  from its inputs or resumed onto its persisted session (`switch_session`) without
  loss or double-run.
- **Depends on:** `AE5.1`, `AE3.1`
