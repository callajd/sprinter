# Workstream `TRK-A` — Daemon / execution

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** the real daemon behind the frozen contract — `LocalPiRunner`,
> `StateStore`, the Job runner + `Repository`, the `RpcServer` handlers, and
> restart safety.
>
> **Prerequisites (cross-workstream):** the **`FDN` (Foundation) workstream is
> landed** — the frozen RPC contract, the owned Pi protocol schema, the domain
> schemas, and the scaffold/`check` gate. `TRK-A` depends on the Foundation
> **only** — never on `TRK-B`.

## Cross-cutting invariants

Repo-wide set (from [`policy.md`](../policy.md) / [`conventions.md`](../conventions.md) /
[`decisions.md`](../decisions.md)).

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | `bun run check` green (format + lint + typecheck + tests) | `bun run check` + CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's modules | coverage threshold in `check` |
| `INV-NOCAST` | No `as`/non-null `!`/`any` | `oxlint` |
| `INV-PIN` | Deps exact-pinned; lockfile committed | `check` + committed lock |
| `INV-NAMING` | Follows `conventions.md` | review |
| `INV-PORT` | No concrete backing/instance depended on directly; external systems + locations behind ports | review |
| `INV-BOUNDARY` | Pi protocol types (`AgentSessionEvent`, `RpcCommand`, …) never cross past `LocalPiRunner`; everything above uses owned `SessionEvent` | review + no Pi-type import outside the runner package |

`INV-GATE`/`INV-COV`/`INV-NOCAST`/`INV-PIN`/`INV-NAMING` apply to every task.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `AE1` | `LocalPiRunner` | — (needs `FDN`) |
| `AE2` | `StateStore` | — (needs `FDN`) |
| `AE3` | Job runner · `Repository` | `AE1`, `AE2` |
| `AE4` | `RpcServer` handlers | `AE3` |
| `AE5` | Restart safety | `AE4` |

```
AE1 ─┐
     ├─► AE3 ─► AE4 ─► AE5
AE2 ─┘
```

---

## Epic `AE1` — `LocalPiRunner`  ·  tags: `INV-PORT`, `INV-BOUNDARY`

The `ExecutionRunner` local adapter.

### AE1.1 — Spawn + NDJSON bridge
- **Done:** spawn `pi --mode rpc` via `effect/unstable/process`, `Scope`-managed
  (killed on scope close); stdout decoded via `Ndjson` → owned Pi protocol schema,
  stdin encoded, pending-command correlation by `id`. Mines Pi's `rpc-process.ts`
  as reference; imports nothing from it.
- **Depends on:** —

### AE1.2 — `SessionHandle` + event translation
- **Done:** `SessionHandle` (`events` `Stream` / `send` / `interrupt` / `result`);
  Pi `AgentSessionEvent` → owned `SessionEvent` translation so nothing above the
  runner sees Pi's type (`INV-BOUNDARY`).
- **Depends on:** `AE1.1`

---

## Epic `AE2` — `StateStore` port + adapter  ·  tag: `INV-PORT`

### AE2.1 — `StateStore` port + SQL adapter
- **Done:** persistence-agnostic `StateStore` Service (`WorkGraphStore` /
  `JobStore` / `EventLogStore`) — work graph + Job model + Issue→Job→session→PR
  mapping + append-only event feed — plus one SQL adapter (SQLite instance) behind
  it. Nothing outside the adapter references SQL/SQLite (`INV-PORT`).
- **Depends on:** —

---

## Epic `AE3` — Job runner · `Repository`  ·  tag: `INV-PORT`

### AE3.1 — Single-Issue Job runner
- **Done:** dispatch a Job → session (via `AE1`) → capture the terminal
  `JobResult`; persist Job/session rows via `StateStore`.
- **Depends on:** `AE1.2`, `AE2.1`

### AE3.2 — `Repository` port + GitHub adapter
- **Done:** repo-scoped `Repository` Service (`CodeOps` / `IssueOps` /
  `PullRequestOps`) + a GitHub adapter behind it (Issue read, PR
  detection/reconciliation, roll-up to Epic/Workstream status in `StateStore`).
- **Depends on:** `AE2.1`

---

## Epic `AE4` — `RpcServer` handlers  ·  tag: `INV-CONTRACT` (from `FDN`)

Implement the real handlers behind the frozen contract; each satisfies the
contract's tests.

### AE4.1 — Query, events & command handlers
- **Done:** `snapshot` served from `StateStore`; `events` fed by a real `PubSub`;
  command handlers (`createWorkstream` / `controlWorkstream`: start/pause/resume/
  cancel/retry) drive the runner.
- **Depends on:** `AE2.1`, `AE3.1`

### AE4.2 — Session-channel handlers
- **Done:** `sessionEvents` / `sessionSend` / `interrupt` / `answerUiRequest`
  bridge a live `SessionHandle`, including the `extension_ui_request` round-trip.
- **Depends on:** `AE1.2`, `AE4.1`

---

## Epic `AE5` — Restart safety

### AE5.1 — Persist, reconcile, re-dispatch
- **Done:** the Job↔session↔PR mapping is persisted; on startup the daemon
  reconciles against `Repository` (which Issues closed / PRs merged) and rolls up
  status; a Job that was `running` at restart is re-dispatched (or resumed onto its
  persisted session) without loss or double-run.
- **Depends on:** `AE4.1`
