# Implementation Plan — Part 2: Track A (Daemon / execution)

Builds the real daemon *behind* the frozen contract, replacing the Foundation's
stub handlers incrementally.

> **Depends only on:** the [Foundation](./foundation.md) — the frozen RPC
> contract, the stub server, the captured Pi transcript fixture, and the repo
> scaffold. **Never on Track B.**

## A1 — `LocalPiRunner` *(architecture slice 1)*
- Owned Pi protocol schema (from F0.3) → spawn `pi --mode rpc` via
  `effect/unstable/process`, `Scope`-managed.
- NDJSON bridge; `SessionHandle` (`events` / `send` / `interrupt` / `result`).
- **Translate Pi `AgentSessionEvent` → the owned `SessionEvent`** so nothing
  above the runner sees Pi's protocol type.
- Worktree lifecycle (create/teardown per Job).

## A2 — `StateStore` port + first adapter
- Define the **persistence-agnostic** `StateStore` Service (`WorkGraphStore` /
  `JobStore` / `EventLogStore`) — work graph + Job model + Issue→Job→session→PR
  mapping + the append-only feed backing the `events` subscription.
- Implement **one adapter** behind it (a SQL-backed adapter, SQLite as the initial
  instance). The daemon depends only on the Service; the backing is swappable with
  no core changes. **Nothing builds to SQLite directly.**

## A3 — Job runner + skill + `Repository`
- Single-Issue Job runner.
- The `implement-issue` Pi skill (agent-side prompt/tools) + result capture
  (report tool or PR-side-effect reconciliation).
- The **`Repository` port + GitHub adapter** (repo-scoped Code / Issues / PRs) for
  Issue read + PR detection/reconciliation.

## A4 — Real `RpcServer` handlers
- Swap stub → real, wired to `StateStore` + runner + a `PubSub`: `snapshot` from
  `StateStore`; `events` / `sessionEvents` from real streams; commands drive the
  runner; the session channel bridges `SessionHandle`.
- **Incremental, per-handler** — Track B repoints from stub to real as each lands.

## A5 — Restart safety
- Persist the mapping; make Jobs re-dispatchable / idempotent so a daemon restart
  neither loses nor double-runs work.
- Full `WorkflowEngine`-grade resume is a fast-follow, not on the cutover path.

---

Cutover is reached jointly with Track B — see the plan
[index](./README.md#convergence--cutover).
