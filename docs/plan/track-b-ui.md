# Implementation Plan — Part 3: Track B (SwiftUI app)

Builds the native client entirely against the Foundation's **stub server**,
repointing to the real daemon per-handler as Track A's A4 lands.

> **Depends only on:** the [Foundation](./foundation.md) — the frozen RPC
> contract, the stub server, the Swift bridge/codegen, and the captured Pi
> transcript fixture. **Never on Track A's internals.**

## B1 — Swift RPC client + `Backend`
- Harden the F0.6 skeleton — streaming subscriptions, reconnect + resync
  (snapshot-then-stream), and the session channel including the
  `extension_ui_request` round-trip.
- All behind a **`Backend`** abstraction (local-daemon vs. remote-daemon
  adapters). **No feature surface assumes a local daemon.**

## B2 — Mission Control
- The cross-repo board from `snapshot` + live `events`: workstream → epic → Issue
  status, agent activity, cost/token budgets, and the "agent is waiting on you"
  inbox.

## B3 — Interactive session
- Drive/interrupt any session (`sessionSend` / `interrupt` / `answerUiRequest`).
- Doubles as the in-app **Planner** (a fresh interactive session whose output
  materializes into the work graph).

## B4 — Inspector
- The transcript↔PR paired view: an agent's complete session (from `sessionEvents`)
  rendered alongside the PR it produced.
- Built against the F0.3 transcript fixture until Track A is live.

---

All four surfaces run against the stub first, then the real daemon. Cutover is
reached jointly with Track A — see the plan
[index](./README.md#convergence--cutover).
