# Workstream `TRK-B` — SwiftUI app

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** the native client — Swift RPC client + `Backend`, Mission Control,
> Interactive session (incl. Planner), and Inspector — built against the frozen
> contract.
>
> **Prerequisites (cross-workstream):** the **`FDN` (Foundation) workstream is
> landed** — the frozen RPC contract, the Swift bridge, and the domain schemas.
> `TRK-B` depends on the Foundation **only** — never on `TRK-A`.

## Cross-cutting invariants

Repo-wide set (from [`policy.md`](../policy.md) / [`conventions.md`](../conventions.md) /
[`decisions.md`](../decisions.md)). Guards on this side resolve to **`make check`**
+ CI and review.

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | `make check` green (swift-format + SwiftLint + build + tests) | `make check` + CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's modules | coverage gate in `make check` |
| `INV-NOFORCE` | No force unwrap/cast/try; no implicitly-unwrapped optionals | `SwiftLint --strict` |
| `INV-PIN` | Deps exact-pinned (`.exact`); `Package.resolved` committed | `check` + committed resolve file |
| `INV-NAMING` | Follows `conventions.md` | review |
| `INV-PORT` | The app depends on a `Backend` port — **no feature surface assumes a local daemon** | review |
| `INV-CONTRACT` | Consumes the frozen contract via the mirrored DTOs; decode tests pass | `make check` + review |

`INV-GATE`/`INV-COV`/`INV-NOFORCE`/`INV-PIN`/`INV-NAMING` apply to every task.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `BE1` | Swift RPC client + `Backend` | — (needs `FDN`) |
| `BE2` | Mission Control | `BE1` |
| `BE3` | Interactive session | `BE1` |
| `BE4` | Inspector | `BE1` |

```
BE1 ─┬─► BE2
     ├─► BE3
     └─► BE4
```

> Cutover (app on the real daemon, driving a real Issue → PR) is the
> cross-workstream **convergence** milestone — see the plan
> [index](./README.md#convergence--cutover), not an epic of this workstream.

---

## Epic `BE1` — Swift RPC client + `Backend`  ·  tags: `INV-PORT`, `INV-CONTRACT`

### BE1.1 — RPC client + `Backend`
- **Done:** a Swift RPC client (queries + streaming subscriptions) whose decodes
  pass the `FE2.4` tests, behind a **`Backend`** port with local-daemon and
  remote-daemon adapters; no feature code references localness (`INV-PORT`).
- **Depends on:** —

### BE1.2 — Reconnect/resync + session channel
- **Done:** reconnect with snapshot-then-stream resync; the session channel wired
  (`sessionEvents` in; `sessionSend`/`interrupt`/`answerUiRequest` out) incl. the
  `extension_ui_request` round-trip.
- **Depends on:** `BE1.1`

---

## Epic `BE2` — Mission Control

### BE2.1 — Board + activity
- **Done:** the cross-repo workstream→epic→Issue board rendered from `snapshot` +
  live `events`, statuses updating (ongoing/paused/complete), with per-agent
  activity.
- **Depends on:** `BE1.2`

### BE2.2 — "Agent waiting on you" inbox
- **Done:** pending `extension_ui_request`s surface as an inbox and are answered
  via `answerUiRequest`.
- **Depends on:** `BE1.2`

---

## Epic `BE3` — Interactive session

### BE3.1 — Session view
- **Done:** a live session view rendering `sessionEvents`, with `sessionSend` +
  `interrupt` controls and inline `extension_ui_request` handling; drives any
  session.
- **Depends on:** `BE1.2`

### BE3.2 — Planner
- **Done:** a fresh interactive planning session whose output materializes into the
  work graph (`createWorkstream`).
- **Depends on:** `BE3.1`

---

## Epic `BE4` — Inspector

### BE4.1 — Transcript ↔ PR view
- **Done:** a full transcript rendered from `SessionEvent` (messages, tool calls,
  diffs, thinking) alongside the PR it produced (PR pane + session↔PR link).
- **Depends on:** `BE1.1`
