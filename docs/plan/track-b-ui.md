# Workstream `TRK-B` — SwiftUI app

> Spec doc for the **`workstream`** skill. `/epic` cuts one issue per task, copies
> its **Done**, and threads each cross-cutting invariant into that acceptance
> (naming its guard).
>
> **Goal:** the native client — Swift RPC client + `Backend`, Mission Control,
> Interactive session (incl. Planner), and Inspector — built against the
> Foundation's **stub**, repointing to the real daemon per-handler as `TRK-A`
> lands.
>
> **Prerequisites (cross-workstream):** the **`FDN` (Foundation) workstream is
> landed** — the frozen RPC contract, the Swift bridge/skeleton (`FE5`), the domain
> schemas (`FE2`), and the transcript fixture (`FE3.1`). `TRK-B` depends on the
> Foundation **only** — never on `TRK-A`'s internals (it runs against the stub).

## Cross-cutting invariants

Repo-wide set (from [`policy.md`](../policy.md) / [`conventions.md`](../conventions.md) /
[`decisions.md`](../decisions.md)); `/epic` threads each applicable one into every
task's **Done**. Guards on this side resolve to **`make check`** (Swift) + CI and
the cold **`pr-review`**.

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | `make check` green (swift-format + SwiftLint + build + tests) | `make check` + CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's modules | coverage gate in `make check` |
| `INV-NOFORCE` | No force unwrap/cast/try; no implicitly-unwrapped optionals | `SwiftLint --strict` |
| `INV-PIN` | Deps exact-pinned (`.exact`); `Package.resolved` committed | `check` + committed resolve file |
| `INV-NAMING` | Follows `conventions.md` | cold `pr-review` |
| `INV-PORT` | The app depends on a `Backend` port — **no feature surface assumes a local daemon** | cold `pr-review` |
| `INV-CONTRACT` | Consumes the frozen contract via the generated/mirrored DTOs; passes the golden-fixture decode tests | `make check` + cold `pr-review` |

`INV-GATE`/`INV-COV`/`INV-NOFORCE`/`INV-PIN`/`INV-NAMING` apply to every task;
`INV-PORT`/`INV-CONTRACT` are tagged per epic.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `BE1` | Swift RPC client + `Backend` | — (needs `FDN`: `FE5`) |
| `BE2` | Mission Control | `BE1` |
| `BE3` | Interactive session | `BE1` |
| `BE4` | Inspector | `BE1` |

```
BE1 ─┬─► BE2
     ├─► BE3
     └─► BE4
```

> All surfaces run against the Foundation **stub** first; repoint to the real
> daemon as `TRK-A`'s `AE4` handlers land. Cutover (app on real daemon, driving a
> real Issue → PR) is the cross-workstream **convergence** milestone — see the plan
> [index](./README.md#convergence--cutover), not an epic of this workstream.

---

## Epic `BE1` — Swift RPC client + `Backend`  ·  tags: `INV-PORT`, `INV-CONTRACT`

### BE1.1 — RPC client transport + streaming
- **Done:** the `FE5.3` skeleton hardened into a client that issues queries and
  consumes streaming subscriptions against the stub; decodes pass the `FE5.2`
  golden fixtures.
- **Depends on:** —

### BE1.2 — `Backend` abstraction
- **Done:** a `Backend` port yielding a connected client, with **local-daemon and
  remote-daemon adapters**; no feature code references localness (`INV-PORT`).
- **Depends on:** `BE1.1`

### BE1.3 — Reconnect/resync + session channel
- **Done:** reconnect with snapshot-then-stream resync; the session channel wired
  (`sessionEvents` in; `sessionSend`/`interrupt`/`answerUiRequest` out) incl. the
  `extension_ui_request` round-trip.
- **Depends on:** `BE1.2`

---

## Epic `BE2` — Mission Control

### BE2.1 — Board
- **Done:** the cross-repo workstream→epic→Issue board rendered from `snapshot` +
  live `events`, statuses updating (ongoing/paused/complete).
- **Depends on:** `BE1.3`

### BE2.2 — Activity & budgets
- **Done:** per-agent activity and token/cost budgets shown from the event feed.
- **Depends on:** `BE2.1`

### BE2.3 — "Agent waiting on you" inbox
- **Done:** pending `extension_ui_request`s surface as an inbox and are answered
  via `answerUiRequest`.
- **Depends on:** `BE1.3`

---

## Epic `BE3` — Interactive session

### BE3.1 — Session view
- **Done:** a live session view rendering `sessionEvents`, with `sessionSend` +
  `interrupt` controls; drives any session.
- **Depends on:** `BE1.3`

### BE3.2 — In-session ui-requests
- **Done:** `extension_ui_request`s (select/confirm/input/editor) render inline and
  answer via `answerUiRequest`.
- **Depends on:** `BE3.1`

### BE3.3 — Planner
- **Done:** a fresh interactive planning session whose output materializes into the
  work graph (`createWorkstream` command).
- **Depends on:** `BE3.1`

---

## Epic `BE4` — Inspector

### BE4.1 — Transcript renderer
- **Done:** a full transcript rendered from `SessionEvent` (messages, tool calls,
  diffs, thinking), built against the `FE3.1` fixture until `TRK-A` is live.
- **Depends on:** `BE1.1`

### BE4.2 — Transcript↔PR pairing
- **Done:** the transcript rendered alongside the PR it produced (PR pane +
  session↔PR link).
- **Depends on:** `BE4.1`
