# Sprinter — Decision Log

Locked architectural decisions and their rationale. Ordered roughly as decided.
Reversals are kept visible on purpose, so they are not re-litigated.

### D1 — Runtime is Pi, exclusively
Agents are Pi. Claude Code is not involved in this project. Cognitive work
(implementation, review, conflict resolution, planning) runs on Pi.

### D2 — Deterministic daemon owns control flow; agents only do cognition
All orchestration — DAG, topo-order, ready-set, parallelism, fail-forward,
epic/close-out sweep, GitHub, durability — is deterministic daemon code. Agents
are stateless cognitive workers invoked as typed functions; only the cognitive
work (`implement`, `review`, `resolve-conflict`, …) is done by agents.

### D3 — Work graph owned by the app, not the agents
Workstream/Epic/Issue + dependencies live in the app (`StateStore`), never inside
an agent's context. GitHub is durable truth for the leaf Issues/PRs only (see D13).

### D4 — Work survives the UI; the UI is a detachable client
A long-lived headless daemon owns the work. Close the client → work continues.
Reopen → the client re-derives all state (snapshot) and re-subscribes (live
events). The UI is never the system.

### D5 — Ports-and-adapters, all three axes pluggable local + remote
`ExecutionRunner`, `StateStore`, and `ControlPlaneTransport` are Effect
Services; local vs. remote is Layer composition; adapters mix freely.
"Where it runs" is adapter selection, not a deployment fork.

**No concrete backing is ever depended on directly.** SQLite, Postgres, a hosted
store, or in-memory are all *adapters behind* the `StateStore` Service; the core
builds to the port, never to the instance. Same discipline for execution
(`pi --mode rpc` local vs. remote runners) and transport.

### D6 — Minimal, open Job-result contract
The daemon core treats a Job result as an opaque envelope (`status` + optional
open `payload` + optional `error`); per-kind handlers interpret it. New Job kinds
never touch the port. Impose minimal constraints.

### D7 — Everything on Effect `v4.0.0-beta.97` primitives
Services/Layers, `Schema`, `Stream`, `Scope`, `Fiber`, `PubSub`, and the
`effect/unstable/*` namespace (`rpc`, `workflow`, `process`, `persistence`,
`eventlog`, `cluster`, `sql`). APIs are confirmed against
`~/.local/share/effect-beta`, never from memory — v4 moved and renamed things
(e.g. RPC is `effect/unstable/rpc`, **not** `@effect/rpc`).

### D8 — Daemon↔client contract on `effect/unstable/rpc`
Transport-agnostic RPC (`RpcGroup`) carries four models: query/snapshot,
streaming subscription, commands, and the interactive-session channel. Transport
is a swappable detail we do not care about.

### D9 — Every session is interactive/interruptible; planning is in the app
No hard split between headless worker and interactive session. Every session runs
headless by default, is promotable to interactive (`prompt`/`steer`/`follow_up`)
and always interruptible (`abort`). Planning is just an interactive session whose
product materializes into the work graph. → the UI needs **one** reusable
interactive-session surface, not a bespoke planner.

### D10 — Native macOS client in SwiftUI
Truly native, and no Rust backend (ruled out Tauri). Consequence: the Mac client
is a **foreign consumer** of the RPC contract — it mirrors the message schemas in
Swift and cannot share Effect types. This puts a premium on a small, stable,
explicitly-versioned RPC surface. An Effect/TS **web** client comes later
(inexpensive, shares types); both eventually.

### D11 — REVERSED: do **not** build on `pi-orchestrator`
Earlier direction was to build on Pi's orchestrator layer (taking surgical,
Pi-style fixes for blocking bugs, working around the rest). **Reversed** once the
daemon became definitively ours + Effect-native with a durable workflow spine:
the orchestrator is a *peer* to our daemon, not a foundation. Building on it adds
a redundant transport hop and inherits its gaps. We instead talk to
`pi --mode rpc` directly and use `rpc-process.ts` as a **reference
implementation**.

*Consequence:* the entire "surgical-fix vs. fork" policy is moot, and the
orchestrator gap list (multi-client UI-handler overwrite; buffered-command;
spawn model-threading; socket auth; no resume) evaporates — that was all *its*
code, which we no longer run.

### D12 — Pi is an external binary, not a dependency
We invoke the `pi` binary as a subprocess. Pi's internals (`pi-ai`,
`pi-agent-core`, `pi-tui`) are sealed behind the process boundary and are **not**
in our dependency graph. The only coupling is the NDJSON wire protocol, which we
**own** as an Effect `Schema` (authored against `rpc-types.ts`, not imported).
→ zero npm dependency on any `@earendil-works/pi-*` package. The real coupling
risk is protocol drift across `pi` versions, managed by pinning + schema
versioning.

### D13 — GitHub is the code layer; Workstreams and Epics are Sprinter-only
GitHub holds **only Issues and the PRs that close them** — nothing higher.
**Workstreams and Epics live entirely in Sprinter** (`StateStore`), along with the
dependency DAG and the workstream→epic→Issue→PR mapping. GitHub **Projects v2 is
rejected**: we keep GitHub code-oriented rather than modeling planning hierarchy
there. Every GitHub Issue stays a real, PR-closable code unit.

*Consequence:* reconciliation is one-directional and Issue/PR-level — Sprinter
reads which Issues closed / which PRs merged and rolls that up into Epic/Workstream
status internally; it never reads planning state back from GitHub. (Milestone-as-
Epic was considered as a fallback for native progress rollup and set aside.)

### D14 — Provider abstraction is universal; model a repo-scoped `Repository`
Extends D5 into a standing invariant: Sprinter depends only on **ports**, and
*every* external system and location-variant is a provider with adapters —
"local vs. remote" is merely one adapter axis. Two ports added:

- **`Repository`** (daemon-side) — everything Sprinter needs from a code host is
  **repo-scoped**: Code (branches/merging), Issues, and PRs. So the port is a
  single **repository**, not a host/org "forge" (name rejected). GitHub.com is
  the first adapter; GitHub Enterprise / other hosts are future adapters. One
  (repo-scoped) workstream binds one `Repository`; cross-repo = many instances.
- **`Backend`** (client-side) — the SwiftUI app depends on a connected
  Sprinter client, with **local-daemon and remote-daemon as adapters**. No UI
  feature surface assumes a local process; mirrors `ControlPlaneTransport`.

The full naming & API conventions derived from this invariant live in
[`conventions.md`](./conventions.md).

### D15 — Engineering policy: coverage, dual `check` gates, strict tooling, pinning, CI
Binding repo standards (full text: [`policy.md`](./policy.md)): **≥75% line &
function** unit-test coverage on all modules; one **`check`** command per side
(`bun run check` / `make check`) running format + lint + typecheck + tests; **TS 7
(`tsc`)** typecheck with **`oxlint` + `oxfmt`** at max strictness (type assertions
banned) on the Bun side, and **SwiftLint `--strict` + swift-format +
warnings-as-errors** (force-unwrap/cast/try banned) on the Swift side; **all deps
pinned to exact versions** with committed lockfiles; **GitHub Actions CI**
enforcing both `check`s on every push to `main` and on PRs.

### D16 — Agent runtime is a provider abstraction; Pi is one implementation (amends D1)
D1 held Pi as the *exclusive* runtime; we amend it. We treat the agent runtime as
an **abstract provider** — the `ExecutionRunner` port — and make `pi --mode rpc`
its first, and for now **only**, adapter (`PiAgentRunner`). We extend D14 (we make
the provider abstraction universal) onto the runtime itself: our core depends on
the **owned, provider-neutral session model**, never on Pi. We treat "where it
runs" (local/remote) as a separate adapter sub-axis *within* an adapter, distinct
from "which engine."

*Key nuance:* Pi's own event/session abstractions **already generalize across
model providers** (Pi drives many underlying models), so they themselves make a
good neutral abstraction. We therefore **mirror Pi's shape as our owned Effect
`Schema`** — we give our types plain names and translate the foreign
`Rpc*`/`AgentSessionEvent` → our `SessionEvent`/`SessionInput`/`UiResponse` at the
adapter boundary — rather than reinventing a different denominator. We do **not**
import Pi's types (D12); FE2.2 validates the owned **Pi-protocol** schema against
real `pi --mode rpc` output (the neutral model is what the Pi adapter translates
that into — the neutral schemas here are not themselves decoded from Pi bytes). We
target no second runtime now: we build the abstraction as insurance against
lock-in, not as a multi-runtime program. Consequence: the frozen RPC contract
(FE2.3) and the client never see a Pi concept, and adding or swapping a runtime
never ripples to the contract.

### D17 — Maximally reactive end-to-end (INV-REACTIVE)
**We push everything reactively from the agent harness to the UI render — we never
poll where a stream fits.** We build the reactive spine as: Pi stdout (NDJSON) →
Effect `Stream` → per-session `PubSub` → streaming RPC (`RpcSchema.Stream` over
`effect/unstable/rpc`) → client subscription → reactive UI render (SwiftUI
`@Observable`; the eventual web client the same way). We thread INV-REACTIVE into
every layer's acceptance:
- we make the contract's `events` (work-graph deltas) and `sessionEvents` channels
  **streaming-first**, not request/response polling;
- we make the owned `SessionEvent` stream carry the **full reactive flow** —
  fine-grained message/tool **deltas** *and* the durable transcript **entries** —
  so a client renders live and reconciles deltas into the transcript-grade record;
- we compose the daemon side with `Stream`/`PubSub`/`Scope`, not poll loops;
- we hydrate with snapshot-on-connect + live-subscribe (D4), not periodic refetch.

### D18 — Closing-PR detection stays an offline heuristic, gated + documented (AE5.1)
The `Repository` port's `closingPullRequest` finds a PR *referencing* an Issue by
scanning the Issue timeline for a `cross-referenced` event whose source carries a
`pull_request` (the GitHub adapter). This is a **heuristic**, not a guarantee:
GitHub emits `cross-referenced` on any mention, so it can pick a PR that references
but does not actually close the Issue. Since startup reconciliation (AE5.1) now
drives real "landed" decisions, we resolve the carried risk (AE3.2 / #27 F1)
**explicitly** rather than silently inheriting it:

- **We keep the offline heuristic**, but the reconciler NEVER lands on it alone: an
  Issue is landed only when the host reports it **closed** AND the referenced PR is
  **merged** (`reconcileIssue`). That gate makes the common case correct offline and
  needs no live GraphQL round-trip.
- **Residual risk (accepted, documented):** an Issue closed for an unrelated reason
  while a *different* merged PR merely references it could be mis-landed. The robust
  signal — GraphQL `closedByPullRequestsReferences`, or gating on the timeline
  `closed` event's associated PR — is a **live-wiring** concern of the GitHub
  adapter (it can only be validated against the real host, not the offline suite),
  so it is tracked as deferred provisioning below, NOT a dropped AE5.1 criterion.
  Roll-up is one-directional (D13) and idempotent, so a future robust signal
  supersedes a stale landing on the next reconcile with no migration.

---

## Deferred (see architecture §10)

- Pi binary provisioning + schema/version-compat policy.
- **Concrete LocalPi `ExecutionRunner` adapter + runnable daemon `main` entrypoint**
  (AE5.1 scope note): AE5.1 delivers the persist/reconcile/re-dispatch **logic** —
  the `StartupReconcile` service wired to the `StateStore`/`Repository`/`JobRunner`
  ports and tested offline (`layerMemory` + a real tmpfile SQLite + fakes). The
  adapter that spawns a real `pi` process and the boot entrypoint that wires the
  production adapters and calls `StartupReconcile.run` are **provisioning**, not part
  of AE5.1's Done.
- **Robust closing-PR signal** (D18): GraphQL `closedByPullRequestsReferences` (or
  the `closed`-event PR) in the GitHub adapter — a live-wiring concern; the offline
  cross-reference heuristic + closed/merged gate is what ships now.
- Inspector transcript rendering: native vs. embed `export-html`.
- StateStore local backing; how much of resume is `WorkflowEngine` vs. custom.
- Remote adapters: `effect/unstable/cluster` vs. hand-rolled tunnel.
- Multi-client write-conflict policy.
