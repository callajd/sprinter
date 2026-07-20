# Sprinter

A GUI-driven, GitHub-aware control plane for planning, running, and monitoring
cross-repo agentic development **workstreams**.

Sprinter is **not** a coding agent. It is the deterministic system that *plans*
the work, *schedules* it as a dependency DAG, *runs* it by driving
[Pi](https://pi.dev) agents as stateless cognitive workers, and gives you
insight into *how* the work was done — including each agent's complete transcript
paired 1:1 with the PR it produced.

## Shape

```
  SwiftUI app  ──┐                        (Effect/TS web client, later)
                 │  effect/unstable/rpc
                 ▼
        ┌──────────────────────┐   GitHub API   ┌──────────┐
        │   Sprinter daemon     │◀──────────────▶│  GitHub  │  ← work-graph truth
        │  (Effect, durable)    │                └──────────┘
        │  • DAG scheduler      │   StateStore
        │  • workflow engine    │◀──────────────▶ (local / hosted)
        │  • reconciliation     │
        └──────────┬───────────┘
                   │ ExecutionRunner  (spawns `pi --mode rpc`, NDJSON/stdio)
                   ▼
            ┌──────────────┐   ┌──────────────┐
            │  pi agent #1 │…  │  pi agent #N │  ← cognitive workers, 1 session = 1 PR
            └──────────────┘   └──────────────┘   (each in its own git worktree)
```

## Domain model

- **Issue** — one ~PR-sized unit of work. Executed by one agent session.
- **Epic** — a related set of Issues.
- **Workstream** — a related set of Epics, defined by one spec, scoped to one repo.

Issues/Epics/Workstreams form a dependency **DAG**; the daemon owns topo-order
and parallelism. At any time many workstreams run across many repos.

## Principles

1. **The daemon owns control flow and state; agents only do cognition.**
   Scheduling, retries, fail-forward, worktrees, GitHub, and durability are
   deterministic daemon code — never delegated to an agent.
2. **The UI is a detachable client, never the system.** Close it and work
   continues; reopen and it re-derives all state and re-subscribes.
3. **Ports-and-adapters.** Execution, state, and transport are Effect Services
   with swappable local/remote Layers. Deployment is adapter selection, not a
   rewrite.
4. **Pi is an external binary, not a dependency.** We invoke `pi --mode rpc` and
   speak an owned NDJSON protocol schema. Zero npm dependency on any `pi-*`
   package.

## Stack

- **Everything** on [Effect](https://effect.website) `v4.0.0-beta.97` primitives.
- Daemon↔client contract on `effect/unstable/rpc`.
- Native macOS client in SwiftUI (a foreign consumer of the RPC contract).

## Docs

- [`docs/architecture.md`](docs/architecture.md) — the reference architecture.
- [`docs/decisions.md`](docs/decisions.md) — the decision log and rationale.
- [`docs/plan/`](docs/plan/) — open workstream specs; one file per workstream.
- [`docs/conventions.md`](docs/conventions.md) — naming & API conventions.
- [`docs/policy.md`](docs/policy.md) — engineering policy (coverage, `check`, lint, pinning, CI).
