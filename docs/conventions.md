# Sprinter — Naming & API Conventions

Coherence, cohesion, and consistency in the API surface are first-class concerns.
These rules are binding; they derive from decision **D14** (the universal provider
invariant).

## Meta-vocabulary (fixed)

- **port** — an Effect `Context.Service` the core depends on (an interface/seam).
- **adapter** — a concrete `Layer` implementing a port.
- **instance** — a running adapter (e.g. "the SQLite instance of `StateStore`").
- "provider" is an informal synonym for *port* only — **never** a concrete type
  suffix.

## Ports

- Named by **role-noun**: `ExecutionRunner`, `StateStore`, `ControlPlaneTransport`,
  `Repository`, `Backend`.
- Do **not** reuse a meta-word (`Provider`, `Adapter`, `Port`) as a concrete port
  suffix.
- The core depends only on ports. *Every* external system and location-variant is
  a port with adapters — "local vs. remote" is just one adapter axis (others:
  which backing, which host).
- Service tag ids: `sprinter/<area>/<Name>` (e.g. `sprinter/repository/Repository`).

## Owned vs. foreign types

- Our domain types get **plain** names: `Job`, `Session`, `SessionEvent`,
  `Workstream`, `Epic`, `Issue`.
- Foreign/protocol types keep a **qualifier**: `AgentSessionEvent`, `RpcCommand`,
  `RpcResponse` are **Pi's**.
- Never leak a foreign type past the boundary that owns its translation — e.g.
  `LocalPiRunner` translates Pi's `AgentSessionEvent` → our `SessionEvent`; nothing
  above the runner sees Pi's type.

## Sub-component suffixes

- Persistence sub-components of a store → `*Store` (`WorkGraphStore`, `JobStore`,
  `EventLogStore`).
- External-system capability groups → `*Ops` (`CodeOps`, `IssueOps`,
  `PullRequestOps`).
- Errors → `*Error` via `Schema.TaggedErrorClass`.

## Domain vocabulary

- Use Sprinter's words, not a vendor's: **PR** / **pull request** (not GitHub's
  "pulls"); **Issue**; **repository**.
- Fixed hierarchy: **Workstream ⊃ Epic ⊃ Issue**. The execution unit is a **Job**
  (1 Job = 1 session = 1 transcript = 1 PR).

## Effect idioms

- Services via `Context.Service`; implementations via `Layer.effect` + `Service.of`.
- Effectful functions via `Effect.fn("Name.method")`; prefer `Effect.gen`.
- Schemas + validation via `Schema`; errors via `Schema.TaggedErrorClass`.
