# Sprinter — Reference Architecture

> Status: design reference. Effect APIs cited are `v4.0.0-beta.97`; code blocks
> are **illustrative idiom**, not final signatures. Exact call shapes are
> verified against the Effect sources (`~/.local/share/effect-beta`) as each
> slice is implemented.

## 1. Purpose

Sprinter is a control plane for agentic software development. A human plans work
in an interactive session; that plan becomes a dependency DAG of Issues/Epics
grouped into Workstreams; a durable daemon schedules and runs each Issue by
driving a Pi agent as a cognitive worker; and a GUI monitors everything and pairs
each agent's transcript with the PR it produced. Multiple workstreams run
concurrently across multiple repositories.

## 2. Domain model

| Concept | Definition | Execution |
|---|---|---|
| **Issue** | one ~PR-sized feature | one agent **session** → one branch → one PR |
| **Epic** | a related set of Issues | scheduled group |
| **Workstream** | a related set of Epics, one spec, one repo | top-level unit driven to done |

Issues/Epics/Workstreams form a **DAG** on their dependencies. The daemon owns
topo-order, the ready-set, and parallelism limits. Work is **repo-scoped** per
workstream; concurrency across repos is unbounded by design.

### Job = Session = Transcript = PR

The unit of execution is a **Job**: one bounded cognitive task, run as one Pi
**session**, producing one durable **transcript**, paired 1:1 with one **PR**.
Job kinds are an open set — `implement`, `review`, `resolve-conflict`,
`address-findings`, `plan`, … The daemon core is agnostic to the kind; kind
lives in per-kind handlers.

### Cognitive worker ↔ interactive session is a spectrum

Every session runs **headless by default** but is **promotable to interactive**
and is always **interruptible**. "Planning" is simply a session started
interactive whose product is a plan. There is no separate interactive subsystem —
one mechanism, pointed at different sessions.

## 3. Topology

```
Clients (SwiftUI now; Effect/TS web later)
   │  effect/unstable/rpc  (transport-agnostic)
   ▼
Sprinter daemon  ── deterministic, durable, long-lived ──────────────┐
   • Scheduler (DAG → ready-set → dispatch)                          │
   • Durable workflow engine (resume across restart)                 │
   • GitHub reconciliation                                           │
   • Ports:  ControlPlaneTransport | StateStore | ExecutionRunner    │
   ▼                          ▲                         │
GitHub (Issues + PRs)      StateStore (work graph)      │ ExecutionRunner
                                                        ▼
                            pi agents  (`pi --mode rpc`, one per Job)
```

Three durability tiers (see §9): **work graph** (StateStore — GitHub holds only
the leaf Issues/PRs; survives everything), **execution** (Pi session JSONL,
resumable), **live process** (ephemeral, reconstructed). The client connection is
a fourth, fully ephemeral tier.

**GitHub boundary:** GitHub is the *code layer* — Issues and the PRs that close
them, nothing higher. **Workstreams and Epics are Sprinter-only**, owned by the
`StateStore`, as is the dependency DAG and the workstream→epic→Issue→PR mapping.
GitHub Projects v2 is deliberately **not** used. Reconciliation is therefore
one-directional and Issue/PR-level: Sprinter reads which Issues closed / which
PRs merged and rolls that up into Epic/Workstream status internally; it never
reads planning state back from GitHub.

## 4. Ports (Effect Services + swappable Layers)

**Invariant:** the daemon core depends only on **ports** — `Context.Service`s —
and *every* external system and location-variant is one, behind swappable adapter
Layers. "Local vs. remote" is merely one adapter axis (others: which backing,
which host). Layers mix freely (e.g. local execution + hosted state + a GitHub
`Repository`). The client mirrors this discipline: the SwiftUI app depends on a
`Backend`, never on a *local* daemon (see §7–8).

> **We make the agent runtime itself a provider (D16, amends D1).**
> `ExecutionRunner` is the abstract agent-runtime port; `pi --mode rpc`
> (`PiAgentRunner`) is its first and only current adapter. Our core depends on the
> owned, provider-neutral session model, never on Pi — we mirror Pi's (already
> model-provider-general) event shape as our owned `Schema` and translate it at the
> adapter boundary. And **we keep everything maximally reactive end-to-end (D17,
> INV-REACTIVE)** — we run a push spine from Pi stdout → `Stream` → `PubSub` →
> streaming RPC → reactive UI.

```ts
// illustrative
import { Context, Effect, Layer, Stream, Scope, Schema } from "effect"

// ── ExecutionRunner ────────────────────────────────────────────────
export class ExecutionRunner extends Context.Service<ExecutionRunner, {
  // Start a session (headless by default); the handle is Scope-managed.
  readonly run: (job: Job) => Effect.Effect<SessionHandle, SpawnError, Scope.Scope>
  // Re-attach to a persisted session (resume path).
  readonly resume: (ref: SessionRef) => Effect.Effect<SessionHandle, SpawnError, Scope.Scope>
}>()("sprinter/execution/ExecutionRunner") {}

export interface SessionHandle {
  readonly sessionId: SessionId
  readonly events: Stream.Stream<SessionEvent>                      // owned; translated from Pi's AgentSessionEvent
  readonly send: (input: SessionInput) => Effect.Effect<void>        // prompt | steer | follow_up
  readonly interrupt: Effect.Effect<void>                           // abort
  readonly result: Effect.Effect<JobResult>                         // settles when the job ends
}

// ── StateStore ─────────────────────────────────────────────────────
export class StateStore extends Context.Service<StateStore, {
  readonly workGraph: WorkGraphStore     // workstream/epic/issue + status + deps
  readonly jobs: JobStore                // job/session registry, transcript refs
  readonly events: EventLogStore         // append-only projection feed
}>()("sprinter/state/StateStore") {}

// ── Repository (repo-scoped code host) ─────────────────────────────
// Everything Sprinter needs from a host is repo-scoped: Code (branches/merge),
// Issues, PRs. The port is a single repository; GitHub is one adapter.
export class Repository extends Context.Service<Repository, {
  readonly code: CodeOps       // branches, push, merge
  readonly issues: IssueOps    // read / list / close, status
  readonly pullRequests: PullRequestOps   // open / read / merge PRs, issue↔PR links
}>()("sprinter/repository/Repository") {}

// ── ControlPlaneTransport ──────────────────────────────────────────
// Not a hand-written service: it is an effect/unstable/rpc RpcGroup served over
// a transport Layer. See §7.
```

**Adapters** — "local/remote" is only one axis; the general shape is
*first instance → other adapters*:

| Port | First instance → other adapters |
|---|---|
| `ExecutionRunner` | local `pi --mode rpc` (`unstable/process`) → remote runner (`unstable/cluster`) |
| `StateStore` | SQL/SQLite (`unstable/sql`) → Postgres, hosted store, in-memory |
| `ControlPlaneTransport` | local socket → authenticated HTTP/WS |
| `Repository` | GitHub.com → GitHub Enterprise, other hosts |
| `Backend` *(client)* | local daemon → remote/hosted daemon |

Per-session/per-job resources (child process, subscriber set) are
candidates for `LayerMap.Service`, which builds and tears down layers keyed by
an id — a natural fit for "one resource bundle per running job."

## 5. Pi integration boundary

**Pi is an external binary, not a library.** The daemon spawns `pi --mode rpc`
and communicates over **newline-delimited JSON on the child's stdio**. Pi's
internals (`pi-ai`, `pi-agent-core`, `pi-tui`) are sealed behind that process
boundary and are **not** in Sprinter's dependency graph.

The only thing crossing the boundary is the **wire protocol**, which Sprinter
**owns** as its own Effect `Schema` (a mirror of the subset of Pi's
`RpcCommand` / `RpcResponse` / `AgentSessionEvent` we use). We author it against
Pi's `packages/coding-agent/src/modes/rpc/rpc-types.ts` as reference; we do not
import it. Owning the schema gives us:

- Schema-validated decoding of untrusted child stdout at the boundary.
- Decoupling from an experimental, churning upstream package's types.
- One canonical protocol definition to hand-mirror in Swift.

### The `pi-orchestrator` package is a reference, not a foundation

`pi-orchestrator` is a *peer* to this daemon — a thin, experimental Node
supervisor + Unix-socket multiplexer over the same `pi --mode rpc` primitive.
Building on it would add a redundant transport hop and inherit its bugs
(multi-client UI-handler overwrite; buffered-command-after-upgrade; no
resume-after-restart; provider/model not threaded through spawn). We instead
**mine its `rpc-process.ts`** for the hard parts — NDJSON framing,
pending-`id` request tracking, the `extension_ui_request` bridge — and
reimplement them natively in Effect.

### ExecutionRunner local adapter (`LocalPiRunner`)

```ts
// illustrative flow, effect/unstable/process
// 1. ChildProcess.make("pi", "--mode", "rpc") |> setCwd(cwd) |> setEnv(...)
// 2. spawn under Scope (ChildProcessSpawner) → auto-kill on scope close
// 3. stdout: Ndjson decode → Schema.decode(AgentSessionEvent) → translate to SessionEvent
//            → published to a per-session PubSub (SessionHandle.events subscribes)
// 4. stdin:  SessionInput → Schema.encode → Ndjson encode → child.stdin
//            correlate responses by `id` (Deferred per pending command)
// 5. interrupt: send `abort`, then process kill on scope close
```

`send`/`interrupt` map directly onto Pi's RPC verbs (`prompt`, `steer`,
`follow_up`, `abort`) — the interactive/interruptible requirement is threading
existing verbs upward, not new capability.

### Provisioning

Because Pi is a binary prerequisite, the real compatibility risk is **protocol
drift** between the installed `pi` version and our owned schema. Options (open,
§10): user-global install, a bundled pinned binary, or a daemon-managed pinned
binary. Whichever we pick, the `pi` version is pinned and matched to a schema
version.

## 6. Daemon core — scheduling and durability

The deterministic orchestration logic — scheduling, topo-order, fail-forward,
sweep — lives in the daemon as ordinary code:

- **Scheduler** — reads the work-graph DAG, computes the ready-set (deps
  satisfied), respects parallelism limits, dispatches Jobs, applies fail-forward
  policy, sweeps close-out debt.
- **Durable workflow spine** — `effect/unstable/workflow`. Each Job is modeled
  as an `Activity` carrying an **`idempotencyKey`** (the Job id), so a completed
  Job is never re-run on resume; the workstream DAG is a `Workflow` composing
  Activities in dependency order. `WorkflowEngine` persistence is what makes
  "survive daemon/machine restart" close to free rather than hand-rolled.
  `DurableDeferred`/`DurableQueue`/`DurableClock` cover waits, backpressure, and
  timeouts across restarts.
- **Reconciliation** — on startup (and periodically) the daemon reconciles its
  work graph against each repo through the `Repository` port (which PRs merged, which issues closed while it
  was down) before deciding what to resume. GitHub is a durable, always-on
  source of truth the daemon never has to rebuild.

### Restart/resume (revised)

Resume is **per-Job**, not a monolithic-agent replay:

1. Reconcile work graph ↔ GitHub.
2. For each Job that was `running`: either re-dispatch from its inputs, or
   `ExecutionRunner.resume(ref)` onto the persisted Pi session (Pi's
   `switch_session`) and continue.
3. Workflow Activities with satisfied idempotency keys short-circuit.

## 7. Daemon ↔ client contract (`effect/unstable/rpc`)

The contract is an `RpcGroup` served over a transport Layer (the transport is a
swappable detail — local socket or authenticated HTTP/WS). It carries **four
models**:

1. **Query / snapshot** — hydrate full state on connect ("reopen → sync every
   workstream, any state"). Request/response RPCs.
2. **Live subscription** — deltas after the snapshot. A **streaming RPC**
   (`RpcSchema.Stream`) backed by a daemon `PubSub`; this is how one daemon fans
   out to many attached clients.
3. **Commands** — the write path: create-workstream-from-plan, start / pause /
   resume / cancel, retry a failed Issue.
4. **Interactive session channel** — for the "every session interactive" and
   "planning in the app" requirements:
   - a streaming RPC to follow a specific session's events (incl. Pi
     `extension_ui_request`s the agent raises mid-run — the "agent is waiting on
     you" inbox),
   - request/response RPCs to `send` (`prompt`/`steer`/`follow_up`),
     `interrupt`, and answer a `ui_request`.

```ts
// illustrative — effect/unstable/rpc
import { RpcGroup, Rpc } from "effect/unstable/rpc"

export class SprinterRpc extends RpcGroup.make(
  Rpc.make("snapshot",          { success: Snapshot }),
  Rpc.make("events",            { success: RpcSchema.Stream(DomainEvent, Never) }),   // live subscription
  Rpc.make("createWorkstream",  { payload: PlanRef, success: WorkstreamId, error: PlanError }),
  Rpc.make("controlWorkstream", { payload: ControlCmd, success: Void }),
  Rpc.make("sessionEvents",     { payload: SessionId, success: RpcSchema.Stream(SessionEvent, Never) }),
  Rpc.make("sessionSend",       { payload: SessionInput }),
  Rpc.make("answerUiRequest",   { payload: UiResponse }),
) {}
```

### Swift-client constraint

The SwiftUI client is a **foreign consumer**: no Effect in Swift, so it cannot
share types — it mirrors the RPC message schemas by hand or codegen against the
wire protocol. This puts a premium on a **small, stable, cleanly-schematized RPC
surface**: every procedure is implemented twice (free in the eventual Effect/TS
web client, by hand in Swift). Keep the group minimal; goldens frozen from the TS
contract keep the two sides in lockstep (both live in one repo and land together).

## 8. UI surfaces

The client is **three surfaces over one contract**, not a monolith:

1. **Interactive session** — drive and interrupt *any* session. Reused for
   **planning** (a fresh interactive session whose output materializes into the
   work graph) *and* for "taking the wheel" of any in-flight Job. Subscribes to
   `sessionEvents`; issues `sessionSend`/`interrupt`/`answerUiRequest`.
2. **Mission Control** — the cross-repo DAG/board: workstream → epic → issue with
   live status (ongoing / paused / complete), per-agent activity, token/cost
   budgets, and the "agent is waiting on you" inbox. Driven by `snapshot` +
   `events`.
3. **Inspector** — the transcript↔PR paired view: an agent's complete session
   rendered alongside the PR it produced. Transcript rendering is an open choice
   (§10): render natively from Pi session entries vs. embed Pi's `export-html`.

## 9. Cross-cutting

### Durability tiers

| Tier | State | Backing | Survives |
|---|---|---|---|
| Work graph | workstream/epic/issue, DAG, status | StateStore (GitHub = leaf Issues/PRs) | everything |
| Execution | in-flight session, transcript | Pi session JSONL | daemon restart (resumable) |
| Live process | running `pi` child, active stream | — | nothing (reconstructed) |
| Client connection | attached view | — | nothing (reattaches) |

### Auth

- **Pi/provider auth** is Pi's, behind the binary (`~/.pi/agent/auth.json`); the
  daemon does not reimplement it.
- **GitHub** via a GitHub App / token in the daemon.
- **Client↔daemon** auth lives in the *remote* `ControlPlaneTransport` layer
  (the local layer is loopback). The daemon never exposes a raw socket remotely.

### Observability

Beyond the domain event feed, the daemon emits Effect spans/metrics
(`effect/unstable/observability`) for scheduler decisions, Job lifecycles, and Pi
process health.

## 10. Open questions

> **Resolved:** GitHub representation — GitHub = Issues + PRs only; Workstreams
> and Epics are Sprinter-only (StateStore); Projects v2 rejected. See §3 "GitHub
> boundary" and decision log D13.

1. **Pi binary provisioning** — global install vs. bundled pinned binary vs.
   daemon-managed; and the schema↔pi version-compat policy.
2. **Transcript rendering in the Inspector** — native from session entries
   (interactive, more work) vs. embed Pi `export-html` (fast, static).
3. **StateStore first adapter** — which backing to implement first *behind* the
   backing-agnostic `StateStore` Service (a SQL adapter, e.g. SQLite, vs. eventlog
   + persistence primitives), and how much of resume is `WorkflowEngine` vs.
   custom. The port never depends on the instance.
4. **Remote adapters** — evaluate `effect/unstable/cluster` for remote execution
   and control-plane vs. a hand-rolled HTTP/WS tunnel.
5. **Multi-client write conflicts** — two clients answering the same
   `ui_request` or both pausing a workstream (likely last-writer-wins + audit,
   deferred).

## 11. Build sequence (proposed slices)

1. **Owned Pi protocol schema** + `ExecutionRunner` local adapter (`LocalPiRunner`):
   spawn, NDJSON bridge, `SessionHandle`, resume. The Pi-specific heart.
2. **StateStore** + the DAG scheduler on the durable `Workflow` spine.
3. **`ControlPlaneTransport`** RpcGroup + the snapshot/subscription/command/
   session models; reconnect + resync protocol.
4. **SwiftUI client** — Mission Control first, then Interactive session, then
   Inspector.
5. **Remote adapters** — cluster evaluation for `ExecutionRunner` and transport.

Each slice verifies its Effect APIs against `~/.local/share/effect-beta` before
committing code.
