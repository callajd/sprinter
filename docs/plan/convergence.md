# Workstream `CVG` — Convergence / cutover

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** point the SwiftUI app at the **real** daemon (Track A handlers real,
> not fakes) and reach **cutover** — drive a real Sprinter **Issue → open PR
> _through the app_**, observed live in Mission Control and paired in the
> Inspector, **restart-safe**. (Plan [index](./README.md#convergence--cutover).)
>
> **Prerequisites (cross-workstream):** **`FDN`, `TRK-A`, and `TRK-B` are all
> landed.** This is the workstream where the two tracks **meet** — see
> [Convergence relaxes the divergence rule](#convergence-relaxes-the-divergence-rule).
> It builds no new feature logic; it **provisions and wires** what both tracks
> deferred behind ports, then proves the loop end-to-end.

## Convergence relaxes the divergence rule

Everywhere else, "Track A and Track B depend ONLY on the Foundation — never on
each other" ([README](./README.md#dependency-rule-invariant)). `CVG` is the sole,
deliberate exception: it is the node where the daemon's served endpoint and the
app's live transport connect. The coupling is **still only through the frozen RPC
contract** — `CVG` wires a concrete transport and a daemon `main`, it does not let
Swift feature code import daemon internals or vice-versa. Every change stays behind
the existing ports (`Backend` / `RpcTransport` on the app side; `ExecutionRunner` /
`StateStore` / `Repository` on the daemon side); convergence supplies the concrete
**adapters** those ports were designed to accept.

## Cross-cutting invariants

This is the one workstream that spans **both** sides, so **both** repo gates apply
— each task names the side(s) it touches and honours that side's gate. From
[`policy.md`](../policy.md) / [`conventions.md`](../conventions.md) /
[`decisions.md`](../decisions.md).

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE-A` | Daemon-side changes: `bun run check` green (format + lint + typecheck + tests) | `bun run check` + CI |
| `INV-GATE-B` | App-side changes: `make check` green (swift-format + SwiftLint + build + tests) | `make check` + CI |
| `INV-COV` | ≥ 75% line & function coverage on the task's modules (the side it touches) | coverage gate in that side's `check` |
| `INV-NOCAST` | Daemon side: no `as`/non-null `!`/`any` | `oxlint` |
| `INV-NOFORCE` | App side: no force unwrap/cast/try; no IUOs | `SwiftLint --strict` |
| `INV-PIN` | New deps exact-pinned; lockfile / `Package.resolved` committed (the side it touches) | `check` + committed lock |
| `INV-NAMING` | Follows `conventions.md` | review |
| `INV-PORT` | The new wiring is **adapters behind existing ports** — no feature surface gains localness/transport knowledge; the concrete transport/daemon `main` are the only new edges | review |
| `INV-EFFECT-DI` | **Daemon side:** every concrete port implementation is provided as an Effect **`Layer`** (`Layer.effect`/`scoped`/`succeed` over a `Context.Service` tag) and composed into the daemon `main`'s layer graph — no `new`/ad-hoc instantiation or manual wiring that bypasses DI; swapping an adapter (fake ↔ real, one host ↔ another) is a **`Layer` substitution**, nothing else | review + `bun run check` |
| `INV-CONTRACT` | Both sides consume the **same** frozen contract. A contract change ripples to the Swift mirror **and** Track A's handlers, batched, with `FE2.4` goldens + decode tests re-passing on both sides | `bun run check` + `make check` + review |
| `INV-RESTART` | Cutover survives a daemon restart mid-flight (AE5 durability exercised end-to-end): reconnect resync + re-dispatch, no lost/duplicated work | integration test + review |

`INV-GATE-A`/`INV-GATE-B` (whichever the task touches), `INV-COV`, `INV-NOCAST`/
`INV-NOFORCE`, `INV-PIN`, `INV-NAMING` apply to every task. `INV-EFFECT-DI`
applies to every **daemon-side** task (`CE1`, and `CE5`'s handler half) and is a
**standing design goal**, not a one-time provisioning detail — as convergence
iterates, each new concrete port stays an Effect `Layer` composed into `main`.

## Epics — set & sequencing

| Epic | Name | Side | Depends on |
|------|------|------|-----------|
| `CE1` | Daemon process — serve the contract for real | daemon (`bun`) | — (needs `TRK-A`) |
| `CE2` | Live client transport | app (`swift`) | `CE1`, (`TRK-B` `BE1`) |
| `CE3` | App shell — SwiftUI Views + `.app` | app (`swift`) | `CE2`, (`TRK-B` `BE2`–`BE4`) |
| `CE4` | Cutover — real Issue → PR through the app, restart-safe | both | `CE1`, `CE2`, `CE3` |
| `CE5` | Batched contract changes (gated on need) | both | — (needs `FDN` contract) |

```
(FDN + TRK-A + TRK-B all landed)

CE1 ─► CE2 ─► CE3 ─► CE4   (the cutover spine)

CE5 (batched contract changes) ┄┄ optional, batched; ripples to CE1 handlers + CE3 UI if run
```

> `CE5` is **conditional** — the two changes it batches are only worth cutting if
> the product needs them for the demo (see the epic). It is sequenced independent
> of the spine; if run, land it **before** the epics it ripples into (`CE1`/`CE3`)
> so the contract re-freeze happens once.

---

## Epic `CE1` — Daemon process (serve the contract for real)  ·  tags: `INV-PORT`, `INV-EFFECT-DI`, `INV-GATE-A`

Provision the runnable daemon: the concrete `ExecutionRunner` over real Pi, a
served `RpcServer` endpoint over a concrete transport, file-backed restart-safe
state, and the `Repository` GitHub adapter hardened for live decisions. All of
this was deferred out of `TRK-A` behind ports (architecture §10) — `CE1` supplies
the adapters and the `main`.

**Design goal (`INV-EFFECT-DI`, binding across all `CE1` tasks):** each concrete
port here is an Effect **`Layer`** over its `Context.Service` tag
(`StateStore`, `ExecutionRunner`, `Repository`, `RpcServer`), and the daemon
`main` is the **composition root** — a single layer graph
(`Layer.provide`/`Layer.mergeAll`) assembling them. No adapter is `new`-ed or hand-
wired outside DI; selecting real-vs-fake or one host-vs-another is a `Layer`
substitution and nothing more. This keeps the real daemon testable by the same
mechanism the fakes used (provide a different `Layer`).

### CE1.1 — Real-Pi `ExecutionRunner` adapter
- **Done:** the concrete `LocalPi` `ExecutionRunner` (the `packages/job`
  `ExecutionRunner` port) over `@sprinter/runner` `makeSession`, replacing the
  fake. It **honours the terminal-result contract** — the session reaches a
  terminal `SessionResult` (spawn `pi` one-shot, or drive to `SessionIdle` /
  `agent_settled` then close the session scope) so `JobRunner.dispatch` never
  hangs on `handle.result` — and builds the **real prompt from Issue content**
  (title/spec/body via `Repository`/`StateStore`), not the id-only
  `promptForJob` placeholder. Provided as an Effect **`Layer`** over the
  `ExecutionRunner` `Context.Service` (`INV-EFFECT-DI`) — a drop-in `Layer`
  substitution for the fake, requiring no change above the tag.
- **Depends on:** — (needs `TRK-A` `AE3`/`AE1`)

### CE1.2 — Daemon `main` + served transport
- **Done:** a runnable daemon entrypoint wiring `StateStore` (**file-backed**,
  not `:memory:`) + `JobRunner` + `CE1.1`'s runner + `Repository` + `RpcServer`
  into a **served endpoint over a concrete transport** (the wire the app dials).
  The `events` feed exposes the **offset-based resync** endpoint (AE1
  `EventLogStore.tail(offset)`) so a reconnecting client catches up deterministically
  rather than only snapshot-on-connect. `main` is the **composition root** — a
  single Effect layer graph (`Layer.provide`/`Layer.mergeAll`) assembling the
  `StateStore`/`ExecutionRunner`/`Repository`/`RpcServer` `Layer`s
  (`INV-EFFECT-DI`); nothing is instantiated outside DI.
- **Depends on:** `CE1.1`

### CE1.3 — `Repository` live-wiring hardening
- **Done:** before live roll-up drives real decisions — **robust closing-PR
  detection** (replace the timeline `cross-referenced` heuristic in
  `packages/repository/src/github.ts` with GraphQL
  `closedByPullRequestsReferences`, or gate on the `closed` event's associated PR;
  the heuristic false-positives when an unrelated merged PR merely mentions a
  hand-closed Issue) and **reconcile error isolation** (`reconcileWorkstream` is
  fail-fast `Effect.forEach`; add per-issue catch-and-continue so one host
  404/403/429 doesn't abort the whole workstream roll-up). Stays the same
  `Repository` `Layer` (`layer`/`layerFetch` over the `Context.Service`) —
  hardening is internal to the adapter, not a new wiring path (`INV-EFFECT-DI`).
- **Depends on:** — (needs `TRK-A` `AE3`)

---

## Epic `CE2` — Live client transport  ·  tags: `INV-PORT`, `INV-CONTRACT`, `INV-GATE-B`

The concrete Swift `RpcTransport` that dials `CE1.2`'s endpoint — the one new
adapter behind the `Backend` port. Feature code (Mission Control / session /
inspector view models) is unchanged; it just receives a live `Backend` instead of
a fake.

### CE2.1 — Concrete transport + endpoint selection
- **Done:** a concrete `RpcTransport` (socket/stdio, matching `CE1.2`'s wire)
  speaking the NDJSON `effect/unstable/rpc` envelope, selected via
  `BackendConnector`/`DaemonEndpoint` (local vs remote); `RpcBackend` drives it
  unchanged. `FE2.4` decode goldens keep passing against real frames.
- **Depends on:** `CE1.2` (needs `TRK-B` `BE1.1`)

### CE2.2 — Backpressure, durable replay, reconnect hardening
- **Done:** the live-firehose hardening `BE1.2` deferred — **demand-gated
  backpressure** (defer the per-batch `Ack` until the consumer drains; a
  **bounded** buffer whose overflow triggers a **snapshot-resync** rather than a
  silent drop); **offset-based durable replay** (resume from the last-seen offset
  via `CE1.2`'s endpoint, not a fresh snapshot re-derive); and **reconnect backoff
  + jitter** (exponential, replacing `WorkGraphResync`'s constant `retryDelay`)
  against a persistently-failing daemon.
- **Depends on:** `CE2.1` (needs `TRK-B` `BE1.2`)

---

## Epic `CE3` — App shell (SwiftUI Views + `.app`)  ·  tags: `INV-PORT`, `INV-GATE-B`

The executable + Views over the already-built, already-tested view models, plus
the shell-side UX the feature epics explicitly pushed to the edge (D10). No view
model logic changes; this is the thin platform edge.

### CE3.1 — App target + feature Views
- **Done:** an `.executableTarget` with a `@main` SwiftUI `App` and the Views that
  render the existing view models — `MissionControlBoard`, `SessionViewModel`,
  `PlannerViewModel`, `InspectorViewModel` — as the running app. The
  `#if os(...)` shell + AppKit/UIKit glue lives here only (D10); feature libraries
  stay platform-neutral.
- **Depends on:** `CE2.1` (needs `TRK-B` `BE2`–`BE4`)

### CE3.2 — Shell UX at the edge
- **Done:** the deferred shell-side UX — **plan construction** (the Planner Views
  build a `WorkstreamPlan` name/repo/spec form from the planning conversation and
  call `materialize`; no transcript→plan auto-extractor); **inbox wait-time
  ordering** + a **no-longer-outstanding** signal (client-side arrival tracking,
  since the mirrored `UiRequestRaised` carries no timestamp); the **LCS/intraline
  diff view** consuming `BE4`'s `DiffLine`s (so unchanged lines aren't shown as
  churn — the projection stays coarse; the view does the intraline pass); and
  **transcript projection memoization** (memoize on `events.count`/last-index or an
  incremental ordered-item map, avoiding the O(n²) re-fold on SwiftUI re-reads).
- **Depends on:** `CE3.1`

---

## Epic `CE4` — Cutover (end-to-end)  ·  tags: `INV-RESTART`, `INV-CONTRACT`

The acceptance epic — the whole loop, for real, restart-safe. Adds no new surface;
proves the composition.

### CE4.1 — Real Issue → PR through the app
- **Done:** from the running app against the real daemon, materialize a plan into a
  workstream, dispatch a real Sprinter **Issue**, and observe a real **PR open** —
  the board updating live in **Mission Control**, the session driving/interruptible
  in the **Interactive session**, and the transcript **paired with the PR** in the
  **Inspector**. An end-to-end test (or scripted harness) exercises the loop
  against a real-but-sandboxed daemon + repo.
- **Depends on:** `CE1`, `CE2`, `CE3`

### CE4.2 — Restart-safe cutover
- **Done:** the loop survives a **daemon restart mid-flight** — on reconnect the
  app resyncs (snapshot + offset replay), the daemon re-dispatches persisted work
  (AE5 durability, now file-backed via `CE1.2`), and no work is lost or duplicated
  (1 Job = 1 session re-attached by id). Verified by a build-write-**restart**-read
  integration pass across the wire.
- **Depends on:** `CE4.1`

---

## Epic `CE5` — Batched contract changes (gated on need)  ·  tags: `INV-CONTRACT`, `INV-EFFECT-DI`

Two changes both tracks flagged as **out of the frozen contract scope**. Cut **only if
the product needs them**; if cut, **batch** them (a contract change ripples to the
Swift mirror *and* Track A handlers — version once, re-freeze goldens once) and
land before the spine epics they ripple into.

### CE5.1 — Distinct `cancelled` terminal status
- **Done:** a distinct terminal `WorkStatus` for cancellation (the base contract maps `cancel` →
  `done`, so a cancelled workstream is indistinguishable from completed). Changes
  `Snapshot`; ripples to the daemon roll-up + the Swift board projection; goldens
  + decode tests re-pass on both sides.
- **Depends on:** — (needs `FDN` contract; gated on need)

### CE5.2 — Notice reconciliation key
- **Done:** a wire-level reconciliation key on `Notice`/`NoticeEntry` (today they
  carry no id, so a notice emitted **both** live and durable would double-render;
  this is safe only because the Pi adapter emits notices live-only). Cut **only if**
  a daemon/Pi version begins emitting a notice both live and durable. Ripples to
  both sides; goldens re-pass.
- **Depends on:** — (needs `FDN` contract; gated on need — conditional)

---

## Provenance — where each task's scope came from

Every task traces to a deferred wiring-constraint or disputed-but-noted finding
recorded during `TRK-A`/`TRK-B` (ledgers + cold reviews + `docs/decisions.md` /
architecture §10). This workstream **discharges** that backlog; it invents no new
scope.

| Task | Origin |
|------|--------|
| `CE1.1` | TRK-A ledger — AE4/AE5 "concrete LocalPi `ExecutionRunner`" (deferred out of #24; #26 cold-review F1/F5: terminal-result contract + real Issue-content prompt) |
| `CE1.2` | TRK-A complete-note "deferred provisioning" (daemon `main` entrypoint + offset `events` resync); AE5 file-backed durability (AE2 #23 cold-review F2) |
| `CE1.3` | TRK-A ledger — AE4/AE5 "`Repository` GitHub-adapter live-wiring hardening" (#27 cold-review: closing-PR heuristic + reconcile fail-fast) |
| `CE2.1` | TRK-B ledger — deferred live RPC transport (BE1.1 built `RpcBackend` over an *injected* `RpcTransport`, tested against a fake) |
| `CE2.2` | TRK-B ledger — "BE1.2 + convergence transport" (#36 F1 demand-gated backpressure + bounded-buffer→resync) and "Convergence transport" (#37 B1/N4 offset replay + reconnect backoff/jitter) |
| `CE3.1` | TRK-B scope note — SwiftUI View + `.app` shell is convergence, not a TRK-B epic (D10); library-only package today (no executable target) |
| `CE3.2` | TRK-B ledger — planner plan-source (#45 N1), inbox wait-time ordering / no-longer-outstanding (#41 N3/N4), inspector LCS diff view (#47 N1), transcript projection perf (#44 N2) |
| `CE4.1` | README §"Convergence → cutover" — the end-to-end goal |
| `CE4.2` | README (restart-safe) + TRK-A AE5 restart safety (exercised end-to-end over the wire) |
| `CE5.1` | TRK-A ledger — AE5 "`cancelled` status" (#30 N2; a batched-contract consideration) |
| `CE5.2` | TRK-B ledger — "notice reconciliation key" (#44 N1; conditional) |
