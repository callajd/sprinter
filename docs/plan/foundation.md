# Workstream `FDN` — Foundation

> Spec doc for the **`workstream`** skill. One epic set, one sequencing graph, one
> invariant set. `/epic` cuts one issue per task, copies its **Done**, and threads
> each cross-cutting invariant into that acceptance (naming its guard).
>
> **Goal:** deliver the entire shared dependency surface for Track A and Track B —
> scaffold + check gate + CI, domain schemas, the frozen RPC contract, the owned
> Pi protocol schema, the stub server, and the Swift bridge.
>
> **Prerequisites:** none. This workstream unblocks `TRK-A` and `TRK-B`.
> **Exit / divergence point:** `FE5.3` green — the stub serves contract v1 and a
> Swift client decodes it.

## Cross-cutting invariants

Bind every epic; `/epic` threads each applicable one into each task's **Done**.
Sourced from [`policy.md`](../policy.md), [`conventions.md`](../conventions.md),
[`decisions.md`](../decisions.md).

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | The side's `check` is green — format + lint + typecheck + tests | `bun run check` / `make check` + CI |
| `INV-COV` | ≥ 75% line **and** function coverage on the task's modules | coverage threshold inside `check` |
| `INV-NOCAST` | No `as`/non-null `!`/`any` (TS); no force unwrap/cast/try (Swift) | `oxlint` / `SwiftLint --strict` |
| `INV-PIN` | All deps exact-pinned; lockfile committed | `check` dep-pin verification + committed lock |
| `INV-NAMING` | Follows `conventions.md` (ports/adapters, owned vs. foreign, `*Store`/`*Ops`) | cold `pr-review` |
| `INV-PORT` | No concrete backing/instance depended on directly; external systems + locations sit behind ports | cold `pr-review` |
| `INV-CONTRACT` | Contract changes are versioned and ripple to stub + Swift mirror; golden-fixture tests pass | `check` (contract tests) + cold `pr-review` |

`INV-GATE`, `INV-COV`, `INV-NOCAST`, `INV-PIN`, `INV-NAMING` apply to **every**
task. `INV-PORT` / `INV-CONTRACT` are tagged per epic below.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `FE1` | Toolchain, check gate & CI | — |
| `FE2` | Domain schemas | `FE1` |
| `FE3` | Pi protocol & fixture | `FE1` |
| `FE4` | RPC contract v1 | `FE2` |
| `FE5` | Stub server & Swift bridge | `FE4`, `FE3` |

```
FE1 ──► FE2 ──► FE4 ──► FE5
   └──► FE3 ───────────►┘
```

---

## Epic `FE1` — Toolchain, check gate & CI

Implements [`policy.md`](../policy.md). The `check` gate built here **is** the
project verification gate.

### FE1.1 — Bun monorepo scaffold
- **Done:** Bun workspace with `packages/{domain,contract,daemon,stub-server}`;
  `.bun-version` + `packageManager` pinned; strict `tsconfig`; Effect
  `4.0.0-beta.97` pinned exact; `save-exact` on; `bun.lock` committed.
- **Depends on:** —

### FE1.2 — Bun check gate
- **Done:** `oxlint` (bans `as`/`!`/`any` as errors) + `oxfmt --check` +
  `tsc --noEmit` + `bun test --coverage` (threshold `line=0.75, function=0.75`)
  all wired behind **`bun run check`**, exiting non-zero on any violation; tooling
  pinned exact. (If `oxfmt` is not release-ready, fall back to Biome and record it.)
- **Depends on:** `FE1.1`

### FE1.3 — Swift app scaffold + `make check`
- **Done:** `apple/Sprinter/` SwiftPM/Xcode project, Swift 6 mode, strict
  concurrency = complete, warnings-as-errors; `SwiftLint --strict` (force
  unwrap/cast/try = error) + `swift-format` + coverage gate (≥75%) behind
  **`make check`**; deps `.exact()`-pinned, `Package.resolved` committed.
- **Depends on:** —

### FE1.4 — CI workflows
- **Done:** GitHub Actions running `bun run check` (Ubuntu) and `make check`
  (macOS, pinned Xcode) on **push to `main`** and on **PRs**; both must pass; red
  `check` blocks.
- **Depends on:** `FE1.2`, `FE1.3`

---

## Epic `FE2` — Domain schemas  ·  tag: `INV-NAMING`

`packages/domain`. Owned Effect `Schema`; plain names for domain types.

### FE2.1 — Read-model schemas
- **Done:** `Schema` for `Workstream`/`Epic`/`Issue`/`Job`/`Session` + their
  statuses, IDs, and the workstream→epic→Issue→PR mapping shape; round-trip
  encode/decode tested.
- **Depends on:** —

### FE2.2 — `SessionEvent` schema
- **Done:** owned `SessionEvent` rich enough to render a full transcript (messages,
  tool calls, diffs, thinking, ui-requests); named plainly (not `AgentSessionEvent`).
- **Depends on:** —

### FE2.3 — Command & session-I/O schemas
- **Done:** control-command payloads (`createWorkstream`/`controlWorkstream`/…),
  `SessionInput`, `UiResponse`; validated.
- **Depends on:** —

---

## Epic `FE3` — Pi protocol & fixture  ·  tag: `INV-BOUNDARY` (defines the owned type)

### FE3.1 — Capture Pi transcript fixture
- **Done:** a real `pi --mode rpc` NDJSON transcript captured to `fixtures/`, plus
  the capture harness/notes to regenerate it.
- **Depends on:** —

### FE3.2 — Owned Pi protocol schema
- **Done:** owned Effect `Schema` mirroring the `RpcCommand`/`RpcResponse`/
  `AgentSessionEvent` subset we use (authored against Pi's `rpc-types.ts`, **not**
  imported), validated by decoding the `FE3.1` fixture.
- **Depends on:** `FE3.1`

---

## Epic `FE4` — RPC contract v1  ·  tag: `INV-CONTRACT`

`packages/contract`, on `effect/unstable/rpc`. Explicitly versioned.

### FE4.1 — RpcGroup: query + events + commands
- **Done:** `snapshot` (query), `events` (streaming subscription), and command
  RPCs defined over the `FE2` schemas; contract is versioned.
- **Depends on:** `FE2.1`, `FE2.3`

### FE4.2 — RpcGroup: session channel
- **Done:** `sessionEvents` (streaming, `SessionEvent`), `sessionSend`,
  `interrupt`, `answerUiRequest` RPCs; the full contract compiles and is exported.
- **Depends on:** `FE4.1`, `FE2.2`

---

## Epic `FE5` — Stub server & Swift bridge  ·  tag: `INV-CONTRACT`

The divergence enabler. `FE5.3` is the workstream exit gate.

### FE5.1 — Stub RpcServer
- **Done:** an `RpcServer` layer serving contract v1 from in-memory fixtures +
  synthetic events, replaying the `FE3.1` transcript over `sessionEvents`; runnable
  locally over the real transport.
- **Depends on:** `FE4.2`, `FE3.1`

### FE5.2 — Swift contract bridge
- **Done:** Schema→Swift DTO codegen (or documented hand-mirror) + golden JSON
  fixtures the Swift decoders are tested against.
- **Depends on:** `FE4.2`

### FE5.3 — Swift client skeleton *(exit gate)*
- **Done:** a minimal Swift RPC client connects to the stub and round-trips one
  `snapshot` query and one live stream. **Divergence point reached.**
- **Depends on:** `FE5.1`, `FE5.2`
