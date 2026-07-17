# Workstream `FDN` — Foundation

> Implementation spec: an epic set + sequencing graph + cross-cutting invariants;
> one issue per **task**, each with acceptance (**Done**) and dependency edges.
>
> **Goal:** the shared dependency surface for Track A and Track B — scaffold +
> check gate + CI, and the contract stack (domain schemas, the owned Pi protocol
> schema, the frozen RPC contract, the Swift bridge).
>
> **Prerequisites:** none. This workstream unblocks `TRK-A` and `TRK-B`.
> **Exit / divergence point:** contract v1 is frozen and the Swift bridge decodes
> it (`FE2.4`). After that, the tracks proceed against the contract.

## Cross-cutting invariants

Bind every epic; each task's **Done** must satisfy the applicable ones. Sourced
from [`policy.md`](../policy.md), [`conventions.md`](../conventions.md),
[`decisions.md`](../decisions.md).

| Id | Invariant | Guard |
|----|-----------|-------|
| `INV-GATE` | The side's `check` is green — format + lint + typecheck + tests | `bun run check` / `make check` + CI |
| `INV-COV` | ≥ 75% line **and** function coverage on the task's modules | coverage threshold inside `check` |
| `INV-NOCAST` | No `as`/non-null `!`/`any` (TS); no force unwrap/cast/try (Swift) | `oxlint` / `SwiftLint --strict` |
| `INV-PIN` | All deps exact-pinned; lockfile committed | `check` dep-pin verification + committed lock |
| `INV-NAMING` | Follows `conventions.md` (ports/adapters, owned vs. foreign, `*Store`/`*Ops`) | review |
| `INV-PORT` | No concrete backing/instance depended on directly; external systems + locations sit behind ports | review |
| `INV-CONTRACT` | Contract changes are versioned and ripple to the Swift mirror; decode tests pass | `check` + review |

`INV-GATE`/`INV-COV`/`INV-NOCAST`/`INV-PIN`/`INV-NAMING` apply to every task.

## Epics — set & sequencing

| Epic | Name | Depends on |
|------|------|-----------|
| `FE1` | Toolchain, check gate & CI | — |
| `FE2` | Contract stack | `FE1` |

```
FE1 ──► FE2
```

---

## Epic `FE1` — Toolchain, check gate & CI

Implements [`policy.md`](../policy.md). The `check` gate built here **is** the
project verification gate.

### FE1.1 — Bun scaffold + check gate
- **Done:** Bun workspace (`packages/{domain,contract,daemon}`), `.bun-version` +
  `packageManager` pinned, strict `tsconfig`, Effect `4.0.0-beta.97` exact,
  `save-exact`, `bun.lock` committed; `oxlint` (bans `as`/`!`/`any`) + `oxfmt
  --check` + `tsc --noEmit` + `bun test --coverage` (`line=0.75, function=0.75`)
  wired behind **`bun run check`**; tooling pinned exact.
- **Depends on:** —

### FE1.2 — Swift scaffold + `make check`
- **Done:** `apple/Sprinter/` project, Swift 6 + strict concurrency = complete +
  warnings-as-errors; `SwiftLint --strict` (force unwrap/cast/try = error) +
  `swift-format` + coverage gate (≥75%) behind **`make check`**; deps
  `.exact()`-pinned, `Package.resolved` committed.
- **Depends on:** —

### FE1.3 — CI
- **Done:** GitHub Actions running `bun run check` (Ubuntu) and `make check`
  (macOS, pinned Xcode) on push to `main` and on PRs; red `check` blocks.
- **Depends on:** `FE1.1`, `FE1.2`

---

## Epic `FE2` — Contract stack  ·  tags: `INV-NAMING`, `INV-PORT`, `INV-CONTRACT`

`packages/domain` + `packages/contract` + the Swift bridge. `FE2.4` is the
divergence gate.

### FE2.1 — Domain schemas
- **Done:** owned Effect `Schema` for the read model (`Workstream`/`Epic`/`Issue`/
  `Job`/`Session` + statuses + IDs + the workstream→epic→Issue→PR mapping), the
  rich `SessionEvent` (transcript-grade; plainly named, not `AgentSessionEvent`),
  and command + session-I/O payloads (`SessionInput`, `UiResponse`); round-trip
  tested.
- **Depends on:** —

### FE2.2 — Owned Pi protocol schema
- **Done:** owned `Schema` mirroring the `RpcCommand`/`RpcResponse`/
  `AgentSessionEvent` subset we use (authored against Pi's `rpc-types.ts`, **not**
  imported), validated against real `pi --mode rpc` output.
- **Depends on:** —

### FE2.3 — RPC contract v1
- **Done:** the versioned `RpcGroup` (`effect/unstable/rpc`) over the domain
  schemas: `snapshot`, `events` (streaming), commands, and the session channel
  (`sessionEvents` streaming `SessionEvent`, `sessionSend`, `interrupt`,
  `answerUiRequest`); compiles and is exported.
- **Depends on:** `FE2.1`

### FE2.4 — Swift contract bridge *(divergence gate)*
- **Done:** Schema→Swift DTO codegen (or documented hand-mirror) for the contract,
  with decode tests over sample contract messages. **Contract frozen + mirrorable
  → divergence point reached.**
- **Depends on:** `FE2.3`
