# Sprinter — Implementation Plan

Three self-contained parts, **each a spec doc for the `workstream` skill** (an
epic set + sequencing graph + cross-cutting invariants). Run each with
`/workstream <file>`; `/epic` cuts one issue per task and threads the invariants
into acceptance.

1. **[Foundation](./foundation.md)** — workstream `FDN`: scaffold + check gate +
   CI, domain schemas, the frozen **RPC contract**, the owned Pi protocol schema,
   the **stub server**, the Swift bridge. Unblocks both tracks; their entire
   dependency surface.
2. **[Track A — Daemon / execution](./track-a-daemon.md)** — workstream `TRK-A`:
   the real daemon behind the contract.
3. **[Track B — SwiftUI app](./track-b-ui.md)** — workstream `TRK-B`: the client
   built against the contract.

**Cross-workstream order:** `FDN` lands first (it is the prerequisite of both);
then `TRK-A` and `TRK-B` run in parallel. Cutover is the convergence milestone
below, not an epic of either track.

## Dependency rule (invariant)

> **Track A and Track B depend ONLY on the Foundation — never on each other.**

Their sole coupling is the frozen **RPC contract**, which is a *Foundation*
artifact. Track B builds entirely against the Foundation's **stub server**;
Track A replaces the stub's handlers with real ones. Because both target the same
contract, they re-converge with no big-bang integration.

```
        FOUNDATION  (blocks both; the only shared dependency)
   scaffold ─► domain schemas ─► RPC contract v1 ─► stub server ──┐
                    │                                 Swift bridge┘
                    ▼
        ══════════ DIVERGENCE POINT ══════════
         │                                   │
     Track A (daemon)                    Track B (SwiftUI)
     real handlers behind contract       UI against the stub, then real
         │                                   │
         └──────────► CONVERGENCE ◄──────────┘
              app on real daemon → cutover
```

## Cross-track discipline

After divergence the tracks are coupled *only* through the contract:

- **Contract changes are events, not edits.** A change to the contract ripples to
  three places — the Swift mirror, the stub, and Track A's handlers. Version the
  contract; batch changes; announce them across tracks.
- **The stub is a maintained Foundation artifact**, not throwaway — it stays the
  fast local target for Track B and the basis of contract tests.
- **Both tracks test against the contract** (golden fixtures), never against each
  other's internals — integration is continuous.

## Convergence → cutover

Point the app at the real daemon (all Track A handlers real). Cutover = drive a
real Sprinter Issue → open PR **through the SwiftUI app**, observed live in
Mission Control and paired in the Inspector, restart-safe.
