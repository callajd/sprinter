# Sprinter — Implementation Plan

Split into **three self-contained parts**:

1. **[Foundation](./foundation.md)** — prerequisites: repo scaffold, domain
   schemas, the frozen **RPC contract**, the **stub server**, and the Swift
   bridge. Unblocks both tracks; this is their entire dependency surface.
2. **[Track A — Daemon / execution](./track-a-daemon.md)** — the real daemon
   built behind the contract.
3. **[Track B — SwiftUI app](./track-b-ui.md)** — the client built against the
   contract.

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
