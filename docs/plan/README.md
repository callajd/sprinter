# Sprinter — Implementation Plan

Three self-contained parts, each an implementation spec (epic set + sequencing
graph + cross-cutting invariants; one issue per task) suitable as `workstream`
input:

1. **[Foundation](./foundation.md)** — workstream `FDN`: scaffold + check gate +
   CI, and the contract stack (domain schemas, owned Pi protocol schema, the
   frozen **RPC contract**, the Swift bridge). Unblocks both tracks; their entire
   dependency surface.
2. **[Track A — Daemon / execution](./track-a-daemon.md)** — workstream `TRK-A`:
   the real daemon behind the contract.
3. **[Track B — SwiftUI app](./track-b-ui.md)** — workstream `TRK-B`: the client
   built against the contract.

## Dependency rule (invariant)

> **Track A and Track B depend ONLY on the Foundation — never on each other.**

Their sole coupling is the frozen **RPC contract**, a *Foundation* artifact. Both
build against it; because both target the same contract, they re-converge with no
big-bang integration.

```
        FOUNDATION  (the only shared dependency)
   scaffold ─► domain schemas ─► RPC contract ─► Swift bridge
                    │
                    ▼
        ══════════ DIVERGENCE POINT ══════════
         │                                   │
     Track A (daemon)                    Track B (SwiftUI)
     handlers behind the contract        UI against the contract
         │                                   │
         └──────────► CONVERGENCE ◄──────────┘
              app on real daemon → cutover
```

**Cross-workstream order:** `FDN` lands first (prerequisite of both); then `TRK-A`
and `TRK-B` run in parallel.

## Cross-track discipline

After divergence the tracks are coupled *only* through the contract:

- **Contract changes are events, not edits.** A change to the contract ripples to
  the Swift mirror and Track A's handlers. Batch contract changes.
- **Both tracks test against the contract**, never against each other's internals.

## Convergence → cutover

Point the app at the real daemon (Track A handlers real). Cutover = drive a real
Sprinter Issue → open PR **through the SwiftUI app**, observed live in Mission
Control and paired in the Inspector, restart-safe.
