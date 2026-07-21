# Sprinter — Swift client

The native macOS/SwiftUI client (decision **D10**: a *foreign consumer* of the
RPC contract; it mirrors the message schemas in Swift and cannot share Effect
types). The package carries the whole client — the contract mirror, the RPC
client and `Backend` port, the feature view models, the app composition layer,
and the SwiftUI executable — with everything except the executable kept
platform-neutral so `make check` verifies it.

## Layout

`Package.swift` is the source of truth for the target graph; the summary below
tracks it.

**Source targets** (`Sources/`):

- `SprinterContract` — the **RPC contract mirror** (FE2.4): hand-written
  `Codable` DTOs mirroring the contract, platform-neutral (Foundation only, no
  AppKit/UIKit). It is a foreign consumer (D10) that decodes the SAME wire bytes
  the TypeScript contract emits (INV-CONTRACT). See `docs/contract-mirror.md`.
- `SprinterBackend` — the Swift RPC client + `Backend` port (BE1.1): the
  transport-generic `effect/unstable/rpc` envelope (NDJSON-framed) over an
  INJECTED transport, behind the seam feature code depends on (INV-PORT).
- `SprinterMissionControl` — Mission Control board + activity view model (BE2.1),
  projecting the `Workstream ⊃ Epic ⊃ Issue` hierarchy from the port-based
  `WorkGraphResync` feed.
- `SprinterSession` — the one reusable interactive-session view model (BE3.1, D9):
  the view-facing transcript with input/interrupt and inline
  `extension_ui_request` handling.
- `SprinterInspector` — the transcript↔PR view model (BE4.1): the reused
  `SprinterSession` transcript (incl. the diff transform) paired with the PR the
  session produced, resolved over the `Snapshot`.
- `SprinterAppSupport` — the app composition layer (CE3.1): daemon-endpoint
  resolution, live-backend wiring (`BackendConnector` → `WorkGraphResync`), and
  the top-level `AppModel` the SwiftUI shell renders.
- `Sprinter` — the executable: the `@main` SwiftUI `App` and the thin feature
  Views. The ONE platform edge — `#if os(...)` and any AppKit/UIKit glue live
  here only, and the Views hold no logic, so this target is the coverage-exempt
  edge (no test target links it; see `scripts/check.sh`).

**Test targets** (`Tests/`): one per source target except the executable —
`SprinterContractTests`, `SprinterBackendTests`, `SprinterMissionControlTests`,
`SprinterSessionTests`, `SprinterInspectorTests`, `SprinterAppSupportTests`. All
run offline against FAKE in-memory transports / scripted `Backend`s — no live
daemon, network, or socket in the gate. `SprinterContractTests` additionally
decodes `Goldens/` — JSON generated FROM the TS contract by
`scripts/generate-goldens.ts`; the gate only DECODES the committed goldens (no
`bun` inside `make check`).

## Verification gate — `make check`

The Swift analog of `bun run check` (`docs/policy.md` §"SwiftUI / Swift side").
`swift-testing` (`Testing`) is the suite framework across every test target.
Ordered stages, non-zero exit on any violation (this is what CI runs, FE1.3):

1. `swift format lint --strict` — Apple formatter, toolchain-pinned (Xcode 26.6).
2. **SwiftLint `--strict`** — the force-unwrap/cast/try family is banned as an
   **error** (`.swiftlint.yml`).
3. `swift build` — Swift 6 language mode, strict concurrency = complete,
   `-warnings-as-errors`.
4. `swift test --enable-code-coverage` — `swift-testing`.
5. Coverage gate — `scripts/check.sh` reads the `llvm-cov` (LLVM, toolchain) TOTAL
   and fails under **75%** line **and** function over `Sources/` (tests + deps excluded).

```sh
make check
```

## Tooling & pinning (INV-PIN)

- **Toolchain:** Swift 6.3.3 / Xcode 26.6 (pins `swift format`).
- **SwiftLint:** pinned via the **SwiftLintPlugins SPM plugin**, `exact("0.65.0")`,
  version-locked in `Package.resolved` (a single reproducible pinning story rather
  than a global Homebrew install). The gate invokes it as
  `swift package --allow-writing-to-package-directory swiftlint --strict`.
- All external dependencies are `.exact()`-pinned in `Package.swift`;
  `Package.resolved` is committed.
