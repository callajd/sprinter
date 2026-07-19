// swift-tools-version: 6.0
import PackageDescription

// Swift-side scaffold for Sprinter (issue FE1.2). The `make check` gate — the
// Swift analog of `bun run check` (policy.md §"SwiftUI / Swift side") — is wired
// in `scripts/check.sh`.
//
// Language rules (INV-NOCAST / policy): Swift 6 language mode implies *complete*
// strict concurrency; `-strict-concurrency=complete` is stated explicitly and
// `-warnings-as-errors` treats every warning as a hard failure so the build step
// of the gate cannot go green with an outstanding diagnostic.
let swiftSettings: [SwiftSetting] = [
  .swiftLanguageMode(.v6),
  .unsafeFlags([
    "-strict-concurrency=complete",
    "-warnings-as-errors",
  ])
]

let package = Package(
  name: "Sprinter",
  platforms: [.macOS(.v14)],
  products: [
    .library(name: "SprinterCore", targets: ["SprinterCore"]),
    // The platform-neutral RPC-contract mirror (FE2.4). No AppKit/UIKit — client
    // shells (macOS, later iOS) consume it as a plain SwiftPM library.
    .library(name: "SprinterContract", targets: ["SprinterContract"]),
    // The Swift RPC client + `Backend` port (BE1.1): the transport-generic
    // envelope framing over the frozen `SprinterContract` DTOs, behind the
    // `Backend` seam feature code depends on (INV-PORT / INV-CONTRACT).
    .library(name: "SprinterBackend", targets: ["SprinterBackend"]),
    // Mission Control feature logic (BE2.1): the board + activity view model,
    // projected from `SprinterBackend`'s port-based `WorkGraphResync` feed.
    // Platform-neutral (Foundation + Observation, no AppKit/UIKit, D10) so the
    // logic is verified by `make check`; the SwiftUI `View` + `.app` shell is
    // convergence, not this epic.
    .library(name: "SprinterMissionControl", targets: ["SprinterMissionControl"]),
    // Interactive-session feature logic (BE3.1): the one reusable session view
    // model (D9), projecting BE1's port-based `InteractiveSession` feed into a
    // view-facing transcript with input/interrupt + inline `extension_ui_request`
    // handling. Platform-neutral (Foundation + Observation, no AppKit/UIKit, D10)
    // so the logic is verified by `make check`; the SwiftUI `View` + `.app` shell
    // is convergence, not this epic.
    .library(name: "SprinterSession", targets: ["SprinterSession"]),
    // Inspector feature logic (BE4.1): the transcript↔PR view model, pairing the
    // reused `SprinterSession` transcript with the PR the session produced (resolved
    // over the `Snapshot` and kept live off BE1's `WorkGraphResync` feed).
    // Platform-neutral (Foundation + Observation, no AppKit/UIKit, D10) so the logic
    // is verified by `make check`; the SwiftUI `View` + `.app` shell is convergence,
    // not this epic.
    .library(name: "SprinterInspector", targets: ["SprinterInspector"]),
  ],
  dependencies: [
    // SwiftLint pinned via SPM plugin so the linter version is locked in
    // `Package.resolved` (INV-PIN) rather than relying on a global install.
    .package(url: "https://github.com/SimplyDanny/SwiftLintPlugins", exact: "0.65.0")
  ],
  targets: [
    .target(
      name: "SprinterCore",
      swiftSettings: swiftSettings
    ),
    .testTarget(
      name: "SprinterCoreTests",
      dependencies: ["SprinterCore"],
      swiftSettings: swiftSettings
    ),
    // The RPC-contract mirror — hand-written `Codable` DTOs, platform-neutral
    // (Foundation only, no AppKit/UIKit). Foreign consumer of the TS contract
    // (D10): it decodes the SAME wire bytes the contract emits (INV-CONTRACT).
    .target(
      name: "SprinterContract",
      swiftSettings: swiftSettings
    ),
    // Decode tests over committed goldens generated FROM the TS contract
    // (`scripts/generate-goldens.ts`). The gate only DECODES the goldens — no bun
    // dependency inside `make check`.
    .testTarget(
      name: "SprinterContractTests",
      dependencies: ["SprinterContract"],
      resources: [.copy("Goldens")],
      swiftSettings: swiftSettings
    ),
    // The RPC client + `Backend` port (BE1.1). Speaks the `effect/unstable/rpc`
    // envelope (NDJSON-framed) over an INJECTED transport; reuses the frozen
    // `SprinterContract` message DTOs and adds only the transport envelope.
    // Foundation only, platform-neutral (no new SPM deps, INV-PIN).
    .target(
      name: "SprinterBackend",
      dependencies: ["SprinterContract"],
      swiftSettings: swiftSettings
    ),
    // Envelope + framing + client + port tested against a FAKE in-memory
    // transport — deterministic and offline, no live daemon/network in the gate.
    .testTarget(
      name: "SprinterBackendTests",
      dependencies: ["SprinterBackend"],
      swiftSettings: swiftSettings
    ),
    // Mission Control board + activity view model (BE2.1). Projects the
    // `Workstream ⊃ Epic ⊃ Issue` hierarchy from the port-based `WorkGraphResync`
    // feed; consumes the mirrored `SprinterContract` DTOs, never a transport
    // (INV-PORT / INV-CONTRACT).
    .target(
      name: "SprinterMissionControl",
      dependencies: ["SprinterBackend", "SprinterContract"],
      swiftSettings: swiftSettings
    ),
    // Board projection + view model tested against a FAKE scripted `Backend`
    // driven through a real `WorkGraphResync` — deterministic and offline, no
    // live daemon/network in the gate.
    .testTarget(
      name: "SprinterMissionControlTests",
      dependencies: ["SprinterMissionControl"],
      swiftSettings: swiftSettings
    ),
    // Interactive-session view model (BE3.1). Projects BE1's `InteractiveSession`
    // feed into a view-facing transcript and drives input/interrupt + the inline
    // `extension_ui_request` round-trip; consumes the mirrored `SprinterContract`
    // DTOs over the `Backend` port, never a transport (INV-PORT / INV-CONTRACT).
    .target(
      name: "SprinterSession",
      dependencies: ["SprinterBackend", "SprinterContract"],
      swiftSettings: swiftSettings
    ),
    // Transcript projection + view model tested against a FAKE scripted `Backend`
    // driving a real `InteractiveSession` — deterministic and offline, no live
    // daemon/network in the gate.
    .testTarget(
      name: "SprinterSessionTests",
      dependencies: ["SprinterSession"],
      swiftSettings: swiftSettings
    ),
    // Inspector view model (BE4.1). Pairs the REUSED `SprinterSession` transcript
    // (incl. the diff transform over an edit/write tool call's `JSONValue` payload)
    // with the session→PR resolver over the `Snapshot`, kept live off BE1's
    // `WorkGraphResync` feed; consumes the mirrored `SprinterContract` DTOs over the
    // `Backend` port, never a transport (INV-PORT / INV-CONTRACT).
    .target(
      name: "SprinterInspector",
      dependencies: ["SprinterSession", "SprinterBackend", "SprinterContract"],
      swiftSettings: swiftSettings
    ),
    // Diff transform + session→PR resolver + view model tested against a FAKE
    // scripted `Backend` driving a real session feed AND a real `WorkGraphResync` —
    // deterministic and offline, no live daemon/network in the gate.
    .testTarget(
      name: "SprinterInspectorTests",
      dependencies: ["SprinterInspector"],
      swiftSettings: swiftSettings
    ),
    // App composition layer (CE3.1): daemon-endpoint resolution and the live-backend
    // wiring (`BackendConnector` → `WorkGraphResync`) plus the top-level `AppModel`
    // the SwiftUI shell renders. Platform-neutral (Foundation + Observation, no
    // AppKit/UIKit, no `#if os(...)`, D10 / INV-PORT), so this — the testable non-View
    // glue — is verified by `make check` (INV-COV); the thin SwiftUI Views live in the
    // executable target, the one platform edge.
    .target(
      name: "SprinterAppSupport",
      dependencies: [
        "SprinterBackend", "SprinterContract", "SprinterMissionControl",
      ],
      swiftSettings: swiftSettings
    ),
    // Endpoint resolution + `AppModel` lifecycle tested against a FAKE `Backend`
    // (a fake `DaemonTransportProvider` for the live connect seam) — deterministic
    // and offline, no live daemon/network/socket in the gate.
    .testTarget(
      name: "SprinterAppSupportTests",
      dependencies: ["SprinterAppSupport"],
      swiftSettings: swiftSettings
    ),
    // The running macOS app (CE3.1): the `@main` SwiftUI `App` + the thin feature
    // Views rendering the existing view models. The `#if os(...)` shell + any
    // AppKit/UIKit glue lives HERE ONLY (D10 / INV-PORT) — the ONE platform edge;
    // the feature libraries stay platform-neutral. Views hold no logic (it all lives
    // in the already-tested view models), so this target is the coverage-exempt
    // platform edge (app entry point + pure view layout; naturally absent from the
    // coverage report since no test target links it — see scripts/check.sh).
    .executableTarget(
      name: "Sprinter",
      dependencies: [
        "SprinterAppSupport", "SprinterBackend", "SprinterContract",
        "SprinterMissionControl", "SprinterSession", "SprinterInspector",
      ],
      swiftSettings: swiftSettings
    ),
  ]
)
