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
  ]
)
