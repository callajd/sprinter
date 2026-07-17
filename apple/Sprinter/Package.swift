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
    .library(name: "SprinterCore", targets: ["SprinterCore"])
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
  ]
)
