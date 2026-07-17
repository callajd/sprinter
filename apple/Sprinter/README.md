# Sprinter — Swift client

Native macOS/SwiftUI client scaffold (decision **D10**: a *foreign consumer* of
the RPC contract; it mirrors the message schemas in Swift and cannot share Effect
types). `FE2.4` builds the real contract bridge — this package is intentionally
minimal, existing so the verification gate runs against real code.

## Layout

- `Sources/SprinterCore/` — owned domain seed. Owned types get **plain** names
  (`Workstream`, `WorkStatus`), per `docs/conventions.md`.
- `Tests/SprinterCoreTests/` — `swift-testing` (`Testing`) suite.

## Verification gate — `make check`

The Swift analog of `bun run check` (`docs/policy.md` §"SwiftUI / Swift side").
Ordered stages, non-zero exit on any violation (this is what CI runs, FE1.3):

1. `swift format lint --strict` — Apple formatter, toolchain-pinned (Xcode 26.6).
2. **SwiftLint `--strict`** — the force-unwrap/cast/try family is banned as an
   **error** (`.swiftlint.yml`).
3. `swift build` — Swift 6 language mode, strict concurrency = complete,
   `-warnings-as-errors`.
4. `swift test --enable-code-coverage` — `swift-testing`.
5. Coverage gate — `scripts/coverage-gate.py` parses the `llvm-cov` JSON and
   fails under **75%** line **and** function over `Sources/`.

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
