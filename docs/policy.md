# Sprinter — Engineering Policy

Binding repository standards. **CI enforces all of it on every push to `main` and
on every PR.** Items marked ⚠️ have a maturity/verification caveat; items marked
*(confirm)* are choices made on the delegated Swift side and open to revision.

## Coverage

- **≥ 75% line coverage and ≥ 75% function coverage** from unit tests, on all
  modules, **both sides**. A drop below threshold fails CI.
- Candidate exclusions (must be explicitly justified in config): generated code
  (schema/codegen output), app entry points, and pure view layout with no logic.
  *(confirm the exclusion list)*
- The global gate is enforced by the test runner; if **per-module** enforcement is
  required (not just aggregate), a post-check parses per-file coverage and fails on
  any module under threshold.

## The `check` command (both sides)

Each side exposes **one** command that runs *format-check + lint + typecheck +
tests (+coverage)* and exits non-zero on any violation. This is exactly what CI
runs, and the gate agents must pass post-cutover.

- **Bun:** `bun run check`
- **SwiftUI:** `make check` (wraps `scripts/check.sh`)

## Bun / TypeScript side

- **Runtime & build:** Bun, version-pinned (`.bun-version` + `packageManager`). Bun
  runs/transpiles TS directly — no `tsc` emit step.
- **Typecheck:** **TypeScript 7** (`tsc --noEmit`), pinned exact. Bun handles
  transpilation, so this is typecheck-only.
- **Lint:** **`oxlint`** (latest, pinned exact) at maximum strictness. **Type
  assertions and unsafe escapes are banned as errors:** `as` assertions
  (`consistent-type-assertions: never`), non-null `!` (`no-non-null-assertion`),
  `any` (`no-explicit-any`), and `@ts-ignore`/`@ts-expect-error` without cause.
  ⚠️ oxlint is primarily syntactic; deep *type-aware* rules are limited — but the
  assertion bans above are syntactic and fully covered.
- **Format:** **`oxfmt`** (oxc formatter, latest pinned); `oxfmt --check` in CI.
  ⚠️ verify `oxfmt` is release-ready; **fallback: Biome** (`biome format`) if not.
- **Tests + coverage:** `bun test --coverage`; threshold in `bunfig.toml`
  (`[test] coverageThreshold = { line = 0.75, function = 0.75 }`).
- **`bun run check`** = `oxfmt --check` → `oxlint` → `tsc --noEmit` →
  `bun test --coverage`.

## SwiftUI / Swift side *(recommended analogs — confirm)*

- **Language:** Swift 6 language mode, **strict concurrency = complete**, warnings
  treated as errors (`-warnings-as-errors`).
- **Lint:** **SwiftLint** (`--strict`, pinned). The force-unwrap/cast family — the
  Swift analog of type assertions — is banned as **errors**: `force_cast` (`as!`),
  `force_unwrapping` (`!`), `force_try` (`try!`), `implicitly_unwrapped_optional`.
- **Format:** **swift-format** (Apple, toolchain-pinned); `swift-format lint
  --strict` in CI.
- **Tests + coverage:** `swift test --enable-code-coverage`; a script parses
  `llvm-cov` output and fails under **75% line & function**.
- **`make check`** = swift-format lint → SwiftLint `--strict` → build
  (warnings-as-errors) → `swift test` → coverage gate.

## Dependency pinning

- **All external dependencies pinned to exact versions — no `^`/`~`.**
  - Bun: exact versions in `package.json`, `save-exact`, commit `bun.lock`.
  - Swift: `.exact("x.y.z")` in `Package.swift`, commit `Package.resolved`.
- **Tooling is pinned too:** `oxlint` / `oxfmt` / `typescript`; `SwiftLint` /
  `swift-format`.
- **Toolchains pinned:** `.bun-version`; the Xcode/Swift toolchain version pinned
  in CI.

## CI/CD

- **GitHub Actions**, triggered on **push to `main`** and on **PRs**.
- **Bun job** (Ubuntu runner): `bun run check`.
- **Swift job** (macOS runner, pinned Xcode): `make check`.
- Both must pass; the coverage gates run inside each `check`. A red `check` blocks.
- Implemented as part of Foundation **F0.1** — the `check` gate *is* the F0.1
  verification gate.
