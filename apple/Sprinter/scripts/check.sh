#!/usr/bin/env bash
#
# Sprinter — Swift-side verification gate (issue FE1.2 / policy.md §"SwiftUI /
# Swift side"). The Swift analog of `bun run check`, and exactly what CI runs on
# the macOS runner (FE1.3). Ordered stages, non-zero exit on ANY violation:
#
#   1. swift format lint --strict   (Apple formatter, toolchain-pinned)
#   2. SwiftLint --strict           (SPM plugin, version-pinned in Package.resolved)
#   3. swift build                  (Swift 6 mode, strict concurrency, warnings-as-errors)
#   4. swift test --enable-code-coverage
#   5. coverage gate                (>= 75% line AND function on Sources/, from llvm-cov)
#
set -euo pipefail

# Run from the package root regardless of caller CWD.
cd "$(dirname "$0")/.."

COVERAGE_MIN="75"

echo "==> [1/5] swift format lint --strict"
swift format lint --strict --recursive Sources Tests

echo "==> [2/5] SwiftLint --strict (pinned SPM plugin)"
swift package --allow-writing-to-package-directory swiftlint --strict

echo "==> [3/5] swift build (warnings-as-errors, strict concurrency)"
swift build

echo "==> [4/5] swift test --enable-code-coverage"
swift test --enable-code-coverage

echo "==> [5/5] coverage gate (>= ${COVERAGE_MIN}% line AND function on Sources/)"
# `--show-codecov-path` only PRINTS the JSON path for the already-built coverage
# data from stage 4; it does not re-run the suite (the test binary is up to date).
codecov_path="$(swift test --enable-code-coverage --show-codecov-path)"
python3 scripts/coverage-gate.py "${codecov_path}" "${COVERAGE_MIN}"

echo "==> make check: PASS"
