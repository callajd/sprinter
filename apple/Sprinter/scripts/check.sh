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
#   5. coverage gate                (>= 75% line AND function on Sources/, from llvm-cov;
#                                    PLUS a presence cross-check: every shipped source
#                                    must appear in the report or be explicitly exempt)
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
# The coverage NUMBERS are computed by llvm-cov (LLVM, ships with the Swift
# toolchain via `xcrun`) — no third-party coverage tooling. This stage only enforces
# the >= MIN policy on its TOTAL. Stage 4 already produced the profdata + instrumented
# test binary; `--show-codecov-path` only PRINTS the data path (no re-run). Scope =
# product sources: the test target (/Tests/) and dependency/generated code (/.build/)
# are excluded; the app executable target is naturally absent (no test target links it).
covdir="$(dirname "$(swift test --enable-code-coverage --show-codecov-path)")"
xctest="$(/bin/ls -d "$(swift build --show-bin-path)"/*.xctest | head -1)"
testbin="${xctest}/Contents/MacOS/$(basename "${xctest}" .xctest)"
report="$(xcrun llvm-cov report "${testbin}" \
  -instr-profile="${covdir}/default.profdata" \
  -ignore-filename-regex='(/Tests/|/\.build/)')"
# The TOTAL row's coverage percentages, in llvm-cov's fixed column order:
# regions, functions, lines, branches. Take function (2nd) and line (3rd).
read -r func_pct line_pct < <(
  printf '%s\n' "${report}" |
    awk '/^TOTAL/ { n=0; for (i=1;i<=NF;i++) if ($i ~ /%$/) { n++; p[n]=$i+0 } print p[2], p[3] }'
)
printf 'coverage: line %.2f%%, function %.2f%% (min %s%%)\n' "${line_pct:-0}" "${func_pct:-0}" "${COVERAGE_MIN}"
awk -v l="${line_pct:-0}" -v f="${func_pct:-0}" -v m="${COVERAGE_MIN}" 'BEGIN {
  fail = 0
  if (l + 0 < m + 0) { printf "FAIL: line coverage %.2f%% < %s%%\n", l, m > "/dev/stderr"; fail = 1 }
  if (f + 0 < m + 0) { printf "FAIL: function coverage %.2f%% < %s%%\n", f, m > "/dev/stderr"; fail = 1 }
  exit fail
}'

# Presence cross-check (the real INV-COV guard, not the TOTAL): llvm-cov only reports
# files linked into the instrumented test binary, so a product target that NO test
# target links contributes invisible 0% coverage and would slip the % gate green. Fail
# if any shipped `Sources/**/*.swift` is absent from the report and not explicitly
# exempt. Exemptions (each justified, per policy.md §Coverage):
#   - `/Sources/Sprinter/`  — the executable target: `@main` App + `#if os(...)` platform
#     shell + thin Views (D10 edge). No logic (it lives in the tested view models /
#     SprinterAppSupport) and it links into no test target, so it never reports.
#   - RpcTransport.swift / Backend.swift — declaration-only `protocol`/error `enum`: no
#     executable statements, so llvm-cov emits no record; their conformers ARE covered.
# `llvm-cov export` carries ABSOLUTE filenames (unlike `report`, which strips the
# common path prefix and would break this cross-check); extract the `/Sources/...`
# suffix of each reported file. Same `-ignore-filename-regex` scope as the % gate.
reported="$(
  xcrun llvm-cov export -summary-only "${testbin}" \
    -instr-profile="${covdir}/default.profdata" \
    -ignore-filename-regex='(/Tests/|/\.build/)' |
    grep -oE '/Sources/[^"]*\.swift' | sort -u
)"
on_disk="$(find Sources -name '*.swift' | sed 's|^Sources|/Sources|' | sort -u)"
missing="$(
  comm -23 <(printf '%s\n' "${on_disk}") <(printf '%s\n' "${reported}") |
    grep -vE '^/Sources/Sprinter/' |
    grep -vxF \
      -e '/Sources/SprinterBackend/RpcTransport.swift' \
      -e '/Sources/SprinterBackend/Backend.swift' ||
    true
)"
if [ -n "${missing}" ]; then
  printf 'FAIL: shipped source(s) absent from coverage report (no test exercises/links them):\n' >&2
  printf '  %s\n' ${missing} >&2
  exit 1
fi

echo "==> make check: PASS"
