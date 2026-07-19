#!/usr/bin/env python3
"""Coverage gate for the Sprinter Swift package (INV-COV).

Parses the llvm-cov JSON produced by `swift test --enable-code-coverage`
(its path comes from `swift test --show-codecov-path`) and fails if aggregate
line OR function coverage over the real product sources (files under
`/Sources/`, excluding the test target) falls below the threshold.

Aggregating counts over the product sources — rather than trusting llvm's
top-level totals, which fold in the test target's own always-executed code —
keeps the gate honest about the code that ships.

It also cross-checks every shipped `Sources/**/*.swift` against the files that
appear in the report and FAILS on any that are absent: llvm-cov only reports
files compiled into the instrumented test binary, so a product target that no
test target links would otherwise contribute invisible 0% coverage and slip the
gate green (matters once the package grows past this single module).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCES_DIR = Path(__file__).resolve().parent.parent / "Sources"

# Shipped sources exempt from the presence requirement, each needing a written
# justification (policy.md §Coverage: exclusions must be explicitly justified —
# generated code, app entry points, pure view layout with no logic). Add a
# `/Sources/...`-suffixed path with a comment saying why.
EXEMPT_SOURCES: set[str] = {
    # Declaration-only files: a `protocol` (and a plain error `enum`) carry no
    # executable statements, so llvm-cov emits no coverage record for them and
    # they can never appear in the report. Their conformers/values ARE exercised
    # (RpcBackend, the fake transport, BackendError assertions) in the files that
    # do report; the seams themselves have no logic to cover.
    "/Sources/SprinterBackend/RpcTransport.swift",
    "/Sources/SprinterBackend/Backend.swift",
}

# Whole-directory exemptions (policy.md §Coverage: app entry points + pure view layout
# with no logic). Each prefix needs a written justification.
EXEMPT_PREFIXES: tuple[str, ...] = (
    # The executable target (CE3.1): the `@main` SwiftUI `App`, the `#if os(...)` /
    # AppKit platform shell, and the thin feature Views. It is the D10 platform edge —
    # all logic lives in the (already-tested) view models and the covered
    # `SprinterAppSupport` composition layer, and the Views hold none. SwiftUI View
    # layout is not linked into any test target and is impractical to unit-test, so the
    # whole target is exempt from the presence requirement (INV-COV covers the real
    # logic modules, which stay >= 75%). Non-View glue lives in SprinterAppSupport,
    # which is NOT exempt and is tested.
    "/Sources/Sprinter/",
)


def is_exempt(path: str) -> bool:
    """Whether a shipped source is exempt from the presence requirement."""
    return path in EXEMPT_SOURCES or path.startswith(EXEMPT_PREFIXES)


def sources_on_disk() -> set[str]:
    """Canonical `/Sources/...` suffix for every shipped Swift source on disk."""
    return {f"/Sources/{p.relative_to(SOURCES_DIR).as_posix()}" for p in SOURCES_DIR.rglob("*.swift")}


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: coverage-gate.py <codecov.json> <min-percent>", file=sys.stderr)
        return 2

    codecov_path = sys.argv[1]
    threshold = float(sys.argv[2])

    with open(codecov_path, encoding="utf-8") as handle:
        report = json.load(handle)

    line_count = line_covered = 0
    func_count = func_covered = 0
    reported: set[str] = set()

    for export in report.get("data", []):
        for file_entry in export.get("files", []):
            filename = file_entry.get("filename", "")
            marker = filename.find("/Sources/")
            if marker == -1:
                continue
            reported.add(filename[marker:])
            summary = file_entry["summary"]
            line_count += summary["lines"]["count"]
            line_covered += summary["lines"]["covered"]
            func_count += summary["functions"]["count"]
            func_covered += summary["functions"]["covered"]

    # Every shipped source must appear in the report; an absent one is un-linked /
    # un-exercised and its 0% coverage would otherwise be invisible to the aggregate.
    missing = sorted(path for path in sources_on_disk() - reported if not is_exempt(path))
    if missing:
        print("coverage gate: shipped source(s) absent from report (no test exercises them):", file=sys.stderr)
        for path in missing:
            print(f"  {path}", file=sys.stderr)
        return 1

    if not reported:
        print("coverage gate: no files under /Sources/ found in report", file=sys.stderr)
        return 1

    line_pct = 100.0 * line_covered / line_count if line_count else 100.0
    func_pct = 100.0 * func_covered / func_count if func_count else 100.0

    print(
        f"coverage: line {line_pct:.2f}% ({line_covered}/{line_count}), "
        f"function {func_pct:.2f}% ({func_covered}/{func_count}) "
        f"over {len(reported)} source file(s)"
    )

    failed = False
    if line_pct < threshold:
        print(f"FAIL: line coverage {line_pct:.2f}% < {threshold:.0f}%", file=sys.stderr)
        failed = True
    if func_pct < threshold:
        print(f"FAIL: function coverage {func_pct:.2f}% < {threshold:.0f}%", file=sys.stderr)
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
