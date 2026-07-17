#!/usr/bin/env python3
"""Coverage gate for the Sprinter Swift package (INV-COV).

Parses the llvm-cov JSON produced by `swift test --enable-code-coverage`
(its path comes from `swift test --show-codecov-path`) and fails if aggregate
line OR function coverage over the real product sources (files under
`/Sources/`, excluding the test target) falls below the threshold.

Aggregating counts over the product sources — rather than trusting llvm's
top-level totals, which fold in the test target's own always-executed code —
keeps the gate honest about the code that ships.
"""

from __future__ import annotations

import json
import sys


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
    counted_files = 0

    for export in report.get("data", []):
        for file_entry in export.get("files", []):
            filename = file_entry.get("filename", "")
            if "/Sources/" not in filename:
                continue
            counted_files += 1
            summary = file_entry["summary"]
            line_count += summary["lines"]["count"]
            line_covered += summary["lines"]["covered"]
            func_count += summary["functions"]["count"]
            func_covered += summary["functions"]["covered"]

    if counted_files == 0:
        print("coverage gate: no files under /Sources/ found in report", file=sys.stderr)
        return 1

    line_pct = 100.0 * line_covered / line_count if line_count else 100.0
    func_pct = 100.0 * func_covered / func_count if func_count else 100.0

    print(
        f"coverage: line {line_pct:.2f}% ({line_covered}/{line_count}), "
        f"function {func_pct:.2f}% ({func_covered}/{func_count}) "
        f"over {counted_files} source file(s)"
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
