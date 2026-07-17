import { defineConfig } from "vitest/config";

/**
 * Test + coverage gate for the Bun/TypeScript side (INV-GATE, INV-COV).
 *
 * Tests are `@effect/vitest` and run under Bun. Coverage uses the **istanbul**
 * provider (source instrumentation): Bun does not implement Node's V8 inspector
 * coverage protocol, so `@vitest/coverage-v8` cannot run here. `all: true` counts
 * every source module even when no test imports it, and `perFile: true` enforces
 * the threshold per module — so an untested or weak module fails the gate, not
 * just the aggregate (policy.md §Coverage).
 */
export default defineConfig({
  test: {
    include: ["packages/*/src/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      reporter: ["text", "lcov"],
      reportsDirectory: "coverage",
      include: ["packages/*/src/**/*.ts"],
      exclude: ["packages/*/src/**/*.test.ts"],
      all: true,
      thresholds: {
        perFile: true,
        lines: 75,
        functions: 75,
      },
    },
  },
});
