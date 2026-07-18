/**
 * `PiSpawnRouter` coverage (CE1.2 / CE1.1-F2) — the per-Job spawn-config routing
 * port and its two adapters, exercised offline:
 *
 *   - {@link layerInheritCwd} — the no-op default: every Job inherits the parent
 *     cwd (an empty {@link PiProcessConfig});
 *   - {@link layerWorktreeRouter} — a per-Job `<baseDir>/<job.id>` worktree, created
 *     on demand against a REAL scoped temp directory (`@effect/platform-bun`
 *     `BunFileSystem`/`BunPath`, Bun-native — no bare `node:*`).
 *
 * Deterministic and offline: no `pi` process, no daemon cwd assumptions.
 */
import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { FileSystem } from "effect/FileSystem";
import { BunFileSystem, BunPath } from "@effect/platform-bun";
import { expect } from "vitest";
import { Job } from "@sprinter/domain";
import { layerInheritCwd, layerWorktreeRouter, PiSpawnRouter } from "./index.ts";

const makeJob = (id: string) =>
  Schema.decodeUnknownEffect(Job)({
    id,
    issueId: "issue-1",
    kind: "implement",
    status: "queued",
  }).pipe(Effect.orDie);

it.effect("layerInheritCwd routes every Job to an empty config (parent cwd)", () =>
  Effect.gen(function* () {
    const router = yield* PiSpawnRouter;
    const job = yield* makeJob("job-1");
    const config = yield* router.configFor(job);
    expect(config).toEqual({});
  }).pipe(Effect.provide(layerInheritCwd)),
);

it.effect("layerWorktreeRouter routes each Job to its own created <baseDir>/<job.id> cwd", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-worktree-" });

    yield* Effect.gen(function* () {
      const router = yield* PiSpawnRouter;
      const jobA = yield* makeJob("job-a");
      const jobB = yield* makeJob("job-b");

      const configA = yield* router.configFor(jobA);
      const configB = yield* router.configFor(jobB);

      // Distinct, per-Job cwds under the base — so concurrent Jobs never collide.
      expect(configA.cwd).toBe(`${baseDir}/job-a`);
      expect(configB.cwd).toBe(`${baseDir}/job-b`);
      expect(configA.cwd).not.toBe(configB.cwd);

      // The worktree directory is created on demand (idempotent: a second resolve
      // of the same Job succeeds against the existing directory).
      expect(yield* fs.exists(`${baseDir}/job-a`)).toBe(true);
      const again = yield* router.configFor(jobA);
      expect(again.cwd).toBe(`${baseDir}/job-a`);
    }).pipe(Effect.provide(layerWorktreeRouter(baseDir)));
  }).pipe(Effect.provide(BunFileSystem.layer), Effect.provide(BunPath.layer)),
);
