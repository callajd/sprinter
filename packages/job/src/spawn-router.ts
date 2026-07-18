/**
 * `PiSpawnRouter` — the PORT that resolves, per {@link Job}, the process spawn
 * configuration (working directory / worktree) the `LocalPi` `ExecutionRunner`
 * adapter starts `pi` with (CVG task CE1.2, wiring-constraint CE1.1-F2).
 *
 * Why a port: a raw `run(job)` spawned `pi` in the DAEMON's own cwd, so every Job
 * ran in the same directory rather than its own worktree. The `Job` domain type
 * carries no worktree field (and inventing one would over-fit the model, D6), so
 * the cwd/worktree ROUTING lives here — a fakeable seam the composition root wires
 * through DI (INV-EFFECT-DI). Selecting "one worktree layout vs. another", or a
 * no-op default vs. a real per-job directory, is a {@link Layer} substitution and
 * nothing more.
 *
 * The port sits BELOW the `ExecutionRunner` tag: only the `LocalPi` adapter reads
 * it, so no consumer above `ExecutionRunner` gains any spawn/localness knowledge
 * (INV-PORT). It yields the runner's neutral {@link PiProcessConfig} (cwd/env — the
 * spawn shape, not a Pi wire type), so nothing Pi-shaped crosses upward
 * (INV-BOUNDARY).
 */
import { Context, Effect, Layer } from "effect";
import { FileSystem } from "effect/FileSystem";
import { Path } from "effect/Path";
import type { Job } from "@sprinter/domain";
import type { PiProcessConfig } from "@sprinter/runner";

/**
 * The per-Job spawn-config router PORT (INV-NAMING, `sprinter/<area>/<Name>`). The
 * `LocalPi` adapter depends on THIS service to decide where a Job's `pi` process
 * runs; a consumer provides the backing (INV-PORT).
 */
export class PiSpawnRouter extends Context.Service<
  PiSpawnRouter,
  {
    /**
     * Resolve the {@link PiProcessConfig} (notably `cwd`) for a {@link Job}. Total —
     * a routing/provisioning failure is a defect, not part of the port's contract.
     */
    readonly configFor: (job: Job) => Effect.Effect<PiProcessConfig>;
  }
>()("sprinter/job/PiSpawnRouter") {}

/**
 * The no-op router: every Job inherits the parent process's cwd (an empty
 * {@link PiProcessConfig}). It is the drop-in default for tests and for a daemon
 * that has no per-job worktree layout — a {@link Layer} substitution for
 * {@link layerWorktreeRouter} (INV-EFFECT-DI).
 */
export const layerInheritCwd: Layer.Layer<PiSpawnRouter> = Layer.succeed(
  PiSpawnRouter,
  PiSpawnRouter.of({ configFor: () => Effect.succeed({}) }),
);

/**
 * The per-Job worktree router: each Job runs `pi` in its OWN directory,
 * `<baseDir>/<job.id>`, created on demand (recursive `makeDirectory`, idempotent).
 * So concurrent Jobs never collide in one cwd and each agent operates on its own
 * worktree. Requires the {@link FileSystem} and {@link Path} services (the daemon
 * edge provides them, e.g. `BunServices`). A genuinely un-creatable base directory
 * is a provisioning defect (`orDie`) — not a routing outcome the port models.
 */
export const layerWorktreeRouter = (
  baseDir: string,
): Layer.Layer<PiSpawnRouter, never, FileSystem | Path> =>
  Layer.effect(
    PiSpawnRouter,
    Effect.gen(function* () {
      const fs = yield* FileSystem;
      const path = yield* Path;
      return PiSpawnRouter.of({
        configFor: (job) =>
          Effect.gen(function* () {
            const cwd = path.join(baseDir, job.id);
            yield* fs.makeDirectory(cwd, { recursive: true }).pipe(Effect.orDie);
            return { cwd };
          }),
      });
    }),
  );
