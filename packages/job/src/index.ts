/**
 * `@sprinter/job` — the single-Issue Job runner (Track A, epic AE3, task AE3.1).
 *
 * The first consumer of both AE1 (`@sprinter/runner`) and AE2 (`@sprinter/state`),
 * joining execution to durability: it dispatches a {@link Job} to an execution,
 * captures its terminal {@link JobResult}, and persists the durable
 * Issue→Job→execution mapping.
 *
 * The public surface is two PORTS, the runner's implementation `Layer`, and the
 * concrete `LocalPi` adapter behind the `ExecutionRunner` port:
 *
 * - {@link ExecutionRunner} — the agent-runtime port the runner dispatches
 *   through, plus its owned {@link ExecutionRunnerError}.
 * - {@link JobRunner} — the Job runner port, and {@link layer}, its implementation
 *   over the `ExecutionRunner` + `StateStore` ports.
 * - {@link layerLocalPi} — the concrete `LocalPi` `ExecutionRunner` adapter over
 *   `@sprinter/runner`'s `makeExecution`; a drop-in `Layer` substitution for the test
 *   fake, requiring a `ChildProcessSpawner` and a {@link PiSpawnRouter} behind it
 *   (INV-EFFECT-DI).
 * - {@link PiSpawnRouter} — the per-Job spawn-config (cwd/worktree) routing port the
 *   `LocalPi` adapter reads, with {@link layerInheritCwd} (no-op default) and
 *   {@link layerWorktreeRouter} (a per-Job `<baseDir>/<job.id>` worktree) adapters.
 *
 * The two ports and the Job runner depend ONLY on ports and owned, provider-neutral
 * types — never on a concrete `pi` process or SQLite instance (INV-PORT); the
 * `LocalPi` adapter is the ONE module that knows the runner's `makeExecution`, and it
 * exposes nothing Pi-shaped above the tag. The `JobResult` envelope is owned by
 * `@sprinter/domain`.
 */
export { ExecutionRunner, ExecutionRunnerError } from "./execution-runner.ts";
export { JobRunner, layer, executionIdFor } from "./job-runner.ts";
export { layerLocalPi } from "./local-pi-runner.ts";
export { layerInheritCwd, layerWorktreeRouter, PiSpawnRouter } from "./spawn-router.ts";
