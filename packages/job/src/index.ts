/**
 * `@sprinter/job` — the single-Issue Job runner (Track A, epic AE3, task AE3.1).
 *
 * The first consumer of both AE1 (`@sprinter/runner`) and AE2 (`@sprinter/state`),
 * joining execution to durability: it dispatches a {@link Job} to a session,
 * captures its terminal {@link JobResult}, and persists the durable
 * Issue→Job→session mapping.
 *
 * The public surface is two PORTS and the runner's implementation `Layer`:
 *
 * - {@link ExecutionRunner} — the agent-runtime port the runner dispatches
 *   through (its local adapter wraps `@sprinter/runner`'s `makeSession`), plus its
 *   owned {@link ExecutionRunnerError}.
 * - {@link JobRunner} — the Job runner port, and {@link layer}, its implementation
 *   over the `ExecutionRunner` + `StateStore` ports.
 *
 * Everything here depends ONLY on ports and owned, provider-neutral types — never
 * on a concrete `pi` process or SQLite instance (INV-PORT). The `JobResult`
 * envelope is owned by `@sprinter/domain`.
 */
export { ExecutionRunner, ExecutionRunnerError } from "./execution-runner.ts";
export { JobRunner, layer } from "./job-runner.ts";
