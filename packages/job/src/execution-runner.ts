/**
 * `ExecutionRunner` — the agent-runtime PORT the Job runner dispatches through
 * (Track A, task AE3.1). It is the fakeable seam over "start an execution for a
 * {@link Job}": the core depends on THIS service, never on a concrete `pi`
 * process (INV-PORT). Its single, current adapter is the local Pi runner
 * (`@sprinter/runner`'s `makeExecution`), wired at the daemon edge where a
 * `ChildProcessSpawner` is available; tests provide a fake that returns a canned
 * {@link ExecutionHandle}.
 *
 * The port is expressed ONLY in owned, provider-neutral types: it takes an owned
 * {@link Job} and yields an owned {@link ExecutionHandle} (the runner's neutral
 * surface). No Pi wire type crosses it (INV-PORT / INV-BOUNDARY). The handle is
 * `Scope`-managed — closing the caller's scope tears the execution down.
 *
 * Tag id follows INV-NAMING (`sprinter/<area>/<Name>`) and matches the
 * architecture's `ExecutionRunner` port (architecture §4).
 */
import { Context, type Effect, Schema } from "effect";
import type { Scope } from "effect/Scope";
import type { AgentContent, Job } from "@sprinter/domain";
import type { ExecutionHandle } from "@sprinter/runner";

/**
 * The owned failure raised when the runner cannot start an execution for a job
 * (INV-NAMING, `*Error` via `Schema.TaggedErrorClass`). An adapter translates its
 * backing-specific spawn/launch failures into this neutral type at the boundary,
 * so no consumer depends on a concrete runtime's error shape (INV-PORT).
 */
export class ExecutionRunnerError extends Schema.TaggedErrorClass<ExecutionRunnerError>()(
  "ExecutionRunnerError",
  {
    /** The runner operation that failed, e.g. `"run"`. */
    operation: Schema.String,
    /** A neutral, human-readable description of the cause. */
    detail: Schema.String,
  },
) {}

/**
 * Start (dispatch) an execution for a {@link Job}. The returned {@link ExecutionHandle}
 * is `Scope`-managed by the caller: it lives for the dispatch and is torn down when
 * the caller's scope closes. `run` fails with the owned {@link ExecutionRunnerError}
 * when the execution cannot be started.
 *
 * **Terminal-result contract (CONSUMER-CRITICAL):** the returned execution MUST reach
 * a terminal `ExecutionResult` for the driven work — i.e. its `events` stream ends and
 * its `result` resolves. The {@link JobRunner} awaits `handle.result` as the terminal
 * authority, so an adapter over a long-lived `pi --mode rpc` process (which stays
 * alive, idle, after a single turn) MUST make the execution one-shot for the dispatch —
 * either spawn a per-job process that exits after the turn, or drive to `ExecutionIdle`
 * and close the execution's scope so `result` resolves. An execution that idles forever
 * without exiting would hang `dispatch`. AE3.1 builds to this contract and tests it
 * with a settling fake; enforcing it is the concrete LocalPi adapter's responsibility
 * (lands with the daemon runtime wiring, AE4/AE5).
 */
export class ExecutionRunner extends Context.Service<
  ExecutionRunner,
  {
    readonly run: (job: Job) => Effect.Effect<ExecutionHandle, ExecutionRunnerError, Scope>;
    /**
     * WHAT this runner runs — the {@link AgentContent} of the agent revision every
     * execution it starts is attributed to (DE2.2 / D2).
     *
     * A PLAIN VALUE, not an effect, for the same reason `StateStore.generation` is one:
     * it cannot change while the service exists. An adapter declares the agent it
     * dispatches through at CONSTRUCTION, so swapping the agent means providing a
     * different `Layer`, and nothing can observe it changing underneath a live
     * dispatch.
     *
     * It is CONTENT, not a whole `Agent`: the revision's identity is derived from the
     * content (`agent-registration.ts`), so an adapter cannot mint an id that
     * contradicts what it actually runs, and re-declaring the same content is an
     * idempotent no-op in the registry rather than a collision.
     */
    readonly agent: AgentContent;
  }
>()("sprinter/execution/ExecutionRunner") {}
