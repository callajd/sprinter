/**
 * `ExecutionRunner` — the agent-runtime PORT the Job runner dispatches through
 * (Track A, task AE3.1). It is the fakeable seam over "start a session for a
 * {@link Job}": the core depends on THIS service, never on a concrete `pi`
 * process (INV-PORT). Its single, current adapter is the local Pi runner
 * (`@sprinter/runner`'s `makeSession`), wired at the daemon edge where a
 * `ChildProcessSpawner` is available; tests provide a fake that returns a canned
 * {@link SessionHandle}.
 *
 * The port is expressed ONLY in owned, provider-neutral types: it takes an owned
 * {@link Job} and yields an owned {@link SessionHandle} (the runner's neutral
 * surface). No Pi wire type crosses it (INV-PORT / INV-BOUNDARY). The handle is
 * `Scope`-managed — closing the caller's scope tears the session down.
 *
 * Tag id follows INV-NAMING (`sprinter/<area>/<Name>`) and matches the
 * architecture's `ExecutionRunner` port (architecture §4).
 */
import { Context, type Effect, Schema } from "effect";
import type { Scope } from "effect/Scope";
import type { Job } from "@sprinter/domain";
import type { SessionHandle } from "@sprinter/runner";

/**
 * The owned failure raised when the runner cannot start a session for a job
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
 * Start (dispatch) a session for a {@link Job}. The returned {@link SessionHandle}
 * is `Scope`-managed by the caller: it lives for the dispatch and is torn down when
 * the caller's scope closes. `run` fails with the owned {@link ExecutionRunnerError}
 * when the session cannot be started.
 */
export class ExecutionRunner extends Context.Service<
  ExecutionRunner,
  {
    readonly run: (job: Job) => Effect.Effect<SessionHandle, ExecutionRunnerError, Scope>;
  }
>()("sprinter/execution/ExecutionRunner") {}
