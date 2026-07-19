/**
 * `@sprinter/daemon` — the control-plane daemon.
 *
 * Task AE4.1 lands the real `SprinterRpc` server handlers behind the frozen
 * contract, plus the reactive spine they run on (INV-CONTRACT / INV-REACTIVE):
 *
 * - {@link handlers} — the `effect/unstable/rpc` handler `Layer` for
 *   `@sprinter/contract`'s `SprinterRpc` (snapshot / events / command handlers),
 *   depending only on the `StateStore`, `JobRunner` and `WorkGraphEvents` PORTS.
 * - {@link WorkGraphEvents} / {@link layerWorkGraphEvents} — the real `PubSub`
 *   feed behind the streaming `events` RPC (D17), carrying offset-stamped deltas.
 * - {@link layerJournaling} — the `StateStore` decorator that both journals every
 *   persisted mutation durably AND fans it out onto that feed stamped with its
 *   durable offset, so the daemon is reactive-plus-durable end-to-end.
 *
 * Task AE4.2 adds the session channel behind the same frozen contract: the
 * {@link SessionRegistry} PORT (`sessionId → live SessionHandle`) the four
 * session-channel handlers resolve against, bridging a live `@sprinter/runner`
 * session's neutral surface (INV-BOUNDARY / INV-PORT).
 *
 * Task AE5.1 adds restart safety: the {@link StartupReconcile} service, wired to
 * the `StateStore` / `Repository` / `JobRunner` PORTS, that on boot reconciles the
 * durable graph against the host and resumes any in-flight Job onto its persisted
 * session — without loss or double-run (INV-PORT).
 *
 * Task CE1.2 provisions the runnable daemon: {@link mainLayer} — the composition
 * root, a single Effect layer graph wiring the file-backed `StateStore`, the real
 * `ExecutionRunner`, `Repository`, and the `RpcServer` handlers into a served
 * endpoint over a concrete socket transport (INV-EFFECT-DI); {@link layerJournaling}
 * + {@link resyncEvents} — durable, offset-based `events` resync (D17); and
 * {@link bootLayer} — the boot-time `StartupReconcile` run. The runnable process
 * entrypoint is the sibling `run.ts` (`sprinter-daemon` bin).
 */
import { contractTag } from "@sprinter/contract";

export { layerJournaling, resyncEvents, resyncFrom } from "./event-journal.ts";
export type { DaemonConfig } from "./main.ts";
export {
  appLayer,
  bootLayer,
  configFromEnv,
  executionRunnerLayer,
  mainLayer,
  socketProtocolLayer,
  stateStoreLayer,
} from "./main.ts";
export { handlers } from "./rpc-handlers.ts";
export { layer as layerSessionRegistry, SessionRegistry } from "./session-registry.ts";
export type { StartupSummary } from "./startup-reconcile.ts";
export { layer as layerStartupReconcile, StartupReconcile } from "./startup-reconcile.ts";
export { layer as layerWorkGraphEvents, WorkGraphEvents } from "./work-graph-events.ts";

/** Human-readable daemon identity banner, keyed to the active contract version. */
export const daemonBanner = (): string => `sprinter-daemon (contract ${contractTag()})`;
