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
 *   feed behind the streaming `events` RPC (D17).
 * - {@link layerPublishing} — the `StateStore` decorator that fans every persisted
 *   mutation out onto that feed, so the daemon is reactive end-to-end.
 *
 * Task AE4.2 adds the session channel behind the same frozen contract: the
 * {@link SessionRegistry} PORT (`sessionId → live SessionHandle`) the four
 * session-channel handlers resolve against, bridging a live `@sprinter/runner`
 * session's neutral surface (INV-BOUNDARY / INV-PORT).
 */
import { contractTag } from "@sprinter/contract";

export { handlers } from "./rpc-handlers.ts";
export { layer as layerSessionRegistry, SessionRegistry } from "./session-registry.ts";
export { layerPublishing } from "./store-publishing.ts";
export { layer as layerWorkGraphEvents, WorkGraphEvents } from "./work-graph-events.ts";

/** Human-readable daemon identity banner, keyed to the active contract version. */
export const daemonBanner = (): string => `sprinter-daemon (contract ${contractTag()})`;
