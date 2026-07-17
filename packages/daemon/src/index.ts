/**
 * `@sprinter/daemon` — the control-plane daemon.
 *
 * Scaffold-stage seed. Later tasks (Track A) build the daemon's RPC server,
 * ports and adapters here. For now it exposes a small identity/banner helper
 * over the contract version so the `check` gate exercises a real module that
 * depends on the sibling workspaces.
 */
import { contractTag } from "@sprinter/contract";

/** Human-readable daemon identity banner, keyed to the active contract version. */
export const daemonBanner = (): string => `sprinter-daemon (contract ${contractTag()})`;
