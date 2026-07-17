/**
 * `@sprinter/contract` — versioned RPC contract surface.
 *
 * Scaffold-stage seed. `FE2.3` replaces this with the real `RpcGroup`
 * (`effect/unstable/rpc`) over the domain schemas. For now it carries the
 * contract version so the `check` gate exercises a real module.
 */

/** Current contract version. Bumped whenever the RPC surface changes (INV-CONTRACT). */
export const CONTRACT_VERSION = 1 as const;

/** Format the contract version as a `v`-prefixed tag. */
export const contractTag = (version: number = CONTRACT_VERSION): string => `v${version}`;
