/**
 * `@sprinter/contract` — the versioned daemon↔client RPC contract surface.
 *
 * Exports the FE2.3 `RpcGroup` (`effect/unstable/rpc`) — {@link SprinterRpc} —
 * built over the FE2.1 owned domain schemas, its contract-version marker, the
 * neutral error types, and the aggregate request/response + streamed-event
 * schemas ({@link ./rpc.ts}). Provider-neutral (D16) and maximally reactive
 * (D17): the surface speaks only owned domain types and streams its `events`
 * and `sessionEvents` feeds.
 */
export * from "./rpc.ts";
