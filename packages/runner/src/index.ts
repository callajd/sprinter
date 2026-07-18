/**
 * `@sprinter/runner` — the local Pi `ExecutionRunner` adapter.
 *
 * This is the ONLY package permitted to import the owned Pi wire schema
 * (`@sprinter/domain/pi/wire`); nothing above it sees a Pi type (INV-BOUNDARY).
 *
 * Task AE1.1 ships the process + NDJSON wire substrate ({@link ./pi-transport.ts}).
 * That substrate necessarily traffics in Pi wire types, so it is INTERNAL and is
 * deliberately NOT re-exported here — the package's public surface stays Pi-free.
 *
 * Task AE1.2 adds the neutral {@link SessionHandle} ({@link ./session-handle.ts}):
 * it consumes the substrate and translates Pi's wire events into the owned
 * `SessionEvent` model, so everything exported here is expressed ONLY in neutral
 * types (`SessionEvent` / `SessionInput` / `UiResponse` from `@sprinter/domain`).
 * No Pi wire type crosses this boundary (INV-BOUNDARY).
 *
 * The public surface: the session factory + handle, its terminal result, the
 * spawn configuration, and the transport error types.
 */
export type { PiProcessConfig } from "./pi-transport.ts";
export { PiRpcError, PiTransportError } from "./pi-transport.ts";
export { make as makeSession, SessionResult } from "./session-handle.ts";
export type { SessionHandle } from "./session-handle.ts";
