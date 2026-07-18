/**
 * `@sprinter/runner` — the local Pi `ExecutionRunner` adapter.
 *
 * This is the ONLY package permitted to import the owned Pi wire schema
 * (`@sprinter/domain/pi/wire`); nothing above it sees a Pi type (INV-BOUNDARY).
 *
 * Task AE1.1 ships the process + NDJSON wire substrate ({@link ./pi-transport.ts}).
 * That substrate necessarily traffics in Pi wire types, so it is INTERNAL and is
 * deliberately NOT re-exported here — the package's public surface stays Pi-free.
 * The neutral `SessionHandle` + `SessionEvent` translation that packages above
 * the runner consume is built on the substrate in AE1.2 and exported then.
 *
 * For now the public surface is the spawn configuration and the transport error
 * types (all provider-neutral: no Pi wire type crosses this boundary).
 */
export type { PiProcessConfig } from "./pi-transport.ts";
export { PiRpcError, PiTransportError } from "./pi-transport.ts";
