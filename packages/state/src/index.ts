/**
 * `@sprinter/state` — the daemon's durable spine (Track A, epic AE2).
 *
 * The public surface is the persistence-agnostic {@link StateStore} PORT (an
 * Effect `Context.Service`) plus its owned error and event schemas, and the
 * SQLite ADAPTER `Layer` behind it. Consumers depend ONLY on the port and choose
 * a backing by providing an adapter layer (INV-PORT); the SQL/SQLite backing is
 * sealed inside {@link ./sqlite.ts} and never leaks a type here — {@link layer}
 * is a `Layer<StateStore, StateStoreError>`, backing-free.
 */
export type {
  AgentStore,
  EventLogStore,
  JobStore,
  SessionLogStore,
  WorkGraphStore,
} from "./store.ts";
export {
  AppendEvent,
  PersistedEvent,
  PersistedSessionEvent,
  StateStore,
  StateStoreError,
} from "./store.ts";
export type { StateStoreConfig } from "./sqlite.ts";
export { layer, layerMemory, SCHEMA_VERSION } from "./sqlite.ts";
