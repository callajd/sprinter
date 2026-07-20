/**
 * The owned instant type shared by every domain schema that stamps a moment.
 *
 * One representation, everywhere: an ISO-8601 UTC instant STRING. It is what the
 * wire already carries (JSON has no date type), what SQLite stores in a `TEXT`
 * column with lexicographic order matching chronological order, and what the
 * Swift mirror models as a `String` — so the same value crosses the contract, the
 * store, and the mirror without a per-boundary conversion (INV-MIRROR).
 *
 * The shape is CHECKED, not merely branded: a `Timestamp` is always
 * `YYYY-MM-DDTHH:MM:SS[.sss]Z`, so a malformed instant is rejected at the schema
 * boundary rather than discovered when something tries to order by it.
 */
import { Schema } from "effect";

/**
 * The ISO-8601 UTC instant pattern: a full date, `T`, a full time with optional
 * fractional seconds, and the literal `Z` (UTC only — a local or offset instant is
 * NOT a `Timestamp`, so two stamps are always directly comparable).
 */
const ISO_8601_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;

/**
 * An instant, as an ISO-8601 UTC string (e.g. `2026-07-20T12:00:00.000Z`). Used
 * wherever the domain stamps a moment — `Agent.retiredAt` today.
 */
export const Timestamp = Schema.String.check(Schema.isPattern(ISO_8601_UTC)).pipe(
  Schema.brand("Timestamp"),
);
export type Timestamp = (typeof Timestamp)["Type"];
