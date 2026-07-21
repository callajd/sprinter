/**
 * The owned instant type shared by every domain schema that stamps a moment.
 *
 * One representation, everywhere: an ISO-8601 UTC instant STRING. It is what the
 * wire already carries (JSON has no date type), what SQLite stores in a `TEXT`
 * column with lexicographic order matching chronological order, and what the
 * Swift mirror models as a `String` — so the same value crosses the contract, the
 * store, and the mirror without a per-boundary conversion (INV-MIRROR).
 *
 * The value is CHECKED for REAL VALIDITY, not merely shape-matched: a `Timestamp`
 * is `YYYY-MM-DDTHH:MM:SS[.f…]Z` AND denotes an instant that actually exists, so
 * `2026-13-45T99:99:99Z`, `2026-02-30T00:00:00.000Z` and `2026-07-20T24:00:00Z`
 * are all rejected at the schema boundary rather than discovered when something
 * tries to order by them. This matters beyond `Agent.retiredAt`: the same type
 * carries EXTERNALLY-SOURCED instants (an observed entity's `observedAt`), where a
 * shape-only check would admit whatever an upstream system happens to emit.
 *
 * Fractional seconds are accepted at ANY precision (`.5Z`, `.123Z`, `.123456Z`) —
 * an upstream that emits microseconds is emitting a valid instant, and truncating
 * or rejecting it would be a needless boundary failure. UTC is expressed ONLY as
 * the literal `Z`: a numeric offset (even the zero offset `+00:00`) is deliberately
 * NOT a `Timestamp`, because one canonical spelling per instant is what makes two
 * stamps directly comparable by string ordering — the property the SQLite `TEXT`
 * column's lexicographic order relies on. A producer holding an offset instant
 * normalises it to `Z` before it becomes a `Timestamp`.
 */
import { Schema } from "effect";

/**
 * The ISO-8601 UTC instant pattern: a full date, `T`, a full time with optional
 * fractional seconds at any precision, and the literal `Z` (UTC only — see the
 * module docstring on why an offset instant is not a `Timestamp`). Shape only; the
 * field values are checked for existence by {@link isRealInstant}.
 */
const ISO_8601_UTC = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z$/;

/**
 * True when `value` is a shape-valid ISO-8601 UTC string that denotes an instant
 * that REALLY EXISTS.
 *
 * The shape match alone accepts impossible field values (month `13`, day `45`,
 * hour `99`, `2026-02-30`), and `Date.parse` alone is not enough either: it accepts
 * some of those by ROLLING OVER (`2026-02-30` → 2 March, `24:00:00` → the next
 * day). So the check is a ROUND TRIP — normalise to the millisecond-precision
 * canonical form, parse it, and require that re-serialising the parsed instant
 * reproduces exactly that form. A rolled-over field can never round-trip, so this
 * rejects every impossible instant while accepting sub-millisecond precision (only
 * the check normalises; the stored/branded value keeps its original digits).
 */
const isRealInstant = (value: string): boolean => {
  const match = ISO_8601_UTC.exec(value);
  if (match === null) return false;
  const [, date = "", time = "", fraction = ""] = match;
  const canonical = `${date}T${time}.${fraction.padEnd(3, "0").slice(0, 3)}Z`;
  const parsed = Date.parse(canonical);
  return !Number.isNaN(parsed) && new Date(parsed).toISOString() === canonical;
};

/**
 * An instant, as an ISO-8601 UTC string (e.g. `2026-07-20T12:00:00.000Z`). Used
 * wherever the domain stamps a moment — `Agent.retiredAt` today. Both the shape and
 * the EXISTENCE of the instant are checked (see {@link isRealInstant}).
 */
export const Timestamp = Schema.String.check(
  Schema.makeFilter((value: string) => isRealInstant(value), {
    expected: "an existing ISO-8601 UTC instant, e.g. 2026-07-20T12:00:00.000Z",
  }),
).pipe(Schema.brand("Timestamp"));
export type Timestamp = (typeof Timestamp)["Type"];
