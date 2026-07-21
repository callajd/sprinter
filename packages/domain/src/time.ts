/**
 * The owned instant type shared by every domain schema that stamps a moment.
 *
 * One representation, everywhere: an ISO-8601 UTC instant STRING. It is what the
 * wire already carries (JSON has no date type), what SQLite stores in a `TEXT`
 * column, and what the Swift mirror models as a `String` — so the same value
 * crosses the contract, the store, and the mirror without a per-boundary
 * conversion (INV-MIRROR).
 *
 * The value is CHECKED for REAL VALIDITY, not merely shape-matched: a `Timestamp`
 * is `YYYY-MM-DDTHH:MM:SS[.f…](Z|+00:00)` AND denotes an instant that actually
 * exists, so `2026-13-45T99:99:99Z`, `2026-02-30T00:00:00.000Z` and
 * `2026-07-20T24:00:00Z` are all rejected at the schema boundary rather than
 * discovered when something tries to order by them. This matters beyond
 * `Agent.retiredAt`: the same type carries EXTERNALLY-SOURCED instants (an observed
 * entity's `observedAt`), where a shape-only check would admit whatever an upstream
 * system happens to emit.
 *
 * ## Decoding NORMALISES; that is what makes string order an instant order
 *
 * A `Timestamp` is not the caller's spelling — it is the CANONICAL spelling of the
 * instant that spelling denotes. Decoding rewrites every accepted input to exactly
 *
 * ```
 * YYYY-MM-DDTHH:MM:SS.sssZ
 * ```
 *
 * — fixed width, always three fractional digits, always the literal `Z`. Only
 * because every `Timestamp` has that ONE shape is
 *
 * ```
 * a < b as strings  ⟺  a is earlier than b as an instant
 * ```
 *
 * true, and only then does a SQLite `TEXT` column's byte-order double as
 * chronological order. Without normalisation the property is simply FALSE, and
 * visibly so: `"2026-07-20T12:00:00Z" > "2026-07-20T12:00:00.500Z"` as strings
 * while being the EARLIER instant, and `…T12:00:00Z` / `…T12:00:00.000Z` are one
 * instant with two spellings that do not sort together. Normalising at the schema
 * boundary — the one place every instant enters the domain — is what makes the
 * ordering claim hold for real rather than by convention.
 *
 * Consequences of normalising, both deliberate:
 *
 * - **Sub-millisecond precision is ACCEPTED and TRUNCATED.** An upstream that emits
 *   microseconds (`.123456Z`) is emitting a valid instant; rejecting it would be a
 *   needless boundary failure. It decodes to `.123Z` — the domain's resolution is
 *   milliseconds, stated here rather than left implicit in whatever the caller sent.
 * - **The zero offset `+00:00` is ACCEPTED and rewritten to `Z`.** It denotes the
 *   same instant, and NORMALISATION — not rejection — is what keeps the canonical
 *   form unique. (Rejecting it, as this type once did, was justified by exactly the
 *   canonicality property normalisation now provides properly.) A NON-zero offset
 *   stays rejected: it is not a UTC instant string, and admitting one would mean
 *   re-deriving the UTC wall clock rather than restating it.
 *
 * ## A LEAP SECOND is a hard decode failure — stated, not hidden
 *
 * `:60` in the seconds field (`2026-06-30T23:59:60Z`) is REJECTED. This is a real
 * exclusion, not an oversight, and it is called out here because the paragraph above
 * advertises this type as carrying EXTERNALLY-SOURCED instants: real upstreams —
 * NTP-derived clocks, some observation APIs, anything restating a UTC broadcast — do
 * emit leap seconds, so a boundary that takes third-party stamps WILL meet one.
 *
 * It falls out of the canonical round trip rather than being spelled as a rule:
 * ECMAScript's time value has no leap second, so `Date.parse` rolls `:60` forward to
 * the following `:00`, which then fails to re-serialise as the form it came from and
 * {@link isRealInstant} refuses it.
 *
 * NORMALISING it — accepting `:60` and storing the rolled-forward `:00` — is
 * deliberately NOT done, and would be worse than refusing. It is not a spelling of the
 * same instant (unlike `+00:00` or a longer fraction): it SILENTLY CHANGES which
 * second the stamp denotes, and two distinct instants would collapse onto one string,
 * so the ordering property this type exists for would quietly stop being injective. A
 * refusal is loud, at the boundary, and attributable to the upstream that sent it.
 *
 * The consequence is a real cost, owned here: DE1.2, which wires `observedAt` to
 * externally-sourced timestamps, must plan for a leap-second stamp arriving as a
 * DECODE FAILURE of the whole observation and decide what that boundary does with it
 * (reject / clamp at the ADAPTER, where the choice is visible) — it must not assume
 * every well-formed upstream instant decodes.
 *
 * `Timestamp` is therefore a decode-side TRANSFORMATION, not a bare filter, and
 * encoding is the identity — the branded value is already canonical, so it goes to
 * the wire, the store, and the mirror byte-for-byte as decoded.
 */
import { Schema, SchemaTransformation } from "effect";

/**
 * The ISO-8601 UTC instant pattern: a full date, `T`, a full time with optional
 * fractional seconds at any precision, and an explicit UTC designator — the literal
 * `Z` or the ZERO offset `+00:00`, which decoding rewrites to `Z`. A NON-zero offset
 * is not matched: it is not a UTC instant string. Shape only; the field values are
 * checked for existence by {@link isRealInstant}.
 */
const ISO_8601_UTC = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?(?:Z|\+00:00)$/;

/**
 * True when `value` is a shape-valid ISO-8601 UTC string that denotes an instant
 * that REALLY EXISTS.
 *
 * The shape match alone accepts impossible field values (month `13`, day `45`,
 * hour `99`, `2026-02-30`), and `Date.parse` alone is not enough either: it accepts
 * some of those by ROLLING OVER (`2026-02-30` → 2 March, `24:00:00` → the next
 * day). So the check is a ROUND TRIP — build the millisecond-precision canonical
 * form from the MATCHED FIELDS, parse it, and require that re-serialising the parsed
 * instant reproduces exactly that form. A rolled-over field can never round-trip, so
 * this rejects every impossible instant while accepting sub-millisecond precision.
 *
 * Building the canonical form from the matched fields (rather than from `value`) is
 * what makes the check independent of the UTC designator: the offset is zero by
 * construction, so the matched date/time fields ARE the UTC wall clock.
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
 * Rewrite an ALREADY-VALIDATED instant string to the canonical
 * `YYYY-MM-DDTHH:MM:SS.sssZ` form — the single spelling every `Timestamp` has.
 *
 * `Date.prototype.toISOString` emits exactly that form, so parsing and
 * re-serialising IS the normalisation: it pads a missing or short fraction to three
 * digits, truncates a longer one to milliseconds, and renders the zero offset as
 * `Z`. It is total on the values {@link isRealInstant} admits — that check is
 * precisely the proof this round trip succeeds — so it runs only behind that filter
 * and needs no failure branch of its own.
 */
const canonicalise = (value: string): string => new Date(Date.parse(value)).toISOString();

/**
 * An instant, as a CANONICAL ISO-8601 UTC string (`2026-07-20T12:00:00.000Z`). Used
 * wherever the domain stamps a moment — `Agent.retiredAt` today.
 *
 * Decoding CHECKS the shape and the EXISTENCE of the instant ({@link isRealInstant})
 * and then NORMALISES it ({@link canonicalise}), so every `Timestamp` has one
 * fixed-width spelling and string order is instant order (see the module docstring).
 * Encoding is the identity: the branded value is already canonical.
 */
export const Timestamp = Schema.String.check(
  Schema.makeFilter((value: string) => isRealInstant(value), {
    expected: "an existing ISO-8601 UTC instant, e.g. 2026-07-20T12:00:00.000Z",
  }),
).pipe(
  Schema.decode(
    SchemaTransformation.transform({ decode: canonicalise, encode: (value: string) => value }),
  ),
  Schema.brand("Timestamp"),
);
export type Timestamp = (typeof Timestamp)["Type"];
