/**
 * The owned {@link Timestamp} — a CANONICAL ISO-8601 UTC instant string.
 *
 * The suite pins three things: the accepted shape (with, without, and beyond
 * millisecond fractional seconds, plus the zero offset); the NORMALISATION every
 * accepted spelling is rewritten to; and the rejections that matter — an instant
 * that does not EXIST (an impossible field value, or one `Date.parse` would silently
 * roll over), a LEAP SECOND, a NON-zero offset, a local instant, a date alone, and
 * free-form text.
 *
 * The load-bearing test is the ORDERING property: `a < b` as strings ⟺ `a` is the
 * earlier instant. That is the whole reason normalisation exists (a SQLite `TEXT`
 * column's byte-order is only chronological order because of it), and it is checked
 * over every ordered pair of a spelling set built to break it — the same instant
 * written several ways, and instants that sort the WRONG way before normalisation.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { Timestamp } from "./time.ts";

const decode = Schema.decodeUnknownEffect(Timestamp);

it.effect("normalises every accepted spelling to YYYY-MM-DDTHH:MM:SS.sssZ", () =>
  Effect.forEach(
    [
      // An absent fraction is padded to three digits …
      ["2026-07-20T12:00:00Z", "2026-07-20T12:00:00.000Z"],
      // … an already-canonical value is unchanged …
      ["2026-07-20T12:00:00.000Z", "2026-07-20T12:00:00.000Z"],
      // … a SHORT fraction is padded (`.5` is 500ms, not 5ms) …
      ["2026-07-20T12:00:00.5Z", "2026-07-20T12:00:00.500Z"],
      // … sub-millisecond precision is accepted and TRUNCATED to the domain's
      // millisecond resolution rather than failing the boundary …
      ["2026-07-20T12:00:00.123456Z", "2026-07-20T12:00:00.123Z"],
      // … and the ZERO offset is accepted and rewritten to `Z`. It denotes the same
      // instant, and normalising it is what keeps the canonical form unique — which
      // is precisely what rejecting it used to be a (worse) proxy for.
      ["2026-07-20T12:00:00+00:00", "2026-07-20T12:00:00.000Z"],
      ["2026-07-20T12:00:00.250+00:00", "2026-07-20T12:00:00.250Z"],
      // A real leap day, and the upper boundary of every field.
      ["2024-02-29T00:00:00Z", "2024-02-29T00:00:00.000Z"],
      ["2026-12-31T23:59:59.999Z", "2026-12-31T23:59:59.999Z"],
    ] as const,
    ([raw, canonical]) =>
      Effect.gen(function* () {
        const decoded = yield* decode(raw);
        expect(decoded).toBe(canonical);
        // Encoding is the identity: the branded value is ALREADY canonical, so it
        // reaches the wire/store/mirror byte-for-byte as decoded.
        expect(yield* Schema.encodeUnknownEffect(Timestamp)(decoded)).toBe(canonical);
      }),
  ),
);

it.effect("orders as strings exactly as the instants order — the normalisation property", () =>
  Effect.gen(function* () {
    // Chosen to BREAK a non-normalising Timestamp: the first four are ONE instant
    // written four ways (three of which sort apart as raw strings), and the pairs
    // `…00Z` / `…00.500Z` and `…00+00:00` / `…00.500Z` sort the WRONG way before
    // normalisation (`"Z" > "."` byte-wise) while being earlier as instants.
    const spellings = [
      "2026-07-20T12:00:00Z",
      "2026-07-20T12:00:00.000Z",
      "2026-07-20T12:00:00+00:00",
      "2026-07-20T12:00:00.0Z",
      "2026-07-20T12:00:00.500Z",
      "2026-07-20T12:00:00.5Z",
      "2026-07-20T12:00:01Z",
      "2026-07-20T11:59:59.999Z",
      "2025-12-31T23:59:59.999999Z",
      "2026-07-21T00:00:00+00:00",
    ];
    const stamps = yield* Effect.forEach(spellings, (raw) => decode(raw));

    // Every ORDERED PAIR, including a value against itself: string comparison and
    // instant comparison must agree in all three directions (<, >, =). This is the
    // property the docstring claims and the SQLite `TEXT` ordering depends on.
    for (const a of stamps) {
      for (const b of stamps) {
        const byString = a < b ? -1 : a > b ? 1 : 0;
        const byInstant = Math.sign(Date.parse(a) - Date.parse(b));
        expect([a, b, byString]).toStrictEqual([a, b, byInstant]);
      }
    }
  }),
);

it.effect("rejects instants that do not exist, even when they are shape-valid", () =>
  Effect.forEach(
    [
      // Impossible field values — a shape-only check accepts every one of these.
      "2026-13-45T99:99:99Z",
      "0000-00-00T00:00:00Z",
      // Values `Date.parse` accepts by ROLLING OVER rather than rejecting: 30
      // February is 2 March, 24:00:00 is the next midnight, and 2026 is not a leap
      // year. None of them round-trips, so none of them decodes.
      "2026-02-30T00:00:00.000Z",
      "2026-07-20T24:00:00Z",
      "2026-02-29T00:00:00Z",
      // The same, spelled with the zero offset: the existence check reads the MATCHED
      // fields, so accepting `+00:00` does not open a hole in it.
      "2026-02-30T00:00:00+00:00",
    ],
    (raw) =>
      Effect.exit(decode(raw)).pipe(Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true))),
  ),
);

it.effect("rejects a LEAP SECOND, and never rolls one silently forward", () =>
  Effect.gen(function* () {
    // A real, correctly-spelled UTC leap second. Real upstreams emit them, and this
    // type carries EXTERNALLY-SOURCED instants, so this is an exclusion worth pinning
    // rather than an accident: ECMAScript's time value has no leap second, so the
    // canonical round trip can never reproduce `:60` and the decode FAILS.
    for (const raw of ["2026-06-30T23:59:60Z", "2016-12-31T23:59:60.000Z"]) {
      expect(Exit.isFailure(yield* Effect.exit(decode(raw)))).toBe(true);
    }

    // And it fails LOUDLY rather than normalising. Rolling `:60` forward to the next
    // `:00` — the one thing a normalising decoder would do here — would silently change
    // WHICH SECOND the stamp denotes and collapse two distinct instants onto one
    // canonical string, so the ordering property this type exists for would stop being
    // injective. The rolled-forward instant is a different value, and it is the one
    // nothing decoded to.
    expect(yield* decode("2026-07-01T00:00:00.000Z")).toBe("2026-07-01T00:00:00.000Z");
  }),
);

it.effect("rejects non-UTC, partial, and free-form instants", () =>
  Effect.forEach(
    [
      // A NON-zero offset is not a UTC instant string. Admitting it would mean
      // re-deriving the UTC wall clock rather than restating it, so it stays out
      // (unlike `+00:00`, which is the SAME wall clock and is merely re-spelled).
      "2026-07-20T12:00:00+02:00",
      "2026-07-20T12:00:00-05:30",
      "2026-07-20T12:00:00-00:00",
      "2026-07-20T12:00:00",
      "2026-07-20",
      "yesterday",
      "",
    ],
    (raw) =>
      Effect.exit(decode(raw)).pipe(Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true))),
  ),
);

it.effect("rejects a non-string instant", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(decode(1_753_012_800_000));
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);
