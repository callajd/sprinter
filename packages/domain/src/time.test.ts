/**
 * The owned {@link Timestamp} — an ISO-8601 UTC instant string. The suite pins the
 * accepted shape (with, without, and beyond millisecond fractional seconds) and the
 * rejections that matter: an instant that does not EXIST (an impossible field value,
 * or one `Date.parse` would silently roll over), a local or offset instant, a date
 * alone, and free-form text. Because two `Timestamp`s must be directly comparable by
 * string order, only the literal `Z` is UTC-explicit enough to qualify.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { Timestamp } from "./time.ts";

it.effect("accepts existing ISO-8601 UTC instants at any fractional precision", () =>
  Effect.forEach(
    [
      "2026-07-20T12:00:00Z",
      "2026-07-20T12:00:00.000Z",
      "2026-07-20T12:00:00.5Z",
      // Sub-millisecond precision IS a valid instant: an upstream that emits
      // microseconds is not emitting a malformed stamp, and the value round-trips
      // unchanged (only the validity check normalises; the branded value keeps its
      // digits).
      "2026-07-20T12:00:00.123456Z",
      // A real leap day, and the upper boundary of every field.
      "2024-02-29T00:00:00Z",
      "2026-12-31T23:59:59.999Z",
    ],
    (raw) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(Timestamp)(raw);
        expect(yield* Schema.encodeUnknownEffect(Timestamp)(decoded)).toBe(raw);
      }),
  ),
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
    ],
    (raw) =>
      Effect.exit(Schema.decodeUnknownEffect(Timestamp)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
  ),
);

it.effect("rejects non-UTC, partial, and free-form instants", () =>
  Effect.forEach(
    [
      "2026-07-20T12:00:00+02:00",
      // The ZERO offset is rejected too. It denotes the same instant as `Z`, but a
      // second spelling breaks the one-canonical-form property that makes string
      // order an instant order; a producer holding an offset instant normalises it.
      "2026-07-20T12:00:00+00:00",
      "2026-07-20T12:00:00",
      "2026-07-20",
      "yesterday",
      "",
    ],
    (raw) =>
      Effect.exit(Schema.decodeUnknownEffect(Timestamp)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
  ),
);

it.effect("rejects a non-string instant", () =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(Schema.decodeUnknownEffect(Timestamp)(1_753_012_800_000));
    expect(Exit.isFailure(exit)).toBe(true);
  }),
);
