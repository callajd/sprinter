/**
 * The owned {@link Timestamp} — an ISO-8601 UTC instant string. The suite pins the
 * accepted shape (with and without fractional seconds) and the rejections that
 * matter: a local or offset instant, a date alone, and free-form text. Because two
 * `Timestamp`s must be directly comparable, only `Z` is UTC-explicit enough to
 * qualify.
 */
import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import { Timestamp } from "./time.ts";

it.effect("accepts ISO-8601 UTC instants with and without fractional seconds", () =>
  Effect.forEach(
    ["2026-07-20T12:00:00Z", "2026-07-20T12:00:00.000Z", "2026-07-20T12:00:00.5Z"],
    (raw) =>
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(Timestamp)(raw);
        expect(yield* Schema.encodeUnknownEffect(Timestamp)(decoded)).toBe(raw);
      }),
  ),
);

it.effect("rejects non-UTC, partial, and free-form instants", () =>
  Effect.forEach(
    [
      "2026-07-20T12:00:00+02:00",
      "2026-07-20T12:00:00",
      "2026-07-20",
      "yesterday",
      "",
      "2026-07-20T12:00:00.0000Z",
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
