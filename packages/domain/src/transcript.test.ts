/**
 * D4's falsifying check — `LiveTranscript` and `SealedTranscript` are DISTINCT TYPES,
 * so a TAIL SUBSCRIPTION on a sealed transcript does not TYPECHECK.
 *
 * The claim being tested is not "the operation is refused"; a boolean flag plus a
 * runtime `if` would satisfy that, and would be forgotten by the next code path that
 * needs to tail something. The claim is that the call is UNREPRESENTABLE — it fails to
 * compile — and the only way to test that is to make the compiler itself the assertion.
 *
 * So the assertions below are CONDITIONAL TYPES evaluated at typecheck time and pinned
 * with `satisfies`. Each also yields a value the runtime test asserts on, so the file
 * has no unused declarations and the failure is legible from both directions:
 *
 * - if the two types ever COLLAPSE (one becomes assignable to the other — a shared
 *   flagged struct, an added `sealed: boolean`, a widened union), the conditional flips
 *   and `satisfies` FAILS THE TYPECHECK, which fails `bun run check`;
 * - the runtime expectations then read as documentation of what the compiler proved.
 *
 * The tail/subscribe operation itself belongs to DE4.2 — this task lands the types it
 * depends on — so it is DECLARED here at the signature that matters (it takes a
 * `LiveTranscript`) rather than implemented.
 */
import { Effect, Schema } from "effect";
import { expect, it } from "vitest";
import { type LiveTranscript, type SealedTranscript, Transcript } from "./read-model.ts";

/**
 * The SHAPE of a tail subscription: it takes an OPEN transcript, because an open
 * transcript is the only kind that has more to deliver. DE4.2 implements it; what is
 * load-bearing here is the parameter type.
 */
declare const subscribeToTail: (transcript: LiveTranscript) => void;

/** The parameter a tail subscription accepts. */
type Tailable = Parameters<typeof subscribeToTail>[0];

/**
 * A `SealedTranscript` is NOT a `Tailable`: `subscribeToTail(sealed)` is a COMPILE
 * error, not a runtime rejection. If this ever became `true`, the two types would no
 * longer be distinct enough to make the mistake unrepresentable — and this line would
 * stop compiling.
 */
const sealedIsNotTailable = false satisfies SealedTranscript extends Tailable ? true : false;

/** …and a `LiveTranscript` IS, so the check above is a real distinction, not a type nobody satisfies. */
const liveIsTailable = true satisfies LiveTranscript extends Tailable ? true : false;

/**
 * The distinction runs BOTH ways: a live transcript is not a sealed one either, so
 * nothing can read a `lastOffset` off a run that has not finished producing one.
 */
const liveIsNotSealed = false satisfies LiveTranscript extends SealedTranscript ? true : false;

it("makes a tail subscription on a SEALED transcript unrepresentable (D4)", () => {
  // The real assertions are the three `satisfies` above, checked by `tsc`. These pin
  // the same facts at runtime so the file states what was proven, and so a future
  // edit that deletes the type-level lines fails visibly rather than silently.
  expect(sealedIsNotTailable).toBe(false);
  expect(liveIsTailable).toBe(true);
  expect(liveIsNotSealed).toBe(false);
  // POSITIVE CONTROL: the live transcript this is all about really is constructible and
  // really is what `subscribeToTail` takes — so "sealed does not typecheck" is a
  // statement about the SEALED case, not about a signature nothing can satisfy.
  const live: LiveTranscript = { _tag: "LiveTranscript" };
  subscribeToTailIsCallable(live);
  expect(live._tag).toBe("LiveTranscript");
});

/**
 * A real call site for {@link subscribeToTail}'s signature — declared, never invoked
 * (the declaration above has no implementation), so the POSITIVE control exercises the
 * TYPE without needing DE4.2's runtime.
 */
const subscribeToTailIsCallable = (transcript: Tailable): void => {
  expect(transcript._tag).toBe("LiveTranscript");
};

it("neither variant carries a flag saying which it is — the TAG is the whole difference", () => {
  const live = Schema.decodeUnknownSync(Transcript)({ _tag: "LiveTranscript" });
  const sealed = Schema.decodeUnknownSync(Transcript)({
    _tag: "SealedTranscript",
    lastOffset: 9,
  });
  // The only keys are the tag and (on the sealed variant) its REQUIRED extent. A
  // `sealed: boolean` — or any other second signal — would be a field that must agree
  // with the tag (INV-SUM/INV-ENFORCE), and would let the two types unify.
  expect(Object.keys(live)).toStrictEqual(["_tag"]);
  expect(Object.keys(sealed).sort()).toStrictEqual(["_tag", "lastOffset"]);
});

it("a sealed transcript's extent is REQUIRED — an extentless seal has no wire form", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const outcome = yield* Effect.exit(
        Schema.decodeUnknownEffect(Transcript)({ _tag: "SealedTranscript" }),
      );
      expect(outcome._tag).toBe("Failure");
      // …while `0` is a perfectly good extent: a run that produced no durable entry has
      // an EMPTY sealed transcript, not an absent one.
      const empty = yield* Schema.decodeUnknownEffect(Transcript)({
        _tag: "SealedTranscript",
        lastOffset: 0,
      });
      expect(empty).toStrictEqual({ _tag: "SealedTranscript", lastOffset: 0 });
    }),
  ));
