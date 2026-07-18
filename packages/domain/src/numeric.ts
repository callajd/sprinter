/**
 * Shared numeric-constraint schemas for the owned domain (read model + session
 * model). `pi/wire.ts` keeps its own copies deliberately — that module stays
 * dependency-free to remain a self-contained foreign wire mirror.
 */
import { Schema } from "effect";

/** A positive integer — GitHub Issue/PR numbers and the like. */
export const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
export type PositiveInt = (typeof PositiveInt)["Type"];

/** A non-negative integer — token counts, retry attempts, delays. */
export const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export type NonNegativeInt = (typeof NonNegativeInt)["Type"];
