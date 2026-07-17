import { it } from "@effect/vitest";
import { expect } from "vitest";
import { CONTRACT_VERSION, contractTag } from "./index.ts";

it("exposes the contract version", () => {
  expect(CONTRACT_VERSION).toBe(1);
});

it("tags the current version by default", () => {
  expect(contractTag()).toBe("v1");
});

it("tags an explicit version", () => {
  expect(contractTag(3)).toBe("v3");
});
