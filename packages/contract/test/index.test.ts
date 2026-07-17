import { describe, expect, test } from "bun:test";
import { CONTRACT_VERSION, contractTag } from "../src/index.ts";

describe("@sprinter/contract", () => {
  test("exposes the contract version", () => {
    expect(CONTRACT_VERSION).toBe(1);
  });

  test("tags the current version by default", () => {
    expect(contractTag()).toBe("v1");
  });

  test("tags an explicit version", () => {
    expect(contractTag(3)).toBe("v3");
  });
});
