import { describe, expect, test } from "bun:test";
import { daemonBanner } from "../src/index.ts";

describe("@sprinter/daemon", () => {
  test("banner includes the contract version", () => {
    expect(daemonBanner()).toBe("sprinter-daemon (contract v1)");
  });
});
