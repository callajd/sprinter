import { it } from "@effect/vitest";
import { expect } from "vitest";
import { daemonBanner } from "./index.ts";

it("banner includes the contract version", () => {
  expect(daemonBanner()).toBe("sprinter-daemon (contract v3)");
});
