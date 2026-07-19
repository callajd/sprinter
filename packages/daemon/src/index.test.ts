import { it } from "@effect/vitest";
import { expect } from "vitest";
import { daemonBanner } from "./index.ts";

it("reports the daemon identity banner", () => {
  expect(daemonBanner()).toBe("sprinter-daemon");
});
