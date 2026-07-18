import { it } from "@effect/vitest";
import { expect } from "vitest";
import { PiRpcError, PiTransportError } from "./index.ts";

it("exposes the neutral transport error types from the barrel", () => {
  const rpcError = new PiRpcError({ command: "prompt", error: "boom" });
  expect(rpcError._tag).toBe("PiRpcError");
  expect(rpcError.error).toBe("boom");

  const transportError = new PiTransportError({ reason: "closed", detail: "ended" });
  expect(transportError._tag).toBe("PiTransportError");
  expect(transportError.reason).toBe("closed");
});
