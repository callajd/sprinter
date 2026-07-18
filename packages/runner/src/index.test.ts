import { it } from "@effect/vitest";
import { Schema } from "effect";
import { expect } from "vitest";
import { makeSession, PiRpcError, PiTransportError, SessionResult } from "./index.ts";

it("exposes the neutral transport error types from the barrel", () => {
  const rpcError = new PiRpcError({ command: "prompt", error: "boom" });
  expect(rpcError._tag).toBe("PiRpcError");
  expect(rpcError.error).toBe("boom");

  const transportError = new PiTransportError({ reason: "closed", detail: "ended" });
  expect(transportError._tag).toBe("PiTransportError");
  expect(transportError.reason).toBe("closed");
});

it("exposes the neutral session factory and result from the barrel", () => {
  expect(typeof makeSession).toBe("function");

  const completed = Schema.decodeUnknownSync(SessionResult)({ _tag: "Completed" });
  expect(completed._tag).toBe("Completed");
  const failed = Schema.decodeUnknownSync(SessionResult)({ _tag: "Failed", error: "boom" });
  expect(failed._tag).toBe("Failed");
});
