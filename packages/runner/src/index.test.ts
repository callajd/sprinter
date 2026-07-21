import { it } from "@effect/vitest";
import { Schema } from "effect";
import { expect } from "vitest";
import { makeExecution, PiRpcError, PiTransportError, ExecutionResult } from "./index.ts";

it("exposes the neutral transport error types from the barrel", () => {
  const rpcError = new PiRpcError({ command: "prompt", error: "boom" });
  expect(rpcError._tag).toBe("PiRpcError");
  expect(rpcError.error).toBe("boom");

  const transportError = new PiTransportError({ reason: "closed", detail: "ended" });
  expect(transportError._tag).toBe("PiTransportError");
  expect(transportError.reason).toBe("closed");
});

it("exposes the neutral execution factory and result from the barrel", () => {
  expect(typeof makeExecution).toBe("function");

  const completed = Schema.decodeUnknownSync(ExecutionResult)({ _tag: "Completed" });
  expect(completed._tag).toBe("Completed");
  const failed = Schema.decodeUnknownSync(ExecutionResult)({ _tag: "Failed", error: "boom" });
  expect(failed._tag).toBe("Failed");
});
