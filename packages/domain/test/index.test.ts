import { describe, expect, test } from "bun:test";
import { decodeWorkstream, isComplete } from "../src/index.ts";

describe("@sprinter/domain", () => {
  test("decodes a valid workstream", () => {
    const ws = decodeWorkstream({ id: "fdn", name: "Foundation", status: "active" });
    expect(ws.name).toBe("Foundation");
    expect(ws.status).toBe("active");
  });

  test("rejects an invalid workstream", () => {
    expect(() => decodeWorkstream({ id: "", name: "x", status: "nope" })).toThrow();
  });

  test("isComplete reflects terminal status", () => {
    const done = decodeWorkstream({ id: "a", name: "A", status: "done" });
    const pending = decodeWorkstream({ id: "b", name: "B", status: "pending" });
    expect(isComplete(done)).toBe(true);
    expect(isComplete(pending)).toBe(false);
  });
});
