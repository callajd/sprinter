import { it } from "@effect/vitest";
import { Context, Option, Schema } from "effect";
import { RpcSchema } from "effect/unstable/rpc";
import { expect } from "vitest";
import {
  Issue,
  IssueId,
  Job,
  Session,
  SessionEvent,
  SessionId,
  SessionInput,
  UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import {
  answerUiRequest,
  ContractVersion,
  CONTRACT_VERSION,
  ControlAction,
  contractTag,
  control,
  createWorkstreamFromPlan,
  events,
  interrupt,
  IssueNotFound,
  PlanRejected,
  retryIssue,
  SessionNotFound,
  sessionEvents,
  sessionSend,
  Snapshot,
  snapshot,
  SprinterRpc,
  WorkGraphEvent,
  WorkstreamNotFound,
  WorkstreamPlan,
} from "./rpc.ts";

/** The full procedure surface of contract v1 (architecture §7). */
const ALL_TAGS = [
  "snapshot",
  "events",
  "createWorkstreamFromPlan",
  "control",
  "retryIssue",
  "sessionEvents",
  "sessionSend",
  "interrupt",
  "answerUiRequest",
] as const;

// Representative, domain-valid fixtures.
const workstream = {
  id: "ws-1",
  name: "Foundation",
  repo: "callajd/sprinter",
  status: "active",
  epics: ["ep-1"],
};
const issue = {
  id: "iss-1",
  epicId: "ep-1",
  number: 10,
  title: "RPC contract v1",
  status: "in_progress",
  dependsOn: ["iss-0"],
};
const job = { id: "job-1", issueId: "iss-1", kind: "implement", status: "running" };
const session = { id: "ses-1", jobId: "job-1", status: "active" };

it("carries every model of contract v1 as a procedure", () => {
  expect([...SprinterRpc.requests.keys()].sort()).toEqual([...ALL_TAGS].sort());
});

it("marks the group with an explicit contract version (INV-CONTRACT)", () => {
  expect(Option.getOrThrow(Context.getOption(SprinterRpc.annotations, ContractVersion))).toBe(2);
  // The version key is a `Context.Reference`, so it resolves to CONTRACT_VERSION
  // from an empty context via its default.
  expect(Context.get(Context.empty(), ContractVersion)).toBe(CONTRACT_VERSION);
  expect(CONTRACT_VERSION).toBe(2);
  expect(contractTag()).toBe("v2");
  expect(contractTag(1)).toBe("v1");
});

it("streams the reactive feeds and not the request/response models (INV-REACTIVE)", () => {
  expect(RpcSchema.isStreamSchema(events.successSchema)).toBe(true);
  expect(RpcSchema.isStreamSchema(sessionEvents.successSchema)).toBe(true);

  expect(RpcSchema.isStreamSchema(snapshot.successSchema)).toBe(false);
  expect(RpcSchema.isStreamSchema(createWorkstreamFromPlan.successSchema)).toBe(false);
  expect(RpcSchema.isStreamSchema(control.successSchema)).toBe(false);
});

it("hydrates full state through the snapshot success schema (resolves to domain types)", () => {
  const full = {
    workstreams: [workstream],
    epics: [{ id: "ep-1", workstreamId: "ws-1", name: "FE2", status: "active", issues: ["iss-1"] }],
    issues: [issue],
    jobs: [job],
    sessions: [session],
  };
  const decoded = Schema.decodeUnknownSync(snapshot.successSchema)(full);

  // The value round-trips through the standalone Snapshot schema and its parts
  // are exactly the FE2.1 domain schemas.
  expect(decoded).toEqual(Schema.decodeUnknownSync(Snapshot)(full));
  expect(Schema.decodeUnknownSync(Workstream)(workstream).id).toBe("ws-1");
  expect(decoded.issues[0]?.number).toBe(10);
});

it("streams owned work-graph deltas over the events feed (INV-REACTIVE)", () => {
  const delta = { _tag: "IssueChanged", issue };
  const decoded = Schema.decodeUnknownSync(events.successSchema.success)(delta);
  expect(decoded).toEqual(Schema.decodeUnknownSync(WorkGraphEvent)(delta));
  if (decoded._tag !== "IssueChanged") throw new Error("expected IssueChanged");
  expect(decoded.issue.id).toBe("iss-1");
});

it("accepts a workstream plan and answers with a WorkstreamId", () => {
  const plan = { plan: { name: "Foundation", repo: "callajd/sprinter", spec: "build it" } };
  const decodedPayload = Schema.decodeUnknownSync(createWorkstreamFromPlan.payloadSchema)(plan);
  expect(decodedPayload).toEqual({
    plan: Schema.decodeUnknownSync(WorkstreamPlan)(plan.plan),
  });

  const id = Schema.decodeUnknownSync(createWorkstreamFromPlan.successSchema)("ws-1");
  expect(id).toBe(Schema.decodeUnknownSync(WorkstreamId)("ws-1"));

  // Its error channel is the neutral PlanRejected.
  const err = Schema.decodeUnknownSync(createWorkstreamFromPlan.errorSchema)({
    _tag: "PlanRejected",
    reason: "empty spec",
  });
  expect(err).toBeInstanceOf(PlanRejected);
  expect(err.reason).toBe("empty spec");
});

it("controls a workstream by id and action, failing with WorkstreamNotFound", () => {
  const payload = Schema.decodeUnknownSync(control.payloadSchema)({
    workstreamId: "ws-1",
    action: "pause",
  });
  expect(payload.action).toBe("pause");
  expect(Schema.decodeUnknownSync(ControlAction)("resume")).toBe("resume");
  expect(() =>
    Schema.decodeUnknownSync(control.payloadSchema)({ workstreamId: "ws-1", action: "x" }),
  ).toThrow();

  const err = Schema.decodeUnknownSync(control.errorSchema)({
    _tag: "WorkstreamNotFound",
    id: "ws-1",
  });
  expect(err).toBeInstanceOf(WorkstreamNotFound);
});

it("retries an issue by id, failing with IssueNotFound", () => {
  const payload = Schema.decodeUnknownSync(retryIssue.payloadSchema)({ issueId: "iss-1" });
  expect(payload.issueId).toBe(Schema.decodeUnknownSync(IssueId)("iss-1"));

  const err = Schema.decodeUnknownSync(retryIssue.errorSchema)({
    _tag: "IssueNotFound",
    id: "iss-1",
  });
  expect(err).toBeInstanceOf(IssueNotFound);
});

it("streams the neutral SessionEvent over sessionEvents, keyed by session id", () => {
  const payload = Schema.decodeUnknownSync(sessionEvents.payloadSchema)({ sessionId: "ses-1" });
  expect(payload.sessionId).toBe(Schema.decodeUnknownSync(SessionId)("ses-1"));

  const event = { _tag: "MessageDelta", messageId: "m1", text: "hi" };
  const decoded = Schema.decodeUnknownSync(sessionEvents.successSchema.success)(event);
  expect(decoded).toEqual(Schema.decodeUnknownSync(SessionEvent)(event));

  // The stream error is the neutral SessionNotFound.
  const err = Schema.decodeUnknownSync(sessionEvents.successSchema.error)({
    _tag: "SessionNotFound",
    id: "ses-1",
  });
  expect(err).toBeInstanceOf(SessionNotFound);
});

it("drives input into a session via sessionSend", () => {
  const input = { text: "go", mode: "prompt" };
  const payload = Schema.decodeUnknownSync(sessionSend.payloadSchema)({
    sessionId: "ses-1",
    input,
  });
  expect(payload.input).toEqual(Schema.decodeUnknownSync(SessionInput)(input));

  const err = Schema.decodeUnknownSync(sessionSend.errorSchema)({
    _tag: "SessionNotFound",
    id: "ses-1",
  });
  expect(err).toBeInstanceOf(SessionNotFound);
});

it("interrupts a session by id", () => {
  const payload = Schema.decodeUnknownSync(interrupt.payloadSchema)({ sessionId: "ses-1" });
  expect(payload.sessionId).toBe("ses-1");
});

it("answers an outstanding UI request via answerUiRequest", () => {
  const response = { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } };
  const payload = Schema.decodeUnknownSync(answerUiRequest.payloadSchema)({
    sessionId: "ses-1",
    response,
  });
  expect(payload.response).toEqual(Schema.decodeUnknownSync(UiResponse)(response));
});

it("rejects a payload that is not an owned domain value", () => {
  // A branded id must be a non-empty string: an empty id fails to decode.
  expect(() => Schema.decodeUnknownSync(retryIssue.payloadSchema)({ issueId: "" })).toThrow();
  // A snapshot with a malformed issue (missing required fields) fails.
  expect(() =>
    Schema.decodeUnknownSync(snapshot.successSchema)({
      workstreams: [],
      epics: [],
      issues: [{ id: "iss-1" }],
      jobs: [],
      sessions: [],
    }),
  ).toThrow();
});

// Reference the imported domain fixtures so the suite documents the exact owned
// shapes the contract carries (INV-PORT / INV-NAMING).
it("uses only owned domain fixtures", () => {
  expect(Schema.decodeUnknownSync(Issue)(issue).epicId).toBe("ep-1");
  expect(Schema.decodeUnknownSync(Job)(job).kind).toBe("implement");
  expect(Schema.decodeUnknownSync(Session)(session).status).toBe("active");
});
