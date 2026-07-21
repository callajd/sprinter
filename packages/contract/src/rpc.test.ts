import { it } from "@effect/vitest";
import { Schema } from "effect";
import { RpcSchema } from "effect/unstable/rpc";
import { expect } from "vitest";
import {
  Agent,
  Execution,
  ExecutionEvent,
  ExecutionId,
  ExecutionInput,
  Issue,
  IssueId,
  Job,
  UiResponse,
  Workstream,
  WorkstreamId,
} from "@sprinter/domain";
import {
  answerUiRequest,
  ControlAction,
  control,
  createWorkstreamFromPlan,
  events,
  interrupt,
  IssueNotFound,
  OffsetExecutionEvent,
  PlanRejected,
  ResyncRequired,
  retryIssue,
  ExecutionNotFound,
  executionEvents,
  executionSend,
  Snapshot,
  snapshot,
  SprinterRpc,
  WorkGraphEvent,
  WorkstreamNotFound,
  WorkstreamPlan,
} from "./rpc.ts";

/** The full procedure surface of the contract (architecture §7). */
const ALL_TAGS = [
  "snapshot",
  "events",
  "createWorkstreamFromPlan",
  "control",
  "retryIssue",
  "executionEvents",
  "executionSend",
  "interrupt",
  "answerUiRequest",
] as const;

// Representative, domain-valid fixtures.
const repository = {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
};
const workstream = {
  id: "ws-1",
  name: "Foundation",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["ep-1"],
};
const issue = {
  id: "iss-1",
  epicId: "ep-1",
  number: 10,
  title: "RPC contract mirror",
  status: "in_progress",
  dependsOn: ["iss-0"],
};
const job = { id: "job-1", issueId: "iss-1", kind: "implement", status: "running" };
const execution = { id: "exe-1", jobId: "job-1", status: "active" };
const agent = {
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read", "edit"],
};

it("carries every model of the contract as a procedure", () => {
  expect([...SprinterRpc.requests.keys()].sort()).toEqual([...ALL_TAGS].sort());
});

it("streams the reactive feeds and not the request/response models (INV-REACTIVE)", () => {
  expect(RpcSchema.isStreamSchema(events.successSchema)).toBe(true);
  expect(RpcSchema.isStreamSchema(executionEvents.successSchema)).toBe(true);

  expect(RpcSchema.isStreamSchema(snapshot.successSchema)).toBe(false);
  expect(RpcSchema.isStreamSchema(createWorkstreamFromPlan.successSchema)).toBe(false);
  expect(RpcSchema.isStreamSchema(control.successSchema)).toBe(false);
});

it("hydrates full state through the snapshot success schema (resolves to domain types)", () => {
  const full = {
    repositories: [repository],
    workstreams: [workstream],
    epics: [{ id: "ep-1", workstreamId: "ws-1", name: "FE2", status: "active", issues: ["iss-1"] }],
    issues: [issue],
    jobs: [job],
    executions: [execution],
    agents: [agent, { ...agent, id: "agt-2", supersedes: "agt-1" }],
    generation: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b",
  };
  const decoded = Schema.decodeUnknownSync(snapshot.successSchema)(full);

  // The value round-trips through the standalone Snapshot schema and its parts
  // are exactly the FE2.1 domain schemas.
  expect(decoded).toEqual(Schema.decodeUnknownSync(Snapshot)(full));
  expect(Schema.decodeUnknownSync(Workstream)(workstream).id).toBe("ws-1");
  // The STATE layer rides the snapshot too: `Workstream.repositoryId` is a REFERENCE,
  // so a client receiving workstreams without their repositories could resolve none of
  // them. Each record carries its own `observedAt` — what DE4.4 renders staleness from.
  expect(decoded.repositories[0]?.id).toBe("repo:github:1296269");
  expect(decoded.repositories[0]?.observedAt).toBe("2026-07-20T12:00:00.000Z");
  expect(decoded.issues[0]?.number).toBe(10);
  // The REGISTRY layer rides the snapshot whole — every revision, no per-repo slice
  // (an Agent names no repository; the per-repo view is a fold, INV-DERIVED).
  expect(decoded.agents.map((a) => a.id)).toEqual(["agt-1", "agt-2"]);
  expect(decoded.agents[1]?.supersedes).toBe("agt-1");
  expect("retiredAt" in (decoded.agents[0] ?? {})).toBe(false);
});

it("carries the registry as an upsert-only AgentChanged delta with no removal variant", () => {
  const retired = { ...agent, retiredAt: "2026-07-20T12:00:00.000Z" };
  const decoded = Schema.decodeUnknownSync(WorkGraphEvent)({
    _tag: "AgentChanged",
    agent: retired,
  });
  if (decoded._tag !== "AgentChanged") throw new Error("expected AgentChanged");
  expect(decoded.agent.retiredAt).toBe("2026-07-20T12:00:00.000Z");
  // Retirement is a STAMP carried by an ordinary change delta; the append-only
  // registry has no removal, so there is no `AgentRemoved` variant to decode.
  expect(() =>
    Schema.decodeUnknownSync(WorkGraphEvent)({ _tag: "AgentRemoved", id: "agt-1" }),
  ).toThrow();
});

it("streams offset-stamped owned work-graph deltas over the events feed (INV-REACTIVE)", () => {
  // The streamed success is the OffsetEvent envelope (CE2.0): the
  // owned delta PLUS the durable offset the client feeds back as `resume.sinceOffset`.
  const delta = { _tag: "IssueChanged", issue };
  const item = { offset: 7, event: delta };
  const decoded = Schema.decodeUnknownSync(events.successSchema.success)(item);
  expect(decoded.event).toEqual(Schema.decodeUnknownSync(WorkGraphEvent)(delta));
  expect(decoded.offset).toBe(7);
  if (decoded.event._tag !== "IssueChanged") throw new Error("expected IssueChanged");
  expect(decoded.event.issue.id).toBe("iss-1");
  // The offset is a NON-NEGATIVE int: a negative offset is rejected.
  expect(() =>
    Schema.decodeUnknownSync(events.successSchema.success)({ offset: -1, event: delta }),
  ).toThrow();
});

const generation = "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b";

it("carries an OPTIONAL resume context on the events request (CE2.0)", () => {
  // Present: a non-negative offset resumes strictly after that point.
  const withCursor = Schema.decodeUnknownSync(events.payloadSchema)({
    resume: { sinceOffset: 12, generation },
  });
  expect(withCursor.resume?.sinceOffset).toBe(12);

  // Absent: the key is optional, so an empty payload decodes (origin replay,
  // backward-compatible with the earlier (pre-cursor) no-field request).
  const noCursor = Schema.decodeUnknownSync(events.payloadSchema)({});
  expect(noCursor.resume).toBeUndefined();

  // A negative offset is not a non-negative int and is rejected.
  expect(() =>
    Schema.decodeUnknownSync(events.payloadSchema)({ resume: { sinceOffset: -1, generation } }),
  ).toThrow();
});

it("makes a cursor without its generation UNREPRESENTABLE, at every offset (INV-SUM)", () => {
  // THE STRUCTURAL PROPERTY. `sinceOffset` and `generation` are ONE value, so the two
  // states a runtime pairing check would have to reject cannot be built at all: a
  // resume context missing either half fails to DECODE.
  expect(() =>
    Schema.decodeUnknownSync(events.payloadSchema)({ resume: { sinceOffset: 12 } }),
  ).toThrow();
  expect(() =>
    Schema.decodeUnknownSync(events.payloadSchema)({ resume: { generation } }),
  ).toThrow();
  // Same on the execution channel — no weaker shape for the second feed.
  expect(() =>
    Schema.decodeUnknownSync(executionEvents.payloadSchema)({
      executionId: "exe-1",
      resume: { sinceOffset: 4 },
    }),
  ).toThrow();

  // And `sinceOffset: 0` is an ORDINARY resume, not a disguised origin request: it is
  // representable ONLY inside a resume context, so it always carries a generation for
  // the daemon to compare. The origin request is the ABSENCE of the whole value — the
  // presence of `resume`, never the value of an offset, is what distinguishes them.
  const zero = Schema.decodeUnknownSync(events.payloadSchema)({
    resume: { sinceOffset: 0, generation },
  });
  expect(zero.resume).toStrictEqual({ sinceOffset: 0, generation });
  expect(Schema.decodeUnknownSync(events.payloadSchema)({}).resume).toBeUndefined();
});

it("pairs a resume cursor with the STORE GENERATION it was minted in", () => {
  // A cursor is a coordinate in ONE generation's log, so the resume context carries the
  // generation alongside it — the value the client read off `Snapshot.generation`.
  const resume = Schema.decodeUnknownSync(events.payloadSchema)({
    resume: { sinceOffset: 12, generation },
  });
  expect(resume.resume?.generation).toBe(generation);
  // The SAME pairing on the execution channel: its per-execution log is dropped by a schema
  // bump exactly as the work-graph log is, so its cursor is generation-scoped too.
  const executionResume = Schema.decodeUnknownSync(executionEvents.payloadSchema)({
    executionId: "exe-1",
    resume: { sinceOffset: 4, generation },
  });
  expect(executionResume.resume?.generation).toBe(generation);

  // The whole context is OPTIONAL because an ORIGIN request (a first connect) has none.
  expect(Schema.decodeUnknownSync(events.payloadSchema)({}).resume).toBeUndefined();
  // An empty generation is not a non-empty string and is rejected.
  expect(() =>
    Schema.decodeUnknownSync(events.payloadSchema)({ resume: { sinceOffset: 1, generation: "" } }),
  ).toThrow();

  // The snapshot is where the generation ORIGINATES, so it is REQUIRED there — a snapshot
  // without one would hand a client state it could never construct a valid resume from.
  expect(() =>
    Schema.decodeUnknownSync(snapshot.successSchema)({
      workstreams: [],
      epics: [],
      issues: [],
      jobs: [],
      executions: [],
      agents: [],
    }),
  ).toThrow();

  // And ResyncRequired names the daemon's CURRENT generation, so the refusal says which
  // context the client must re-hydrate into — not merely that its cursor is bad.
  // A streaming RPC's error rides the `RpcSchema.Stream` envelope, so the channel under
  // test is `successSchema.error` (its `errorSchema` is the non-streaming one, `never`).
  const refusal = Schema.decodeUnknownSync(events.successSchema.error)({
    _tag: "ResyncRequired",
    sinceOffset: 2,
    maxOffset: 3,
    generation,
  });
  expect(refusal).toBeInstanceOf(ResyncRequired);
  expect(refusal.generation).toBe(generation);
  // The refusal is expressible even when the cursor is WITHIN the log's extent — the case
  // an offset-only rule could not detect at all.
  expect(refusal.sinceOffset).toBeLessThanOrEqual(refusal.maxOffset);

  // `executionEvents` therefore has a TWO-error channel: the existence question and the
  // generation question are independent, and both must be expressible on it.
  expect(
    Schema.decodeUnknownSync(executionEvents.successSchema.error)({
      _tag: "ExecutionNotFound",
      id: "exe-9",
    }),
  ).toBeInstanceOf(ExecutionNotFound);
  expect(
    Schema.decodeUnknownSync(executionEvents.successSchema.error)({
      _tag: "ResyncRequired",
      sinceOffset: 2,
      maxOffset: 3,
      generation,
    }),
  ).toBeInstanceOf(ResyncRequired);
});

it("decodes the events request through the wire JSON codec — {} replays from origin (CE2.0 B1)", () => {
  // Regression for the events-payload end-to-end decode bug (CE2.0 re-review): the daemon decodes
  // each request payload through `Schema.toCodecJson(payloadSchema)` — the exact seam
  // `RpcServer` drives over the NDJSON serializer, NOT the `RpcTest` shortcut that
  // bypasses serialization. The events payload is a `Struct`, so what the
  // client puts on the wire for the `payload` key matters end-to-end.
  const codec = Schema.toCodecJson(events.payloadSchema);

  // The canonical Effect client (and the fixed Swift client) send a PRESENT empty
  // object `{}` for `.events({})`; it MUST decode to the origin-replay case (no
  // `resume`). This is the assertion that would have caught the Swift client
  // omitting the `payload` key.
  expect(Schema.decodeUnknownSync(codec)({}).resume).toBeUndefined();

  // Documents the boundary the omitted-payload bug tripped: an ABSENT payload
  // (`undefined`, an omitted `payload` key on the wire) is NOT a valid events
  // request under the events `Struct` schema and fails to decode ("Expected object, got
  // undefined"). The fix is to send a present `{}`, not to widen the schema to accept
  // `undefined`.
  expect(() => Schema.decodeUnknownSync(codec)(undefined)).toThrow();
});

it("accepts a workstream plan and answers with a WorkstreamId", () => {
  const plan = {
    plan: {
      name: "Foundation",
      repository: { host: "github", owner: "callajd", name: "sprinter" },
      spec: "build it",
    },
  };
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

it("streams the offset-stamped durable transcript over executionEvents, keyed by execution id", () => {
  // The streamed success is the OffsetExecutionEvent envelope: a durable,
  // transcript-grade ExecutionEvent PLUS the durable per-execution offset the client feeds back
  // as `resume.sinceOffset`. It mirrors the `events` feed's OffsetEvent envelope.
  const payload = Schema.decodeUnknownSync(executionEvents.payloadSchema)({ executionId: "exe-1" });
  expect(payload.executionId).toBe(Schema.decodeUnknownSync(ExecutionId)("exe-1"));

  const event = {
    _tag: "EntryAppended",
    entry: { _tag: "AssistantMessage", id: "a1", text: "hi" },
  };
  const item = { offset: 7, event };
  const decoded = Schema.decodeUnknownSync(executionEvents.successSchema.success)(item);
  expect(decoded).toEqual(Schema.decodeUnknownSync(OffsetExecutionEvent)(item));
  expect(decoded.offset).toBe(7);
  expect(decoded.event).toEqual(Schema.decodeUnknownSync(ExecutionEvent)(event));

  // A negative offset is rejected (NonNegativeInt), matching the `events` envelope.
  expect(() =>
    Schema.decodeUnknownSync(executionEvents.successSchema.success)({ offset: -1, event }),
  ).toThrow();

  // The stream error is the neutral ExecutionNotFound.
  const err = Schema.decodeUnknownSync(executionEvents.successSchema.error)({
    _tag: "ExecutionNotFound",
    id: "exe-1",
  });
  expect(err).toBeInstanceOf(ExecutionNotFound);
});

it("carries an OPTIONAL resume context on the executionEvents request", () => {
  // Present cursor: resume STRICTLY AFTER that durable per-execution offset.
  const withCursor = Schema.decodeUnknownSync(executionEvents.payloadSchema)({
    executionId: "exe-1",
    resume: { sinceOffset: 12, generation },
  });
  expect(withCursor.resume?.sinceOffset).toBe(12);

  // Absent cursor: the KEY is omitted (present-but-empty beyond `executionId`) → replay the
  // execution's durable transcript from the ORIGIN.
  const noCursor = Schema.decodeUnknownSync(executionEvents.payloadSchema)({
    executionId: "exe-1",
  });
  expect(noCursor.resume).toBeUndefined();

  // A negative cursor is rejected (NonNegativeInt).
  expect(() =>
    Schema.decodeUnknownSync(executionEvents.payloadSchema)({
      executionId: "exe-1",
      resume: { sinceOffset: -1, generation },
    }),
  ).toThrow();

  // Through the wire JSON codec: a request with executionId and NO resume key decodes to
  // the origin-replay case (mirrors the `events` B1 seam for the execution channel).
  const codec = Schema.toCodecJson(executionEvents.payloadSchema);
  expect(Schema.decodeUnknownSync(codec)({ executionId: "exe-1" }).resume).toBeUndefined();
  expect(
    Schema.decodeUnknownSync(codec)({
      executionId: "exe-1",
      resume: { sinceOffset: 3, generation },
    }).resume?.sinceOffset,
  ).toBe(3);
});

it("drives input into an execution via executionSend", () => {
  const input = { text: "go", mode: "prompt" };
  const payload = Schema.decodeUnknownSync(executionSend.payloadSchema)({
    executionId: "exe-1",
    input,
  });
  expect(payload.input).toEqual(Schema.decodeUnknownSync(ExecutionInput)(input));

  const err = Schema.decodeUnknownSync(executionSend.errorSchema)({
    _tag: "ExecutionNotFound",
    id: "exe-1",
  });
  expect(err).toBeInstanceOf(ExecutionNotFound);
});

it("interrupts an execution by id", () => {
  const payload = Schema.decodeUnknownSync(interrupt.payloadSchema)({ executionId: "exe-1" });
  expect(payload.executionId).toBe("exe-1");
});

it("answers an outstanding UI request via answerUiRequest", () => {
  const response = { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } };
  const payload = Schema.decodeUnknownSync(answerUiRequest.payloadSchema)({
    executionId: "exe-1",
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
      executions: [],
      agents: [],
      generation: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b",
    }),
  ).toThrow();
  // And a malformed agent (an unparseable `retiredAt`) fails the same way.
  expect(() =>
    Schema.decodeUnknownSync(snapshot.successSchema)({
      workstreams: [],
      epics: [],
      issues: [],
      jobs: [],
      executions: [],
      agents: [{ ...agent, retiredAt: "yesterday" }],
      generation: "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b",
    }),
  ).toThrow();
});

// Reference the imported domain fixtures so the suite documents the exact owned
// shapes the contract carries (INV-PORT / INV-NAMING).
it("uses only owned domain fixtures", () => {
  expect(Schema.decodeUnknownSync(Issue)(issue).epicId).toBe("ep-1");
  expect(Schema.decodeUnknownSync(Job)(job).kind).toBe("implement");
  expect(Schema.decodeUnknownSync(Execution)(execution).status).toBe("active");
  expect(Schema.decodeUnknownSync(Agent)(agent).tools).toEqual(["read", "edit"]);
});
