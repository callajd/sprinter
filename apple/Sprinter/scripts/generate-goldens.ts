/**
 * One-off golden generator for the Swift contract mirror (issue FE2.4).
 *
 * Encodes REPRESENTATIVE values of every contract-v1 message schema through the
 * MERGED TypeScript contract (`@sprinter/contract` over `@sprinter/domain`),
 * writing the resulting wire JSON to
 * `Tests/SprinterContractTests/Goldens/*.json`. The Swift `SprinterContract`
 * mirror is decode-tested against these committed goldens, so it is validated
 * against REAL contract output rather than hand-typed JSON that could drift in
 * the same direction as the mirror (INV-CONTRACT).
 *
 * This script is NOT part of any gate: `make check` only DECODES the committed
 * goldens (no bun dependency inside the Swift gate). It is re-run by a human when
 * the contract changes — see `docs/contract-mirror.md` (the INV-CONTRACT ripple
 * procedure).
 *
 * Run from anywhere in the repo:
 *   bun run apple/Sprinter/scripts/generate-goldens.ts
 *
 * `Schema.encodeUnknownSync` VALIDATES its input against the schema before
 * encoding, so every representative value below is proven contract-valid as a
 * side effect of generation.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import {
  Epic,
  Issue,
  Job,
  PullRequestRef,
  Session,
  SessionEvent,
  SessionInput,
  TranscriptEntry,
  UiResponse,
  Usage,
  Workstream,
} from "../../../packages/domain/src/index.ts";
import {
  answerUiRequest,
  control,
  ControlAction,
  createWorkstreamFromPlan,
  events,
  interrupt,
  IssueNotFound,
  PlanRejected,
  retryIssue,
  sessionEvents,
  sessionSend,
  Snapshot,
  SessionNotFound,
  WorkGraphEvent,
  WorkstreamNotFound,
  WorkstreamPlan,
} from "../../../packages/contract/src/index.ts";

const here = dirname(fileURLToPath(import.meta.url));
const goldensDir = join(here, "..", "Tests", "SprinterContractTests", "Goldens");
mkdirSync(goldensDir, { recursive: true });

/** Encode a value through its schema (validating it) and write the wire JSON. */
const write = <S extends Schema.Codec<unknown, unknown>>(
  name: string,
  schema: S,
  value: unknown,
): void => {
  const encoded = Schema.encodeUnknownSync(schema)(value);
  writeFileSync(join(goldensDir, `${name}.json`), `${JSON.stringify(encoded, null, 2)}\n`);
};

// ── Owned domain node fixtures (also embedded in the snapshot) ───────────────

const prMerged = { number: 15, url: "https://github.com/callajd/sprinter/pull/15", merged: true };
const prOpen = { number: 16, url: "https://github.com/callajd/sprinter/pull/16", merged: false };

const issueWithPr = {
  id: "iss-1",
  epicId: "ep-1",
  number: 10,
  title: "RPC contract v1",
  status: "in_review",
  dependsOn: ["iss-0"],
  pr: prOpen,
};
const issueNoPr = {
  id: "iss-2",
  epicId: "ep-1",
  number: 11,
  title: "Swift contract bridge",
  status: "in_progress",
  dependsOn: [],
};

const jobFull = {
  id: "job-1",
  issueId: "iss-1",
  kind: "implement",
  status: "running",
  sessionId: "ses-1",
  transcriptRef: "transcripts/ses-1.jsonl",
  pr: prMerged,
};
const jobMinimal = { id: "job-2", issueId: "iss-2", kind: "review", status: "queued" };

const session = { id: "ses-1", jobId: "job-1", status: "active" };
const epic = {
  id: "ep-1",
  workstreamId: "ws-1",
  name: "FE2",
  status: "active",
  issues: ["iss-1", "iss-2"],
};
const workstream = {
  id: "ws-1",
  name: "Foundation",
  repo: "callajd/sprinter",
  status: "active",
  epics: ["ep-1"],
};

// ── Snapshot (hydration) ─────────────────────────────────────────────────────

write("snapshot", Snapshot, {
  workstreams: [workstream],
  epics: [epic],
  issues: [issueWithPr, issueNoPr],
  jobs: [jobFull, jobMinimal],
  sessions: [session],
});

// Individual owned nodes (exercise every DTO + optional-key present/absent).
write("workstream", Workstream, workstream);
write("epic", Epic, epic);
write("issue-with-pr", Issue, issueWithPr);
write("issue-no-pr", Issue, issueNoPr);
write("job-full", Job, jobFull);
write("job-minimal", Job, jobMinimal);
write("session", Session, session);
write("pull-request-ref", PullRequestRef, prMerged);

// The distinct terminal `cancelled` WorkStatus (contract v2 / CE5.1) — a cancelled
// planning node is terminal-but-not-`done`, so the mirror + board render it apart.
write("workstream-cancelled", Workstream, { ...workstream, status: "cancelled" });
write("epic-cancelled", Epic, { ...epic, status: "cancelled" });

// ── WorkGraphEvent — every variant ───────────────────────────────────────────

write("work-graph-events", Schema.Array(WorkGraphEvent), [
  { _tag: "WorkstreamChanged", workstream },
  { _tag: "EpicChanged", epic },
  { _tag: "IssueChanged", issue: issueWithPr },
  { _tag: "JobChanged", job: jobFull },
  { _tag: "SessionChanged", session },
]);

// ── SessionEvent — every variant (+ optional present/absent) ─────────────────

const usageFull = {
  inputTokens: 1200,
  outputTokens: 340,
  cacheReadTokens: 800,
  cacheWriteTokens: 64,
};
const usageMinimal = { inputTokens: 10, outputTokens: 5 };

write("session-events", Schema.Array(SessionEvent), [
  { _tag: "TurnStarted" },
  { _tag: "TurnCompleted", usage: usageFull },
  { _tag: "TurnCompleted" },
  { _tag: "MessageStarted", messageId: "m1" },
  { _tag: "MessageDelta", messageId: "m1", text: "Hello", reasoning: "thinking" },
  { _tag: "MessageDelta", messageId: "m1", text: "world" },
  { _tag: "MessageDelta", messageId: "m1", reasoning: "more thought" },
  { _tag: "MessageDelta", messageId: "m1" },
  { _tag: "MessageCompleted", messageId: "m1" },
  { _tag: "ToolStarted", id: "t1", name: "read_file", input: { path: "/etc/hosts", limit: 20 } },
  { _tag: "ToolProgress", id: "t1", partial: { bytes: 512 } },
  { _tag: "ToolCompleted", id: "t1", output: ["line-a", "line-b"], isError: false },
  { _tag: "SessionIdle" },
  { _tag: "RetryScheduled", attempt: 2, delayMs: 1500, error: "429 rate limited" },
  { _tag: "ContextCompacted" },
  { _tag: "UiRequestRaised", id: "req-1", kind: "select", prompt: "Pick one", options: ["a", "b"] },
  { _tag: "UiRequestRaised", id: "req-2", kind: "confirm", prompt: "Proceed?" },
  { _tag: "Notice", id: "notice-disk", level: "warn", message: "disk space low" },
  { _tag: "StatusChanged", key: "phase", text: "planning" },
  {
    _tag: "EntryAppended",
    entry: { _tag: "AssistantMessage", id: "a1", text: "done", reasoning: "because" },
  },
  // A content-derived notice with NO reconciliation key — the `NoticeId` optional-key
  // is ABSENT (contract v2 / CE5.2). The consumer keys these by arrival sequence so
  // distinct occurrences stay distinct rather than collapsing onto one item.
  { _tag: "Notice", level: "error", message: "retry failed after 5 attempt(s)" },
]);

// ── TranscriptEntry — every variant ──────────────────────────────────────────

write("transcript-entries", Schema.Array(TranscriptEntry), [
  { _tag: "UserMessage", id: "u1", text: "please fix the bug" },
  { _tag: "AssistantMessage", id: "a1", text: "on it", reasoning: "planning" },
  { _tag: "AssistantMessage", id: "a2", text: "no reasoning here" },
  { _tag: "ToolCall", id: "c1", name: "grep", input: { pattern: "TODO" } },
  { _tag: "ToolResult", id: "c1", output: { matches: 3 }, isError: false },
  { _tag: "NoticeEntry", id: "notice-compile", level: "error", message: "compilation failed" },
]);

// ── Usage — full + minimal ───────────────────────────────────────────────────

write("usages", Schema.Array(Usage), [usageFull, usageMinimal]);

// ── SessionInput — every mode (+ images present/absent) ──────────────────────

write("session-inputs", Schema.Array(SessionInput), [
  { text: "kick it off", mode: "prompt", images: ["img-ref-1"] },
  { text: "actually, focus here", mode: "steer" },
  { text: "one more thing", mode: "followUp" },
]);

// ── UiResponse — every UiAnswer variant ──────────────────────────────────────

write("ui-responses", Schema.Array(UiResponse), [
  { requestId: "req-1", answer: { _tag: "Value", value: "option-a" } },
  { requestId: "req-2", answer: { _tag: "Confirmed", confirmed: true } },
  { requestId: "req-3", answer: { _tag: "Cancelled" } },
]);

// ── WorkstreamPlan ───────────────────────────────────────────────────────────

write("workstream-plan", WorkstreamPlan, {
  name: "Foundation",
  repo: "callajd/sprinter",
  spec: "Build the daemon↔client contract and its mirrors.",
});

// ── ControlAction — every literal ────────────────────────────────────────────

write("control-actions", Schema.Array(ControlAction), ["start", "pause", "resume", "cancel"]);

// ── Contract errors — every variant ──────────────────────────────────────────
//
// The contract errors are `Schema.TaggedErrorClass`es, so they encode from CLASS
// INSTANCES (not plain objects). Decode each representative wire object into its
// instance, then encode it back through its own schema to capture the real wire
// shape.
const encodedErrors = [
  Schema.encodeUnknownSync(WorkstreamNotFound)(
    Schema.decodeUnknownSync(WorkstreamNotFound)({ _tag: "WorkstreamNotFound", id: "ws-9" }),
  ),
  Schema.encodeUnknownSync(IssueNotFound)(
    Schema.decodeUnknownSync(IssueNotFound)({ _tag: "IssueNotFound", id: "iss-9" }),
  ),
  Schema.encodeUnknownSync(SessionNotFound)(
    Schema.decodeUnknownSync(SessionNotFound)({ _tag: "SessionNotFound", id: "ses-9" }),
  ),
  Schema.encodeUnknownSync(PlanRejected)(
    Schema.decodeUnknownSync(PlanRejected)({ _tag: "PlanRejected", reason: "empty spec" }),
  ),
];
writeFileSync(
  join(goldensDir, "contract-errors.json"),
  `${JSON.stringify(encodedErrors, null, 2)}\n`,
);

// ── Command payloads (the wire the daemon receives) ──────────────────────────

// The `events` request cursor is OPTIONAL (contract v3 / CE2.0): both wire forms
// are captured — present (`sinceOffset` key set) and absent (key omitted, the
// backward-compatible origin replay) — so the Swift mirror decodes each.
write("payload-events", events.payloadSchema, { sinceOffset: 12 });
write("payload-events-no-offset", events.payloadSchema, {});
write("payload-create-workstream-from-plan", createWorkstreamFromPlan.payloadSchema, {
  plan: { name: "Foundation", repo: "callajd/sprinter", spec: "build it" },
});
write("payload-control", control.payloadSchema, { workstreamId: "ws-1", action: "pause" });
write("payload-retry-issue", retryIssue.payloadSchema, { issueId: "iss-1" });
write("payload-session-events", sessionEvents.payloadSchema, { sessionId: "ses-1" });
write("payload-session-send", sessionSend.payloadSchema, {
  sessionId: "ses-1",
  input: { text: "go", mode: "prompt" },
});
write("payload-interrupt", interrupt.payloadSchema, { sessionId: "ses-1" });
write("payload-answer-ui-request", answerUiRequest.payloadSchema, {
  sessionId: "ses-1",
  response: { requestId: "req-1", answer: { _tag: "Confirmed", confirmed: true } },
});

// ── Arbitrary JSON (Schema.Unknown tool payloads) ────────────────────────────
//
// The tool `input`/`output`/`partial` fields are `Schema.Unknown`: any JSON. This
// golden exercises the Swift `JSONValue` mirror across every JSON kind (null,
// bool, number, string, array, object), generated the same way as the rest.
write("json-values", Schema.Array(Schema.Unknown), [
  null,
  true,
  false,
  42,
  3.5,
  "text",
  [1, 2, 3],
  { key: "value", nested: { flag: null } },
]);

// ── Command response ─────────────────────────────────────────────────────────

// `createWorkstreamFromPlan` answers with a bare `WorkstreamId` (a JSON string).
write("response-workstream-id", createWorkstreamFromPlan.successSchema, "ws-1");

console.log(`Wrote goldens to ${goldensDir}`);
