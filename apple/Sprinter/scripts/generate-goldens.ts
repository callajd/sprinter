/**
 * The golden generator for the Swift contract mirror (issue FE2.4).
 *
 * Encodes REPRESENTATIVE values of every contract message schema through the
 * MERGED TypeScript contract (`@sprinter/contract` over `@sprinter/domain`),
 * writing the resulting wire JSON to
 * `Tests/SprinterContractTests/Goldens/*.json`. The Swift `SprinterContract`
 * mirror is decode-tested against these committed goldens, so it is validated
 * against REAL contract output rather than hand-typed JSON that could drift in
 * the same direction as the mirror (INV-CONTRACT).
 *
 * `make check` only DECODES the committed goldens (no bun dependency inside the
 * Swift gate), so the SWIFT side alone cannot notice a stale fixture. The ROOT gate
 * closes that hole: `bun run check:goldens` (`./check-goldens.ts`) re-runs this
 * generator into a temporary directory and diffs, so a contract change that was not
 * re-frozen fails CI rather than waiting to be caught by review — see
 * `docs/contract-mirror.md` (the INV-CONTRACT ripple procedure).
 *
 * Run from anywhere in the repo:
 *   bun run apple/Sprinter/scripts/generate-goldens.ts [outputDir]
 *
 * `outputDir` defaults to the committed `Tests/SprinterContractTests/Goldens`; the
 * freshness check passes a temp directory so it never touches the working tree.
 *
 * `Schema.encodeUnknownSync` VALIDATES its input against the schema before
 * encoding, so every representative value below is proven contract-valid as a
 * side effect of generation.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { assertGoldenCoverage, type WrittenGolden } from "./golden-coverage.ts";
import {
  Agent,
  Epic,
  Execution,
  ExecutionEvent,
  ExecutionInput,
  Issue,
  Job,
  PullRequestRef,
  Repository,
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
  executionEvents,
  ExecutionNotFound,
  executionSend,
  interrupt,
  IssueNotFound,
  OffsetEvent,
  OffsetExecutionEvent,
  PlanRejected,
  ResyncRequired,
  retryIssue,
  Snapshot,
  WorkGraphEvent,
  WorkstreamNotFound,
  WorkstreamPlan,
} from "../../../packages/contract/src/index.ts";

const here = dirname(fileURLToPath(import.meta.url));
/** Where the committed goldens live — the default target, and what the gate diffs against. */
export const COMMITTED_GOLDENS_DIR = join(here, "..", "Tests", "SprinterContractTests", "Goldens");
const goldensDir = process.argv[2] ?? COMMITTED_GOLDENS_DIR;
mkdirSync(goldensDir, { recursive: true });

/**
 * Every golden written by this run, with the schema it went through — the input to the
 * ABSENT-FORM guard at the bottom of the file (`assertGoldenCoverage`, issue #89 N3).
 */
const written: WrittenGolden[] = [];

/** Write already-encoded wire JSON, and register it for the coverage census. */
const writeEncoded = <S extends Schema.Codec<unknown, unknown>>(
  name: string,
  schema: S,
  encoded: unknown,
): void => {
  writeFileSync(join(goldensDir, `${name}.json`), `${JSON.stringify(encoded, null, 2)}\n`);
  written.push({ name, ast: schema.ast, encoded });
};

/** Encode a value through its schema (validating it) and write the wire JSON. */
const write = <S extends Schema.Codec<unknown, unknown>>(
  name: string,
  schema: S,
  value: unknown,
): void => {
  writeEncoded(name, schema, Schema.encodeUnknownSync(schema)(value));
};

// ── Owned domain node fixtures (also embedded in the snapshot) ───────────────

const prMerged = { number: 15, url: "https://github.com/callajd/sprinter/pull/15", merged: true };
const prOpen = { number: 16, url: "https://github.com/callajd/sprinter/pull/16", merged: false };

const issueWithPr = {
  id: "iss-1",
  epicId: "ep-1",
  number: 10,
  title: "RPC contract mirror",
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
  executionId: "exe-1",
  transcriptRef: "transcripts/exe-1.jsonl",
  pr: prMerged,
};
const jobMinimal = { id: "job-2", issueId: "iss-2", kind: "review", status: "queued" };

// ── Execution fixtures (DE2.2) ───────────────────────────────────────────────
//
// TWO of them, and the pair is deliberate — between them they pin every shape the
// entity has: the ROOT execution OMITS `parent` (the absent form the #89 census needs)
// and runs AUTONOMOUS with a LIVE transcript; the CHILD CARRIES `parent`, runs
// INTERACTIVE, and carries a SEALED transcript with its extent. So each `mode` literal
// and each transcript variant appears in the corpus, and `parent` appears in both its
// present and its omitted form.
//
// `agentId` REFERENCES a revision in the registry fixtures below — the ROOT names
// `agt-1` and the CHILD names `agt-2`, so the corpus pins that DIFFERENT executions in
// ONE job may run DIFFERENT agent revisions rather than a job-wide one. An execution
// names the revision of the run it records, and the reference is a real foreign key in
// the store.
const execution = {
  id: "exe-1",
  jobId: "job-1",
  agentId: "agt-1",
  mode: "autonomous",
  transcript: { _tag: "LiveTranscript" },
};
const executionChild = {
  id: "exe-2",
  jobId: "job-1",
  agentId: "agt-2",
  parent: "exe-1",
  mode: "interactive",
  transcript: { _tag: "SealedTranscript", lastOffset: 12 },
};

// ── Registry fixtures (DE1.1) ────────────────────────────────────────────────
//
// The registry is APPEND-ONLY and GLOBAL, and a stored revision is IMMUTABLE, so
// BOTH mutating operations are an append of a NEW id: `agentRevised` is an EDIT —
// a new revision linked to the one it replaced by `supersedes` — and `agentRetired`
// is a RETIREMENT — a new revision carrying `supersedes` AND the `retiredAt` stamp
// (retired-ness is the stamp's presence; there is no status enum, INV-SUM).
// `agentOriginal` exercises BOTH optional keys ABSENT. No agent names a repository:
// "agents used in this repo" is a fold over that repo's executions (INV-DERIVED).
const agentOriginal = {
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read", "edit", "bash"],
};
const agentRevised = {
  id: "agt-2",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.1.0",
  tools: ["read", "edit", "bash", "web_search"],
  supersedes: "agt-1",
};
// A retirement is LIFECYCLE-ONLY: it carries the SAME name/model/version/tools as
// the head it retires, and differs ONLY in `id`, `supersedes` and `retiredAt`. So
// this fixture is `agentRevised`'s content verbatim plus the stamp — spelled as a
// spread so it CANNOT drift. (It is also what the `StateStore` port now enforces: a
// retiring revision that rewrote content would be rejected, so a fixture showing one
// would teach a wire shape the daemon will not produce.)
const agentRetired = {
  ...agentRevised,
  id: "agt-3",
  supersedes: "agt-2",
  retiredAt: "2026-07-20T12:00:00.000Z",
};

const epic = {
  id: "ep-1",
  workstreamId: "ws-1",
  name: "FE2",
  status: "active",
  issues: ["iss-1", "iss-2"],
};
// ── STATE-layer fixture (DE1.2) ──────────────────────────────────────────────
//
// A `Repository` is REFERENCED, not owned — it is a snapshot of something on a code
// host — so it carries `observedAt` (INV-OBSERVED), which the owned nodes never do.
// Its `refs` are the OBSERVED ref map, ordered by branch name; an EMPTY one is valid
// ("nothing observed yet"), and `repository-no-refs` pins that shape too.
const repository = {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [
    { name: "feat/x-1", sha: "89abcdef0123456789abcdef0123456789abcdef" },
    { name: "main", sha: "0123456789abcdef0123456789abcdef01234567" },
  ],
  observedAt: "2026-07-20T12:00:00.000Z",
};

const workstream = {
  id: "ws-1",
  name: "Foundation",
  // A REFERENCE to the repository above, not a bare string: the two cannot disagree
  // about which repository this workstream is bound to.
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["ep-1"],
};

// ── Snapshot (hydration) ─────────────────────────────────────────────────────

/**
 * A fixed STORE GENERATION for the fixtures. The real one is minted per store
 * (a UUID from `createSchema`); the goldens pin the SHAPE, so a stable literal keeps
 * them byte-reproducible — the property `check-goldens.ts` gates on.
 */
const generation = "8f0d0a3e-4a7a-4a2e-9b5e-0f2c1d3e4a5b";

write("snapshot", Snapshot, {
  // The STATE layer: every repository the daemon has observed. It is hydrated because
  // `Workstream.repositoryId` is a REFERENCE — without it a client could resolve none
  // of them, nor render how stale each observation is.
  repositories: [repository],
  workstreams: [workstream],
  epics: [epic],
  issues: [issueWithPr, issueNoPr],
  jobs: [jobFull, jobMinimal],
  executions: [execution, executionChild],
  // BYTE order by id (SQLite's default BINARY collation) — the order `listAgents`
  // pins and the daemon hydrates in.
  // It is presentational (a client upserts by id); a lineage is read off
  // `supersedes`, never off this order.
  agents: [agentOriginal, agentRevised, agentRetired],
  // The coordinate space these offsets live in. A client retains it with the state and
  // hands it back on every cursor-bearing request, so a cursor from a destroyed
  // generation is refused rather than silently resumed (INV-FRESH).
  generation,
});

// Individual owned nodes (exercise every DTO + optional-key present/absent).
write("repository", Repository, repository);
// An EMPTY ref set is a VALID observation — "nothing observed yet", not a malformed
// record — so the mirror must decode it as an empty list rather than a missing key.
write("repository-no-refs", Repository, { ...repository, refs: [] });
write("workstream", Workstream, workstream);
write("epic", Epic, epic);
write("issue-with-pr", Issue, issueWithPr);
write("issue-no-pr", Issue, issueNoPr);
write("job-full", Job, jobFull);
write("job-minimal", Job, jobMinimal);
write("execution", Execution, execution);
write("execution-child", Execution, executionChild);
write("agent-original", Agent, agentOriginal);
write("agent-revised", Agent, agentRevised);
write("agent-retired", Agent, agentRetired);
write("pull-request-ref", PullRequestRef, prMerged);

// The distinct terminal `cancelled` WorkStatus (CE5.1) — a cancelled
// planning node is terminal-but-not-`done`, so the mirror + board render it apart.
write("workstream-cancelled", Workstream, { ...workstream, status: "cancelled" });
write("epic-cancelled", Epic, { ...epic, status: "cancelled" });

// ── WorkGraphEvent — every variant ───────────────────────────────────────────

write("work-graph-events", Schema.Array(WorkGraphEvent), [
  { _tag: "RepositoryChanged", repository },
  { _tag: "WorkstreamChanged", workstream },
  { _tag: "EpicChanged", epic },
  { _tag: "IssueChanged", issue: issueWithPr },
  { _tag: "JobChanged", job: jobFull },
  { _tag: "ExecutionChanged", execution },
  { _tag: "AgentChanged", agent: agentRevised },
]);

// ── OffsetEvent — the streamed `events` success envelope (CE2.0) ──
//
// Each streamed item pairs a WorkGraphEvent with its DURABLE offset, so a client
// can feed the offset back as the request's `sinceOffset` cursor. Real `event_log`
// offsets are 1-based (> 0), so the sample resumes from a mid-log position (3…6)
// rather than the origin — the strict `> sinceOffset` ordering is the durable-replay
// slice a reconnect resumes from.

write("offset-events", Schema.Array(OffsetEvent), [
  { offset: 3, event: { _tag: "WorkstreamChanged", workstream } },
  { offset: 4, event: { _tag: "IssueChanged", issue: issueWithPr } },
  { offset: 5, event: { _tag: "ExecutionChanged", execution } },
  { offset: 6, event: { _tag: "AgentChanged", agent: agentRetired } },
]);

// ── OffsetExecutionEvent — the streamed `executionEvents` success envelope ───────────────────
//
// The ONE channel serves BOTH modalities: DURABLE transcript-grade events carry a per-execution
// `offset` (replayable; the client feeds it back as `sinceOffset`), while EPHEMERAL live deltas
// ride the SAME channel OFFSET-LESS (the `offset` key is omitted). The sample interleaves both:
// the durable entries resume from a mid-transcript position (2,3,4), and the ephemeral deltas
// (TurnStarted, MessageDelta, UiRequestRaised) prove the mirror decodes a missing `offset` to
// `nil` and still surfaces the event.

write("offset-execution-events", Schema.Array(OffsetExecutionEvent), [
  // Ephemeral live delta — NO `offset` key (turn lifecycle).
  { event: { _tag: "TurnStarted" } },
  {
    offset: 2,
    event: {
      _tag: "EntryAppended",
      entry: { _tag: "AssistantMessage", id: "a1", text: "on it", reasoning: "planning" },
    },
  },
  // Ephemeral message partial — offset-less.
  { event: { _tag: "MessageDelta", messageId: "a1", text: "on " } },
  { offset: 3, event: { _tag: "Notice", id: "notice-disk", level: "warn", message: "disk low" } },
  // Ephemeral interactive request — offset-less.
  { event: { _tag: "UiRequestRaised", id: "u1", kind: "confirm", prompt: "proceed?" } },
  {
    offset: 4,
    event: {
      _tag: "EntryAppended",
      entry: { _tag: "ToolResult", id: "c1", output: 3, isError: false },
    },
  },
]);

// ── ExecutionEvent — every variant (+ optional present/absent) ───────────────────────────────

const usageFull = {
  inputTokens: 1200,
  outputTokens: 340,
  cacheReadTokens: 800,
  cacheWriteTokens: 64,
};
const usageMinimal = { inputTokens: 10, outputTokens: 5 };

write("execution-events", Schema.Array(ExecutionEvent), [
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
  { _tag: "ExecutionIdle" },
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
  // is ABSENT (CE5.2). The consumer keys these by arrival sequence so
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

// ── ExecutionInput — every mode (+ images present/absent) ────────────────────────────────────

write("execution-inputs", Schema.Array(ExecutionInput), [
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

// The plan carries the repository's NATURAL KEY, not an id: it is composed by a
// client that has never seen a `RepositoryId` (D6). The daemon resolves the key
// through the `CodeHost` port, and refuses a plan the host does not recognise.
write("workstream-plan", WorkstreamPlan, {
  name: "Foundation",
  repository: { host: "github", owner: "callajd", name: "sprinter" },
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
  Schema.encodeUnknownSync(ExecutionNotFound)(
    Schema.decodeUnknownSync(ExecutionNotFound)({ _tag: "ExecutionNotFound", id: "exe-9" }),
  ),
  Schema.encodeUnknownSync(PlanRejected)(
    Schema.decodeUnknownSync(PlanRejected)({ _tag: "PlanRejected", reason: "empty spec" }),
  ),
  // The resume refusal, shared by BOTH cursor-bearing feeds (`events` and
  // `executionEvents`): the client's cursor is not from the daemon's current store
  // generation, so it must discard its retained state and re-hydrate from `snapshot`.
  // NOTE the cursor here is WITHIN the log's extent (2 <= 3) — the case an offset-only
  // rule cannot detect, and the reason the generation is an explicit identity. The
  // `generation` field names the daemon's CURRENT one.
  Schema.encodeUnknownSync(ResyncRequired)(
    Schema.decodeUnknownSync(ResyncRequired)({
      _tag: "ResyncRequired",
      sinceOffset: 2,
      maxOffset: 3,
      generation,
    }),
  ),
];
// Written from the ALREADY-ENCODED objects (they were encoded one schema at a time
// above), but registered against the union of the five error schemas so the coverage
// census sees every variant — an error case that stopped being represented here would
// fail generation rather than quietly leave the mirror's variant unexercised.
writeEncoded(
  "contract-errors",
  Schema.Array(
    Schema.Union([
      WorkstreamNotFound,
      IssueNotFound,
      ExecutionNotFound,
      PlanRejected,
      ResyncRequired,
    ]),
  ),
  encodedErrors,
);

// ── Command payloads (the wire the daemon receives) ──────────────────────────

// The `events` request RESUME CONTEXT is OPTIONAL (CE2.0): both wire forms are
// captured — present (a `resume` object carrying BOTH coordinates) and absent (key
// omitted, the origin replay) — so the Swift mirror decodes each. The cursor and its
// generation are ONE nested value, so "a cursor without its generation" has no wire
// form to freeze: absence of `resume` is the only origin request there is.
write("payload-events", events.payloadSchema, { resume: { sinceOffset: 12, generation } });
write("payload-events-no-offset", events.payloadSchema, {});
write("payload-create-workstream-from-plan", createWorkstreamFromPlan.payloadSchema, {
  plan: {
    name: "Foundation",
    repository: { host: "github", owner: "callajd", name: "sprinter" },
    spec: "build it",
  },
});
write("payload-control", control.payloadSchema, { workstreamId: "ws-1", action: "pause" });
write("payload-retry-issue", retryIssue.payloadSchema, { issueId: "iss-1" });
// The `executionEvents` request resume context is OPTIONAL, and is the SAME nested
// value `events` carries: both wire forms are captured — present and absent (the
// origin-replay case) — so the Swift mirror decodes each.
write("payload-execution-events", executionEvents.payloadSchema, {
  executionId: "exe-1",
  resume: { sinceOffset: 12, generation },
});
write("payload-execution-events-no-offset", executionEvents.payloadSchema, {
  executionId: "exe-1",
});
write("payload-execution-send", executionSend.payloadSchema, {
  executionId: "exe-1",
  input: { text: "go", mode: "prompt" },
});
write("payload-interrupt", interrupt.payloadSchema, { executionId: "exe-1" });
write("payload-answer-ui-request", answerUiRequest.payloadSchema, {
  executionId: "exe-1",
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

// ── The ABSENT-FORM guard (issue #89, finding N3) ────────────────────────────
//
// Runs AFTER every fixture is written, over the schemas they were written through: each
// `Schema.optionalKey` reachable from a golden must be CARRIED by one golden and OMITTED
// by another, and each tagged-union case must appear somewhere. A new optional field or
// union case therefore cannot land without a fixture that pins it — the check is derived
// from the contract, not from a hand-kept list that goes stale.
assertGoldenCoverage(written);

// `process.stdout.write`, not `console.log`: this script is inside the lint gate
// (`check:lint` covers `apple/Sprinter/scripts`), and it matches how `check-goldens.ts`
// already reports — one way of writing to the terminal across both scripts.
process.stdout.write(`Wrote goldens to ${goldensDir}\n`);
