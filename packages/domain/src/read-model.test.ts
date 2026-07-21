import { it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import { expect } from "vitest";
import {
  Epic,
  isComplete,
  Issue,
  isIssueLanded,
  isTerminal,
  isExecutionLive,
  Job,
  PullRequestRef,
  Execution,
  Workstream,
} from "./read-model.ts";

/** Decode `raw`, re-encode, and assert the encoded value equals the input (round-trip). */
const assertRoundTrip = (schema: Schema.Codec<unknown, unknown>, raw: unknown) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknownEffect(schema)(raw);
    const encoded = yield* Schema.encodeUnknownEffect(schema)(decoded);
    expect(encoded).toStrictEqual(raw);
  });

const workstream = {
  id: "ws-fdn",
  name: "Foundation",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["epic-fe1", "epic-fe2"],
};

const epic = {
  id: "epic-fe2",
  workstreamId: "ws-fdn",
  name: "Contract stack",
  status: "pending",
  issues: ["issue-8"],
};

const issueWithoutPr = {
  id: "issue-8",
  epicId: "epic-fe2",
  number: 8,
  title: "FE2.1 — Domain + neutral execution schemas",
  status: "in_progress",
  dependsOn: [],
};

const issueWithPr = {
  ...issueWithoutPr,
  status: "done",
  dependsOn: ["issue-7"],
  pr: { number: 42, url: "https://github.com/callajd/sprinter/pull/42", merged: true },
};

const jobMinimal = {
  id: "job-1",
  issueId: "issue-8",
  kind: "implement",
  status: "queued",
};

const jobFull = {
  ...jobMinimal,
  status: "succeeded",
  executionId: "exe-1",
  transcriptRef: "transcripts/exe-1.jsonl",
  pr: { number: 42, url: "https://github.com/callajd/sprinter/pull/42", merged: false },
};

/** A ROOT execution, still running: no `parent`, and an OPEN transcript. */
const execution = {
  id: "exe-1",
  jobId: "job-1",
  agentId: "agt-1",
  mode: "autonomous",
  transcript: { _tag: "LiveTranscript" },
};

/** A SUBAGENT's execution: it names its parent, holds the turn interactively, and has settled. */
const childExecution = {
  ...execution,
  id: "exe-2",
  parent: "exe-1",
  mode: "interactive",
  transcript: { _tag: "SealedTranscript", lastOffset: 12 },
};

it.effect("round-trips the whole read model (with and without optional keys)", () =>
  Effect.gen(function* () {
    yield* assertRoundTrip(Workstream, workstream);
    yield* assertRoundTrip(Epic, epic);
    yield* assertRoundTrip(Issue, issueWithoutPr);
    yield* assertRoundTrip(Issue, issueWithPr);
    yield* assertRoundTrip(PullRequestRef, issueWithPr.pr);
    yield* assertRoundTrip(Job, jobMinimal);
    yield* assertRoundTrip(Job, jobFull);
    yield* assertRoundTrip(Execution, execution);
    yield* assertRoundTrip(Execution, childExecution);
  }),
);

it.effect("rejects representative invalid inputs", () =>
  Effect.gen(function* () {
    const invalids: ReadonlyArray<readonly [Schema.Codec<unknown, unknown>, unknown]> = [
      [Workstream, { ...workstream, status: "nope" }],
      [Workstream, { ...workstream, name: "" }],
      [Workstream, { ...workstream, epics: [""] }],
      [Epic, { ...epic, workstreamId: "" }],
      [Issue, { ...issueWithoutPr, number: 0 }],
      [Issue, { ...issueWithoutPr, number: 1.5 }],
      [Issue, { ...issueWithoutPr, status: "merged" }],
      [Job, { ...jobMinimal, kind: "unknown-kind" }],
      [Job, { ...jobMinimal, status: "done" }],
      // `mode` is a CLOSED literal set, not a string.
      [Execution, { ...execution, mode: "supervised" }],
      // A transcript is one of exactly two variants…
      [Execution, { ...execution, transcript: { _tag: "PartialTranscript" } }],
      // …and a SEALED one carries its extent as REQUIRED payload — there is no sealed
      // transcript without a `lastOffset`, and no flag standing in for the variant
      // (INV-SUM).
      [Execution, { ...execution, transcript: { _tag: "SealedTranscript" } }],
      // Every reference is required and branded: an execution with no agent is not a
      // shape the domain has (its `agentId` is a foreign key in the store).
      [Execution, { ...execution, agentId: "" }],
      [Execution, { ...execution, parent: "" }],
      [PullRequestRef, { number: -1, url: "x", merged: true }],
    ];
    yield* Effect.forEach(invalids, ([schema, raw]) =>
      Effect.exit(Schema.decodeUnknownEffect(schema)(raw)).pipe(
        Effect.map((exit) => expect(Exit.isFailure(exit)).toBe(true)),
      ),
    );
  }),
);

it.effect("reads liveness off the TRANSCRIPT — the model's only expression of it", () =>
  Effect.gen(function* () {
    const running = yield* Schema.decodeUnknownEffect(Execution)(execution);
    const settled = yield* Schema.decodeUnknownEffect(Execution)(childExecution);
    expect(isExecutionLive(running)).toBe(true);
    expect(isExecutionLive(settled)).toBe(false);
    // The subagent's tree edge and its per-execution mode survive the decode: `mode`
    // lives HERE and nowhere above (INV-MODE), and a child names its parent.
    expect(settled.parent).toBe("exe-1");
    expect(settled.mode).toBe("interactive");
    expect(running.mode).toBe("autonomous");
    // A root carries no `parent` KEY at all — absent, never null.
    expect("parent" in running).toBe(false);
  }),
);

it.effect("decodes a valid workstream", () =>
  Effect.gen(function* () {
    const ws = yield* Schema.decodeUnknownEffect(Workstream)(workstream);
    expect(ws.name).toBe("Foundation");
    expect(ws.epics).toHaveLength(2);
  }),
);

it.effect("isComplete tracks the terminal done status", () =>
  Effect.gen(function* () {
    const active = yield* Schema.decodeUnknownEffect(Workstream)(workstream);
    const done = yield* Schema.decodeUnknownEffect(Epic)({ ...epic, status: "done" });
    expect(isComplete(active)).toBe(false);
    expect(isComplete(done)).toBe(true);
  }),
);

it.effect("decodes the distinct terminal cancelled WorkStatus (CE5.1)", () =>
  Effect.gen(function* () {
    yield* assertRoundTrip(Workstream, { ...workstream, status: "cancelled" });
    yield* assertRoundTrip(Epic, { ...epic, status: "cancelled" });
  }),
);

it.effect("isTerminal covers done AND cancelled; isComplete stays done-only", () =>
  Effect.gen(function* () {
    const active = yield* Schema.decodeUnknownEffect(Workstream)(workstream);
    const done = yield* Schema.decodeUnknownEffect(Epic)({ ...epic, status: "done" });
    const cancelled = yield* Schema.decodeUnknownEffect(Epic)({ ...epic, status: "cancelled" });
    expect(isTerminal(active)).toBe(false);
    expect(isTerminal(done)).toBe(true);
    expect(isTerminal(cancelled)).toBe(true);
    // cancelled is terminal-but-not-done.
    expect(isComplete(cancelled)).toBe(false);
  }),
);

it.effect("isIssueLanded requires a merged PR and done status", () =>
  Effect.gen(function* () {
    const landed = yield* Schema.decodeUnknownEffect(Issue)(issueWithPr);
    const open = yield* Schema.decodeUnknownEffect(Issue)(issueWithoutPr);
    const doneNoPr = yield* Schema.decodeUnknownEffect(Issue)({
      ...issueWithoutPr,
      status: "done",
    });
    expect(isIssueLanded(landed)).toBe(true);
    expect(isIssueLanded(open)).toBe(false);
    expect(isIssueLanded(doneNoPr)).toBe(false);
  }),
);
