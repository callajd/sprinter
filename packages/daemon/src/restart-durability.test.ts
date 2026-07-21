/**
 * Durable-persistence restart proof (AE5.1, carried AE2 / #23 F2) — the restart
 * premise the whole {@link StartupReconcile} rests on: the Job↔execution↔PR mapping
 * survives a process restart.
 *
 * This is a real **build-write-rebuild-read** cycle against a FILE-backed
 * {@link StateStore} adapter (`layer({ filename })`) on a real tmpfile — NOT
 * `:memory:`, which proves nothing about a restart. The write goes through one
 * layer instance whose scope is then CLOSED (tearing down the SQLite connection —
 * the "process exit"); a FRESH layer is then built on the SAME file (the "restart")
 * and the durable rows are read back and asserted intact.
 *
 * The tmpfile lives under a scoped OS temp directory (`@effect/platform-bun`
 * `BunFileSystem` → `FileSystem.makeTempDirectoryScoped`, Bun-native, no bare
 * `node:*`), deleted when the test scope closes.
 */
import { it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import { FileSystem } from "effect/FileSystem";
import { BunFileSystem } from "@effect/platform-bun";
import { expect } from "vitest";
import {
  Agent,
  Epic,
  Execution,
  Issue,
  Job,
  PullRequestRef,
  Repository,
  Workstream,
} from "@sprinter/domain";
import { layer, StateStore } from "@sprinter/state";

const decode = <A, I>(schema: Schema.Codec<A, I>, raw: I): A =>
  Schema.decodeUnknownSync(schema)(raw);

/**
 * The repository the workstream fixture is anchored to — `repositoryId` is a real
 * FOREIGN KEY, so it has to be stored before anything references it.
 */
const repository = decode(Repository, {
  id: "repo:github:1296269",
  host: "github",
  owner: "callajd",
  name: "sprinter",
  refs: [{ name: "main", sha: "0123456789abcdef0123456789abcdef01234567" }],
  observedAt: "2026-07-20T12:00:00.000Z",
});
const workstream = decode(Workstream, {
  id: "ws-a",
  name: "Track A",
  repositoryId: "repo:github:1296269",
  status: "active",
  epics: ["epic-1"],
});

const epic = decode(Epic, {
  id: "epic-1",
  workstreamId: "ws-a",
  name: "Epic",
  status: "active",
  issues: ["issue-1"],
});

const pr = decode(PullRequestRef, {
  number: 10,
  url: "https://github.com/callajd/sprinter/pull/10",
  merged: true,
});

const issue = decode(Issue, {
  id: "issue-1",
  epicId: "epic-1",
  number: 1,
  title: "Issue 1",
  status: "in_review",
  dependsOn: [],
  pr,
});

const agent = decode(Agent, {
  id: "agt-1",
  name: "implementer",
  model: "claude-opus-4-8",
  version: "1.0.0",
  tools: ["read"],
});

const execution = decode(Execution, {
  id: "execution-job-1",
  jobId: "job-1",
  agentId: "agt-1",
  mode: "autonomous",
  transcript: { _tag: "SealedTranscript", lastOffset: 4 },
});

const job = decode(Job, {
  id: "job-1",
  issueId: "issue-1",
  kind: "implement",
  status: "running",
  executionId: "execution-job-1",
  transcriptRef: "transcript://execution-job-1",
  pr,
});

it.effect("the Job↔execution↔PR mapping survives a restart (build-write-rebuild-read)", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-restart-" });
    const filename = `${dir}/state.db`;

    // ── build + write, then TEAR DOWN the layer (the "process exit") ──────────
    yield* Effect.gen(function* () {
      const store = yield* StateStore;
      yield* store.repositories.putRepository(repository);
      yield* store.workGraph.putWorkstream(workstream);
      yield* store.workGraph.putEpic(epic);
      yield* store.workGraph.putIssue(issue);
      // The agent revision and the job row FIRST — both are foreign keys the execution
      // names, so this order is the constraint, not a convention (DE2.2).
      yield* store.agents.putAgent(agent);
      yield* store.jobs.putJob(job);
      yield* store.jobs.putExecution(execution);
    }).pipe(Effect.provide(layer({ filename })));

    // ── REBUILD a fresh layer on the SAME file (the "restart") + read back ────
    yield* Effect.gen(function* () {
      const store = yield* StateStore;

      // The Job row comes back intact — status, execution link, transcript ref, PR.
      const reloadedJob = Option.getOrThrow(yield* store.jobs.getJob(job.id));
      expect(reloadedJob).toStrictEqual(job);

      // The execution is still reachable both by id and through its job (which resolves
      // to the tree's ROOT), transcript seal and all.
      const byJob = Option.getOrThrow(yield* store.jobs.getExecutionForJob(job.id));
      expect(byJob).toStrictEqual(execution);
      const byId = Option.getOrThrow(yield* store.jobs.getExecution(execution.id));
      expect(byId).toStrictEqual(execution);

      // The Issue's PR ref survives the round-trip.
      const reloadedIssue = Option.getOrThrow(yield* store.workGraph.getIssue(issue.id));
      expect(reloadedIssue.pr).toStrictEqual(pr);
    }).pipe(Effect.provide(layer({ filename })));
  }).pipe(Effect.provide(BunFileSystem.layer)),
);
