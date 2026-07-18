/**
 * Live validation against the REAL `pi --mode rpc` binary (FE2.2, INV-CONTRACT).
 *
 * This spawns the installed `pi` (v0.80.10) in `--mode rpc` via Effect's
 * `effect/unstable/process` `ChildProcessSpawner`, writes real `RpcCommand`s to
 * stdin, captures the actual NDJSON emitted on stdout (split strictly on `\n`),
 * and asserts our `Schema.decode` accepts the ACTUAL bytes — not a hand-made
 * fixture. If Pi's real shape drifts from what we mirror, this test fails.
 *
 * WHAT IS VALIDATED LIVE HERE: the NDJSON framing and the response envelope —
 * `get_state` (`RpcSessionState`), an async command ack (`abort`), and the
 * universal error response (`prompt` with no provider auth). Driving a full
 * model turn (streaming `AgentSessionEvent` message/tool/turn events) requires
 * Pi provider auth (`~/.pi/agent/auth.json`), which is NOT assumed present, so
 * those streaming shapes are covered by source-authored fixtures in
 * `wire.test.ts` instead. See the PR body for the live-vs-fixture split.
 */
import { existsSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { BunServices } from "@effect/platform-bun";
import { it } from "@effect/vitest";
import { Effect, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { expect } from "vitest";
import { PiRpcResponse, PiServerMessage } from "./wire.ts";

const piBin = join(homedir(), ".bun", "bin", "pi");
const hasPi = existsSync(piBin);

const commands = [
  { id: "state", type: "get_state" },
  { id: "ack", type: "abort" },
  { id: "err", type: "prompt", message: "say hi" },
];
const stdinBytes = new TextEncoder().encode(
  `${commands.map((command) => JSON.stringify(command)).join("\n")}\n`,
);

/** Spawn `pi --mode rpc`, drive the commands, and collect the first `n` stdout lines. */
const captureLines = (cwd: string, n: number) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const command = ChildProcess.make(piBin, ["--mode", "rpc"], {
      cwd,
      // Keep stdin open so Pi stays alive; the test scope kills it once we have
      // the lines we need.
      stdin: { stream: Stream.make(stdinBytes), endOnDone: false },
    });
    const handle = yield* spawner.spawn(command);
    return yield* handle.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.take(n),
      Stream.runCollect,
    );
  });

it.live.skipIf(!hasPi)("decodes real `pi --mode rpc` NDJSON output against the wire schema", () =>
  Effect.gen(function* () {
    const cwd = mkdtempSync(join(tmpdir(), "sprinter-pi-rpc-"));
    const lines = yield* captureLines(cwd, commands.length);

    // NDJSON framing: exactly one record per command, split on `\n`.
    expect(lines.length).toBe(commands.length);

    // Every real line decodes through the top-level server-message union.
    for (const line of lines) {
      yield* Schema.decodeUnknownEffect(PiServerMessage)(JSON.parse(line));
    }

    const [stateLine, ackLine, promptLine] = lines;

    // get_state → a real `RpcSessionState` snapshot.
    const state = yield* Schema.decodeUnknownEffect(PiRpcResponse)(JSON.parse(stateLine ?? ""));
    if (!(state.type === "response" && state.success && state.command === "get_state")) {
      throw new Error(`expected a successful get_state response, got ${stateLine}`);
    }
    expect(typeof state.data.sessionId).toBe("string");
    expect(state.data.sessionId.length).toBeGreaterThan(0);

    // abort → a real async command ack.
    const ack = yield* Schema.decodeUnknownEffect(PiRpcResponse)(JSON.parse(ackLine ?? ""));
    expect(ack.type).toBe("response");
    if (!(ack.type === "response" && ack.command === "abort" && ack.success)) {
      throw new Error(`expected a successful abort ack, got ${ackLine}`);
    }

    // prompt → a real response envelope for the "prompt" command. Without
    // provider auth this is the error variant; with auth it is the success
    // ack. Either is a valid mirrored `RpcResponse` for command "prompt".
    const prompt = yield* Schema.decodeUnknownEffect(PiRpcResponse)(JSON.parse(promptLine ?? ""));
    expect(prompt.type).toBe("response");
    if (prompt.type === "response") {
      expect(prompt.command).toBe("prompt");
    }
  }).pipe(Effect.provide(BunServices.layer), Effect.timeout("30 seconds")),
);
