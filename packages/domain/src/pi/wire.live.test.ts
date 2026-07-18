/**
 * Live validation against the REAL `pi --mode rpc` binary (FE2.2, INV-CONTRACT).
 *
 * Opt-in via the `SPRINTER_PI_BIN` config (an absolute path to `pi`), read through
 * Effect's `Config` (and thus the ambient `ConfigProvider`). When set, this spawns
 * that binary in `--mode rpc` via `effect/unstable/process`, drives real commands,
 * captures the actual NDJSON on stdout, and decodes the real bytes against our wire
 * schema — the Pi-drift gate. When unset (e.g. a runner with no pi) it logs and is a
 * green no-op.
 *
 * Bun + Effect only, zero `node:*`: config via `Config`, filesystem + process via
 * the Bun platform layer — imported DYNAMICALLY inside the run path. Its `bun`
 * builtin import is fine at run time under Bun but unresolvable when a runner merely
 * *collects* this file, so we never load it unless we actually run.
 *
 * WHAT IS VALIDATED LIVE: NDJSON framing + the response envelope (`get_state`, the
 * `abort` ack, and the `prompt` error/ack). A full model turn needs Pi provider
 * auth; those streaming shapes are covered by source-authored fixtures in
 * `wire.test.ts`. See the PR body for the live-vs-fixture split.
 */
import { it } from "@effect/vitest";
import { Config, Effect, FileSystem, Option, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { PiRpcResponse, PiServerMessage } from "./wire.ts";

type PiResponse = (typeof PiRpcResponse)["Type"];

const commands = [
  { id: "state", type: "get_state" },
  { id: "ack", type: "abort" },
  { id: "err", type: "prompt", message: "say hi" },
];
const stdin = `${commands.map((command) => JSON.stringify(command)).join("\n")}\n`;

const decodeServerMessage = Schema.decodeUnknownEffect(Schema.fromJsonString(PiServerMessage));
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(PiRpcResponse));

/** Spawn `pi --mode rpc` in a scoped temp cwd, drive the commands, collect `n` lines. */
const captureLines = (piBin: string, n: number) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "sprinter-pi-rpc-" });
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make(piBin, ["--mode", "rpc"], {
        cwd,
        // Keep stdin open so Pi stays alive; the scope kills it once we have the lines.
        stdin: { stream: Stream.make(stdin).pipe(Stream.encodeText), endOnDone: false },
      }),
    );
    return yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.take(n),
      Stream.runCollect,
    );
  });

/** Fail on the Effect channel (not via `throw`) if no driven command produced `label`. */
const requireResponse = (
  responses: ReadonlyArray<PiResponse>,
  lines: ReadonlyArray<string>,
  label: string,
  predicate: (response: PiResponse) => boolean,
) =>
  responses.some(predicate)
    ? Effect.void
    : Effect.fail(new Error(`no ${label} among ${JSON.stringify(lines)}`));

const validate = (piBin: string) =>
  Effect.gen(function* () {
    const lines = yield* captureLines(piBin, commands.length);

    // Framing + drift: every captured line must decode through the mirrored union.
    yield* Effect.forEach(lines, (line) => decodeServerMessage(line), { discard: true });

    // The response envelopes among the lines (an event line decodes to None here).
    const decoded = yield* Effect.forEach(lines, (line) =>
      decodeResponse(line).pipe(Effect.option),
    );
    const responses = decoded.filter(Option.isSome).map((some) => some.value);

    // Each command we drove must have produced its response — matched BY COMMAND
    // (order-independent) so reordering / an unsolicited future event never flips it.
    yield* requireResponse(
      responses,
      lines,
      "successful get_state response",
      (r) => r.command === "get_state" && r.success,
    );
    yield* requireResponse(
      responses,
      lines,
      "successful abort ack",
      (r) => r.command === "abort" && r.success,
    );
    yield* requireResponse(responses, lines, "prompt response", (r) => r.command === "prompt");
  });

it.live("decodes real `pi --mode rpc` NDJSON output against the wire schema", () =>
  Config.string("SPRINTER_PI_BIN").pipe(
    Config.option,
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.logInfo("SPRINTER_PI_BIN unset — skipping the live Pi drift validation"),
        // Load the Bun platform layer only now (see the file header): fine at run
        // time under Bun, but must not be loaded during CI collection.
        onSome: (piBin) =>
          Effect.promise(() => import("@effect/platform-bun")).pipe(
            Effect.flatMap((platformBun) =>
              validate(piBin).pipe(Effect.provide(platformBun.BunServices.layer)),
            ),
          ),
      }),
    ),
    Effect.timeout("30 seconds"),
  ),
);
