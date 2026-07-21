/**
 * The GOLDEN-FRESHNESS gate (INV-MIRROR / INV-CONTRACT) — a stage of
 * `bun run check`.
 *
 * The goldens are the frozen wire output of the TypeScript contract, and the Swift
 * mirror is decode-tested against them. That only keeps the two sides in lockstep
 * while the committed fixtures ACTUALLY match what the contract emits today —
 * and nothing was checking that. `make check` merely DECODES the committed files
 * (it has no bun dependency, by design), so a contract change landed without
 * re-running the generator leaves both gates green while the Swift mirror is
 * validated against a wire shape the daemon no longer produces. The guard
 * INV-MIRROR names then guards nothing.
 *
 * This closes it: re-run `./generate-goldens.ts` into a TEMPORARY directory and
 * compare, byte for byte, against the committed ones. A difference — a changed
 * file, a new fixture the generator now writes, or a committed fixture the
 * generator no longer produces — fails with the exact command to fix it.
 *
 * It never writes into the working tree: the generator is pointed at a temp
 * directory that is removed on every exit path, so a failing run leaves the
 * repository exactly as it found it (a gate that "fixed" the tree as a side effect
 * would make the failure invisible in CI and dirty a developer's checkout).
 */
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const generator = join(here, "generate-goldens.ts");
const committedDir = join(here, "..", "Tests", "SprinterContractTests", "Goldens");

/** Every golden in a directory, as `name → contents`. */
const readGoldens = (dir: string): Map<string, string> =>
  new Map(
    readdirSync(dir)
      .filter((name) => name.endsWith(".json"))
      .sort()
      .map((name) => [name, readFileSync(join(dir, name), "utf8")]),
  );

const scratch = mkdtempSync(join(tmpdir(), "sprinter-goldens-"));
try {
  const generated = Bun.spawnSync(["bun", "run", generator, scratch], { stdout: "pipe" });
  if (generated.exitCode !== 0) {
    process.stderr.write(new TextDecoder().decode(generated.stderr));
    process.stderr.write("FAIL: the golden generator did not run.\n");
    process.exit(1);
  }

  const committed = readGoldens(committedDir);
  const fresh = readGoldens(scratch);

  const stale = [...fresh.keys()].filter((name) => committed.get(name) !== fresh.get(name));
  const orphaned = [...committed.keys()].filter((name) => !fresh.has(name));

  if (stale.length > 0 || orphaned.length > 0) {
    for (const name of stale) {
      process.stderr.write(
        committed.has(name)
          ? `  stale (contract output changed): ${name}\n`
          : `  missing (the generator writes it, the repo does not have it): ${name}\n`,
      );
    }
    for (const name of orphaned) {
      process.stderr.write(`  orphaned (the generator no longer writes it): ${name}\n`);
    }
    process.stderr.write(
      "FAIL: the committed goldens are not what the contract emits (INV-MIRROR).\n" +
        "THE FIX IS: bun run apple/Sprinter/scripts/generate-goldens.ts — then re-run the\n" +
        "Swift gate, because a wire-shape change needs its Swift mirror in the SAME change.\n",
    );
    process.exit(1);
  }

  process.stdout.write(`goldens: ${fresh.size} fixtures match the contract's output\n`);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
