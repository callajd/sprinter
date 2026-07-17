/**
 * Dependency-pin guard (INV-PIN, decision D7).
 *
 * Fails the `check` gate if any `package.json` manifest declares a dependency
 * with a non-exact range (`^`, `~`, `>`, `<`, `*`, `x`, or a `-` range). Every
 * external dependency and tool must be exact-pinned; the committed `bun.lock`
 * plus `save-exact` in `bunfig.toml` keep it that way.
 */
import { Glob } from "bun";

const DEP_FIELDS = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

const EXACT_SEMVER = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/u;

interface Violation {
  readonly manifest: string;
  readonly field: string;
  readonly name: string;
  readonly range: string;
}

const manifestSchema = new Glob("**/package.json");

const violations: Array<Violation> = [];

for await (const path of manifestSchema.scan({ cwd: ".", absolute: false })) {
  if (path.includes("node_modules")) continue;

  const file = Bun.file(path);
  const parsed: unknown = await file.json();
  if (typeof parsed !== "object" || parsed === null) continue;

  for (const field of DEP_FIELDS) {
    const record = Reflect.get(parsed, field);
    if (typeof record !== "object" || record === null) continue;

    for (const name of Object.keys(record)) {
      const range = Reflect.get(record, name);
      if (typeof range !== "string") continue;

      // Workspace protocol links are allowed; anything else must be exact.
      if (range.startsWith("workspace:")) continue;

      if (!EXACT_SEMVER.test(range)) {
        violations.push({ manifest: path, field, name, range });
      }
    }
  }
}

if (violations.length > 0) {
  const lines = violations.map((v) => `  ${v.manifest} > ${v.field} > ${v.name}: "${v.range}"`);
  process.stderr.write(
    [
      "Dependency pin violations (INV-PIN): non-exact ranges found",
      "",
      ...lines,
      "",
      "Pin every dependency to an exact version (no ^ / ~ / ranges).",
      "",
    ].join("\n"),
  );
  process.exitCode = 1;
} else {
  process.stdout.write("check:pins — all dependencies are exact-pinned\n");
}
