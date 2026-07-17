/**
 * Dependency- and toolchain-pin guard (INV-PIN, decision D7).
 *
 * Effect-native CLI: enumerates every committed `package.json`, decodes each
 * through a `Schema` (safe parsing — no `JSON.parse`/`any`) read via the
 * `FileSystem` port (Bun adapter), and fails the `check` gate on the Effect
 * error channel — a typed `PinCheckError` surfaced by `BunRuntime.runMain` as a
 * non-zero exit — if any dependency range is non-exact, or the Bun toolchain
 * pins (`.bun-version` vs `package.json#packageManager`) disagree or drift.
 */
import { BunFileSystem, BunRuntime } from "@effect/platform-bun";
import { Glob } from "bun";
import { Console, Effect, FileSystem, Schema } from "effect";

const EXACT_SEMVER = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/u;
const DEP_FIELDS = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

/** Gate failure carrying the human-readable pin report (INV-PIN). */
class PinCheckError extends Schema.TaggedErrorClass<PinCheckError>()("PinCheckError", {
  report: Schema.String,
}) {}

const DependencyMap = Schema.Record(Schema.String, Schema.String);

/** The slice of a `package.json` the pin guard reads; excess keys are ignored. */
const Manifest = Schema.Struct({
  packageManager: Schema.optional(Schema.String),
  dependencies: Schema.optional(DependencyMap),
  devDependencies: Schema.optional(DependencyMap),
  peerDependencies: Schema.optional(DependencyMap),
  optionalDependencies: Schema.optional(DependencyMap),
});

const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(Manifest));

/** Normalise any read/parse/decode failure for `path` into a `PinCheckError`. */
const failWith =
  (path: string) =>
  (cause: unknown): PinCheckError =>
    cause instanceof PinCheckError
      ? cause
      : new PinCheckError({ report: `  ${path}: ${String(cause)}` });

/** Enumerate committed `package.json` manifests, skipping installed dependencies. */
const scanManifests = Effect.tryPromise({
  try: async () => {
    const paths: Array<string> = [];
    for await (const path of new Glob("**/package.json").scan({ cwd: ".", absolute: false })) {
      if (!path.includes("node_modules")) paths.push(path);
    }
    return paths;
  },
  catch: (cause) => new PinCheckError({ report: `Failed to scan manifests: ${String(cause)}` }),
});

/** Read + decode one manifest through the FileSystem port. */
const readManifest = (
  fs: FileSystem.FileSystem,
  path: string,
): Effect.Effect<(typeof Manifest)["Type"], PinCheckError> =>
  fs.readFileString(path).pipe(Effect.flatMap(decodeManifest), Effect.mapError(failWith(path)));

/** Non-exact dependency ranges declared by one manifest. */
const depViolations = (manifest: string, parsed: (typeof Manifest)["Type"]): Array<string> => {
  const out: Array<string> = [];
  for (const field of DEP_FIELDS) {
    const record = parsed[field];
    if (record === undefined) continue;
    for (const [name, range] of Object.entries(record)) {
      if (range.startsWith("workspace:")) continue;
      if (!EXACT_SEMVER.test(range)) out.push(`  ${manifest} > ${field} > ${name}: "${range}"`);
    }
  }
  return out;
};

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const dependencyErrors: Array<string> = [];
  for (const manifest of yield* scanManifests) {
    dependencyErrors.push(...depViolations(manifest, yield* readManifest(fs, manifest)));
  }

  // Toolchain pins: `.bun-version` and `package.json#packageManager` must agree and be exact.
  const bunVersion = (yield* fs
    .readFileString(".bun-version")
    .pipe(Effect.mapError(failWith(".bun-version")))).trim();
  const root = yield* readManifest(fs, "package.json");

  const toolchainErrors: Array<string> = [];
  if (!EXACT_SEMVER.test(bunVersion)) {
    toolchainErrors.push(`  .bun-version: "${bunVersion}" is not an exact version`);
  }
  if (root.packageManager !== `bun@${bunVersion}`) {
    const shown = root.packageManager === undefined ? "undefined" : `"${root.packageManager}"`;
    toolchainErrors.push(
      `  package.json > packageManager: ${shown} must equal "bun@${bunVersion}" (.bun-version)`,
    );
  }

  if (dependencyErrors.length > 0 || toolchainErrors.length > 0) {
    const sections: Array<string> = [];
    if (dependencyErrors.length > 0) {
      sections.push(
        "Dependency pin violations (INV-PIN): non-exact ranges found",
        "",
        ...dependencyErrors,
        "",
      );
    }
    if (toolchainErrors.length > 0) {
      sections.push("Toolchain pin violations (INV-PIN):", "", ...toolchainErrors, "");
    }
    return yield* Effect.fail(new PinCheckError({ report: sections.join("\n") }));
  }

  yield* Console.log(
    `check:pins — dependencies exact-pinned; toolchain pinned to bun@${bunVersion}`,
  );
});

BunRuntime.runMain(
  program.pipe(
    Effect.tapError((error) => Console.error(error.report)),
    Effect.provide(BunFileSystem.layer),
  ),
);
