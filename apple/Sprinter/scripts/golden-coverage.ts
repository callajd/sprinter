/**
 * The ABSENT-FORM guard for the golden corpus (issue #89, finding N3).
 *
 * The encode-agreement harness can only catch a mirror that emits `"k": null` where the
 * contract OMITS the key if some golden actually OMITS that key. Which goldens exist was
 * a matter of PROSE — `GoldenCase.swift` listed, by hand, the optional-bearing types and
 * the fixtures pinning their present/absent forms — and prose does not fail a build. A new
 * `Schema.optionalKey` field whose fixture always populates it would leave the whole
 * omission-vs-`null` question unasked for that field, silently, while every gate stayed
 * green. Epics DE2–DE4 add `Execution`, `Session`, `Workspace`, `PullRequest`, `Spec`,
 * `SpecRevision` and the transcript variants under exactly that risk. (`Session` here is
 * DE2.4's UNIT OF WORK — a forward reference to a type that does not exist yet, not the
 * process-level type #103 renamed to `Execution`. Do not rename it.)
 *
 * So the property is derived from the SCHEMAS instead of asserted about the files, at
 * FIELD granularity rather than file granularity:
 *
 * 1. **Declare** — walk the AST of every schema a golden is written through and collect
 *    every `Schema.optionalKey` property reachable from it, plus every case of every
 *    tagged union reachable from it. This is the full set of things a golden COULD pin,
 *    read off the contract itself, so a newly added optional field or union case enters
 *    the set the moment it is declared — no list to update.
 * 2. **Observe** — walk the same AST alongside the JSON each golden actually encoded to,
 *    recording for each optional key the goldens that CARRY it and the goldens that OMIT
 *    it, and for each union case the goldens it appears in.
 * 3. **Verdict** — every optional key must have at least one golden of each form, and
 *    every union case must appear at least once. Anything else throws, which fails
 *    `bun run check:goldens` (the generator is spawned by it and its stderr is surfaced).
 *
 * The walk is schema-directed and the census is keyed by the field's DECLARATION (see
 * {@link Census}), so a field is one entry however many goldens embed its type and by
 * whatever route: `Job.executionId` reached through `job-minimal` and through
 * `snapshot.jobs[1]` is the same entry, and its two forms may be pinned by different
 * goldens.
 *
 * Why here and not in the Swift suite: the Swift mirror has no idea which of its
 * `Optional` properties came from `Schema.optionalKey` and which are its own modelling,
 * and it cannot see a variant that no golden instantiates at all. The schema knows both.
 */
import * as SchemaAST from "effect/SchemaAST";

/** A golden as it was written: its name, the schema it went through, and the wire JSON. */
export interface WrittenGolden {
  readonly name: string;
  readonly ast: SchemaAST.AST;
  readonly encoded: unknown;
}

/** One `Schema.optionalKey` property, and the goldens pinning each of its two forms. */
interface OptionalKeyEntry {
  /** A schema path to the field, for the failure message (e.g. `snapshot.jobs[].pr`). */
  readonly label: string;
  readonly carriedBy: Set<string>;
  readonly omittedBy: Set<string>;
}

/** One tagged union, its declared cases, and the cases some golden instantiates. */
interface UnionEntry {
  readonly label: string;
  readonly cases: ReadonlyMap<string, SchemaAST.AST>;
  readonly observed: Set<string>;
}

/**
 * The census is keyed by DECLARATION, not by node: a `PropertySignature` for an optional
 * key, and a union's `types` array for a union.
 *
 * The node itself is the wrong key. `Schema.optionalKey(Usage)` clones the `Usage` node to
 * carry the optional-ness in its `Context`, so `TurnCompleted.usage` and the standalone
 * `Usage` are DIFFERENT node objects describing the same type — keying by node would
 * demand `usages` cover `Usage.cacheReadTokens`'s absent form once for every place a
 * `Usage` is embedded. The clone shares the ORIGINAL's `propertySignatures` array (and so
 * its `PropertySignature` objects), which is exactly "the same field of the same type,
 * wherever it is reached".
 */
interface Census {
  readonly optionalKeys: Map<SchemaAST.PropertySignature, OptionalKeyEntry>;
  readonly unions: Map<ReadonlyArray<SchemaAST.AST>, UnionEntry>;
}

/** Looks through a `Suspend` (recursive schema) to the node it stands for. */
const deref = (ast: SchemaAST.AST): SchemaAST.AST =>
  SchemaAST.isSuspend(ast) ? deref(ast.thunk()) : ast;

const isJsonObject = (value: unknown): value is { readonly [key: string]: unknown } =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** The `_tag` literal of a union member, or `undefined` when it carries none. */
const caseTag = (ast: SchemaAST.AST): string | undefined => {
  const node = deref(ast);
  if (!SchemaAST.isObjects(node)) return undefined;
  const tag = node.propertySignatures.find((ps) => ps.name === "_tag");
  if (tag === undefined) return undefined;
  const type = deref(tag.type);
  return SchemaAST.isLiteral(type) && typeof type.literal === "string" ? type.literal : undefined;
};

/**
 * The union's cases keyed by `_tag`, or `undefined` when it is not a tagged union (a
 * union of literals or primitives — nothing that can carry an optional key).
 */
const taggedCases = (union: SchemaAST.Union): ReadonlyMap<string, SchemaAST.AST> | undefined => {
  const cases = new Map<string, SchemaAST.AST>();
  for (const member of union.types) {
    const tag = caseTag(member);
    // A duplicate tag would make the union undiscriminable; treat it as untagged rather
    // than silently dropping a case.
    if (tag === undefined || cases.has(tag)) return undefined;
    cases.set(tag, member);
  }
  return cases.size > 0 ? cases : undefined;
};

/** The element schema at `index` of an array/tuple node, if the node describes one. */
const elementAt = (node: SchemaAST.Arrays, index: number): SchemaAST.AST | undefined =>
  node.elements[index] ?? node.rest[0];

/** Step 1 — everything reachable from `ast` that a golden could pin. */
const declare = (
  census: Census,
  ast: SchemaAST.AST,
  path: string,
  visited: Set<SchemaAST.AST>,
): void => {
  const node = deref(ast);
  // Recursive schemas (and merely repeated ones) reach the same node again; what it
  // declares does not depend on the path taken to it, so visiting it once is enough — and
  // is what terminates the walk on a recursive schema.
  if (visited.has(node)) return;
  visited.add(node);

  if (SchemaAST.isObjects(node)) {
    for (const ps of node.propertySignatures) {
      const childPath = `${path}.${String(ps.name)}`;
      if (SchemaAST.isOptional(ps.type) && !census.optionalKeys.has(ps)) {
        census.optionalKeys.set(ps, {
          label: childPath,
          carriedBy: new Set(),
          omittedBy: new Set(),
        });
      }
      declare(census, ps.type, childPath, visited);
    }
    for (const signature of node.indexSignatures) {
      declare(census, signature.type, `${path}[*]`, visited);
    }
    return;
  }
  if (SchemaAST.isArrays(node)) {
    node.elements.forEach((element, index) =>
      declare(census, element, `${path}[${index}]`, visited),
    );
    for (const rest of node.rest) declare(census, rest, `${path}[]`, visited);
    return;
  }
  if (SchemaAST.isUnion(node)) {
    const cases = taggedCases(node);
    if (cases === undefined) {
      for (const member of node.types) declare(census, member, path, visited);
      return;
    }
    if (!census.unions.has(node.types)) {
      census.unions.set(node.types, { label: path, cases, observed: new Set() });
    }
    for (const [tag, member] of cases) declare(census, member, `${path}<${tag}>`, visited);
  }
};

/** Step 2 — what the golden's actual wire JSON pins, walked alongside its schema. */
const observe = (census: Census, ast: SchemaAST.AST, value: unknown, golden: string): void => {
  const node = deref(ast);

  if (SchemaAST.isObjects(node) && isJsonObject(value)) {
    const named = new Set<string>();
    for (const ps of node.propertySignatures) {
      const key = String(ps.name);
      named.add(key);
      const carried = Object.hasOwn(value, key);
      const entry = census.optionalKeys.get(ps);
      if (entry !== undefined) (carried ? entry.carriedBy : entry.omittedBy).add(golden);
      if (carried) observe(census, ps.type, value[key], golden);
    }
    const signature = node.indexSignatures[0];
    if (signature !== undefined) {
      for (const [key, member] of Object.entries(value)) {
        if (!named.has(key)) observe(census, signature.type, member, golden);
      }
    }
    return;
  }
  if (SchemaAST.isArrays(node) && Array.isArray(value)) {
    value.forEach((element: unknown, index) => {
      const elementAst = elementAt(node, index);
      if (elementAst !== undefined) observe(census, elementAst, element, golden);
    });
    return;
  }
  if (SchemaAST.isUnion(node)) {
    const entry = census.unions.get(node.types);
    if (entry !== undefined) {
      if (!isJsonObject(value) || typeof value["_tag"] !== "string") return;
      const tag = value["_tag"];
      const member = entry.cases.get(tag);
      if (member === undefined) {
        throw new Error(`${golden}: ${entry.label} carries the unknown case "${tag}"`);
      }
      entry.observed.add(tag);
      observe(census, member, value, golden);
      return;
    }
    // An untagged union: only worth descending when the value's shape names exactly one
    // member. Anything ambiguous is a shape this walker was never taught to resolve, and
    // silently not descending is how an optional key goes uncounted — so it says so.
    const candidates = node.types.filter((member) => SchemaAST.isObjects(deref(member)));
    const [only] = candidates;
    if (candidates.length === 1 && only !== undefined) {
      observe(census, only, value, golden);
      return;
    }
    if (candidates.length > 1 && isJsonObject(value)) {
      throw new Error(
        `${golden}: cannot resolve an UNTAGGED union of ${candidates.length} object schemas ` +
          `— extend golden-coverage.ts rather than leaving its optional keys uncounted.`,
      );
    }
  }
};

/**
 * Fails generation unless every `Schema.optionalKey` reachable from a golden's schema has
 * a golden pinning its PRESENT form and a golden pinning its ABSENT form, and every
 * tagged-union case reachable from a golden's schema appears in some golden.
 *
 * @throws when the corpus does not pin one of those forms — the message names the field
 *   or case and the goldens that do cover it.
 */
export const assertGoldenCoverage = (goldens: readonly WrittenGolden[]): void => {
  const census: Census = { optionalKeys: new Map(), unions: new Map() };
  const visited = new Set<SchemaAST.AST>();
  for (const golden of goldens) declare(census, golden.ast, golden.name, visited);
  for (const golden of goldens) observe(census, golden.ast, golden.encoded, golden.name);

  const failures: string[] = [];
  const list = (names: Set<string>): string =>
    names.size === 0 ? "no golden reaches this field at all" : [...names].sort().join(", ");
  for (const entry of census.optionalKeys.values()) {
    if (entry.omittedBy.size === 0) {
      failures.push(
        `  ${entry.label}: optionalKey — NO golden OMITS it, so nothing can catch a ` +
          `mirror emitting null here (carried by: ${list(entry.carriedBy)})`,
      );
    }
    if (entry.carriedBy.size === 0) {
      failures.push(
        `  ${entry.label}: optionalKey — NO golden CARRIES it, so its present form is ` +
          `unpinned (omitted by: ${list(entry.omittedBy)})`,
      );
    }
  }
  for (const entry of census.unions.values()) {
    for (const tag of entry.cases.keys()) {
      if (!entry.observed.has(tag)) {
        failures.push(`  ${entry.label}<${tag}>: tagged-union case in NO golden`);
      }
    }
  }

  if (failures.length > 0) {
    throw new Error(
      "FAIL: the golden corpus does not pin every optional key and union case (issue #89).\n" +
        `${failures.sort().join("\n")}\n` +
        "THE FIX IS: add or amend a fixture in generate-goldens.ts so each optional key\n" +
        "appears in one golden and is OMITTED in another — the omitted-vs-null difference\n" +
        "is unobservable for a field no golden ever leaves out.",
    );
  }
};
