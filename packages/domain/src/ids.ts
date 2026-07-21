/**
 * Branded identifier types for the owned read model, plus the two VALUE types the
 * code-host transition layer is built on ({@link CommitSha} / {@link BranchName}).
 *
 * Each ID is a non-empty string with a nominal {@link Schema.brand} so the
 * type checker rejects passing (say) an `EpicId` where a `WorkstreamId` is
 * required, even though both are structurally `string`. Branding adds no
 * runtime check beyond non-emptiness — it narrows the TypeScript type only
 * (INV-NAMING: owned types, plain names).
 *
 * {@link CommitSha} and {@link BranchName} are DIFFERENT in kind and deliberately so:
 * they are not identifiers we mint, they are values a code HOST hands us, and their
 * whole point is that a malformed one cannot be CONSTRUCTED (INV-ENFORCE). So they
 * carry a real check in the SCHEMA — the one place every such value enters the domain
 * — rather than a call-site guard a sibling path can skip. A branded
 * `NonEmptyString` would accept `"zzz"` as a commit sha, which is exactly the state
 * this module exists to make unreachable.
 */
import { Schema } from "effect";

/** Identifies a {@link Workstream} — the top of the `Workstream ⊃ Epic ⊃ Issue` hierarchy. */
export const WorkstreamId = Schema.NonEmptyString.pipe(Schema.brand("WorkstreamId"));
export type WorkstreamId = (typeof WorkstreamId)["Type"];

/** Identifies an {@link Epic} — a related set of Issues within a workstream. */
export const EpicId = Schema.NonEmptyString.pipe(Schema.brand("EpicId"));
export type EpicId = (typeof EpicId)["Type"];

/** Identifies an {@link Issue} — one ~PR-sized unit of code work. */
export const IssueId = Schema.NonEmptyString.pipe(Schema.brand("IssueId"));
export type IssueId = (typeof IssueId)["Type"];

/** Identifies a {@link Job} — one bounded cognitive task (1 Job = 1 session = 1 transcript = 1 PR). */
export const JobId = Schema.NonEmptyString.pipe(Schema.brand("JobId"));
export type JobId = (typeof JobId)["Type"];

/** Identifies a {@link Session} — one agent run executing a {@link Job}. */
export const SessionId = Schema.NonEmptyString.pipe(Schema.brand("SessionId"));
export type SessionId = (typeof SessionId)["Type"];

/**
 * Identifies an {@link Agent} — a member of the REGISTRY layer: owned, global,
 * and scoped to NO repository. Because the registry is append-only, an `AgentId`
 * identifies one immutable REVISION: editing an agent mints a NEW id whose record
 * points back at the previous one through `supersedes`.
 */
export const AgentId = Schema.NonEmptyString.pipe(Schema.brand("AgentId"));
export type AgentId = (typeof AgentId)["Type"];

/**
 * Identifies one STORE GENERATION — the lifetime of a single durable store, from
 * the moment its schema is created to the moment it is dropped and recreated.
 *
 * It is an OWNED, provider-neutral identifier (no SQL, no file path, no version
 * number leaks through it), which is what lets it appear on the daemon↔client
 * contract alongside the rest of the read model (INV-PORT). It is minted FRESH
 * every time the schema is created, so two generations never share one — that is
 * the whole point: durable offsets are only comparable WITHIN a generation, so a
 * cursor is meaningful only when paired with the generation it was minted in.
 *
 * A generation ends only by drop-and-recreate (`SCHEMA_VERSION`,
 * `packages/state/src/sqlite.ts` — INV-FRESH never migrates), and the schema is
 * applied at store construction, so a new generation is observable only across a
 * daemon RESTART. It is opaque: nothing may parse it, order it, or infer age from
 * it — the only defined operation is equality.
 */
export const StoreGenerationId = Schema.NonEmptyString.pipe(Schema.brand("StoreGenerationId"));
export type StoreGenerationId = (typeof StoreGenerationId)["Type"];

/**
 * Identifies a {@link Repository} — a repository as OBSERVED on a code host, and the
 * anchor the whole `── STATE ──` layer hangs from (`Workstream.repositoryId` today;
 * `Issue`/`PullRequest`/`SpecRevision` later).
 *
 * It is OPAQUE: nothing may parse it, order it, or read an owner/name back out of
 * it — equality is the only defined operation. The NATURAL key of a repository is
 * `(host, owner, name)`, and that triple is what the store holds `UNIQUE`, so
 * "which repository is this?" is answered by the key, never by decomposing the id.
 *
 * Because the natural key is what identifies a repository, the id a code-host adapter
 * mints for one is a FUNCTION of that key: re-observing the same repository must
 * produce the same id, or a refresh (DE1.2 D7 — replace the record wholesale) would
 * try to insert a SECOND row for a triple the store holds unique and fail. That is a
 * property of the minting adapter, not something a consumer may rely on by inspecting
 * the value.
 */
export const RepositoryId = Schema.NonEmptyString.pipe(Schema.brand("RepositoryId"));
export type RepositoryId = (typeof RepositoryId)["Type"];

/**
 * The shape of a full git object name: EXACTLY 40 LOWERCASE hex characters.
 *
 * Lowercase-only is a canonicality rule, not a stylistic one, and it is the same
 * argument {@link ./time.ts}'s `Timestamp` makes: `"ABC…"` and `"abc…"` denote ONE
 * commit, so admitting both would put two spellings of one value in the store, on the
 * wire and in the mirror — and equality (the only operation a sha has here) would then
 * be false for two references to the same commit. Every code host Sprinter reads emits
 * lowercase, so this is a check, not a transformation.
 *
 * The length is fixed at 40 for the same reason: an ABBREVIATED sha (`"a1b2c3d"`) is
 * not a shorter spelling of the same value but a PREFIX QUERY that a repository must
 * resolve, and whose resolution can change as the repository grows objects. Accepting
 * one would mean storing a value whose referent is not fixed.
 */
const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/;

/**
 * A full git commit object name — 40 lowercase hex characters ({@link FULL_COMMIT_SHA}).
 *
 * The check lives HERE, in the schema, so a malformed sha cannot be CONSTRUCTED
 * anywhere in the domain (INV-ENFORCE): `"zzz"`, an uppercase spelling, a 7-character
 * abbreviation and a 41-character string all fail to DECODE, at the one boundary every
 * externally-sourced sha crosses, rather than being caught by whichever call site
 * happened to remember to look.
 *
 * It is a VALUE, not an identifier: two `CommitSha`s are the same commit exactly when
 * their strings are equal, and nothing orders them (ancestry is a graph question the
 * code host answers, never a string comparison).
 */
export const CommitSha = Schema.String.check(
  Schema.makeFilter((value: string) => FULL_COMMIT_SHA.test(value), {
    expected: "a full 40-character lowercase hex commit sha",
  }),
).pipe(Schema.brand("CommitSha"));
export type CommitSha = (typeof CommitSha)["Type"];

/**
 * True when `value` is a usable git branch (ref) name.
 *
 * This enforces the subset of `git check-ref-format` that is DECIDABLE from the name
 * alone and that a malformed value would actually break — it is deliberately a
 * MINIMUM, not a re-implementation of git's full grammar:
 *
 * - NON-EMPTY. An empty ref names nothing.
 * - No ASCII WHITESPACE or CONTROL characters. These are the characters that make a
 *   name unquotable in the shell commands and URLs a branch name reaches, and git
 *   refuses them outright.
 * - No `..`. Git reserves it (it is the range operator), so `a..b` is not a branch.
 * - No leading or trailing `/`, and no `//`. A ref is a `/`-separated path with
 *   non-empty components; each of these produces an empty component.
 * - Does not end in `.lock`. Git uses `<ref>.lock` files for its own locking, so such
 *   a name can never be created as a ref.
 * - Is not exactly `@`. Git reserves the bare `@` as shorthand for `HEAD`.
 *
 * Rules git also has that are NOT enforced here (a leading/trailing `.` in a component,
 * `@{`, a trailing `.`, `~^:?*[`, a backslash) are omitted deliberately: they would
 * reject names no code host produces, and the value of this check is that every branch
 * name in the domain is USABLE, not that it is byte-identical to git's own verdict.
 * Widening it later is a pure narrowing of accepted input and needs no migration.
 */
const isValidBranchName = (value: string): boolean => {
  if (value.length === 0) return false;
  // ASCII whitespace and CONTROL characters, tested by CODE POINT rather than by a
  // regex character class: the class would have to hold literal control bytes (or
  // escapes a linter flags), while the numeric form STATES the range — C0 (< 0x20),
  // space (0x20) and DEL (0x7f) — in the open.
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0;
    if (code <= 0x20 || code === 0x7f) return false;
  }
  if (value.includes("..")) return false;
  if (value.startsWith("/") || value.endsWith("/")) return false;
  if (value.includes("//")) return false;
  if (value.endsWith(".lock")) return false;
  return value !== "@";
};

/**
 * A git branch name, checked against {@link isValidBranchName} in the SCHEMA so a
 * malformed one cannot be constructed (INV-ENFORCE) — `""`, `"feat x"`, `"a..b"`,
 * `"/x"`, `"x/"`, `"a//b"`, `"x.lock"` and `"@"` all fail to decode, while ordinary
 * names (`"main"`, `"feat/x-1"`, `"release/2.0"`) pass.
 *
 * A branch name is a NAME, not an identifier: it is the key of a repository's observed
 * ref map ({@link Repository.refs}), where each name appears at most once — a
 * constraint the store holds as a composite PRIMARY KEY rather than leaving to a
 * writer.
 */
export const BranchName = Schema.String.check(
  Schema.makeFilter((value: string) => isValidBranchName(value), {
    expected: "a valid git branch name",
  }),
).pipe(Schema.brand("BranchName"));
export type BranchName = (typeof BranchName)["Type"];
