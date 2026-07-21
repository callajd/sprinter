/**
 * The owned `── STATE ──` entity {@link Repository} — a repository as OBSERVED on a
 * code host (DE1.2).
 *
 * It is the anchor the whole state layer hangs from. Until now repository identity in
 * the domain was a bare `repo: string` on `Workstream`, which is not an identity at
 * all: two spellings of one repository (`"callajd/sprinter"`, `"CallaJD/Sprinter"`)
 * are two different anchors, nothing can be referenced FROM it, and no constraint can
 * be expressed about it. `Repository` replaces that string with a real entity carrying
 * a real branded id, so `Workstream.repositoryId` — and later `Issue.repositoryId`,
 * `PullRequest.repositoryId`, `SpecRevision.boundTo` — reference ONE record.
 *
 * ## It is REFERENCED, not owned — hence `observedAt` (INV-OBSERVED)
 *
 * Sprinter does not create repositories; it reads them off a code host. So a
 * `Repository` is a SNAPSHOT of something outside the system, and it carries the
 * instant that snapshot was taken. `observedAt` is the shared owned {@link Timestamp}
 * — the same canonical instant type every other stamp in the domain uses, never a
 * second time type. Owned entities (`Workstream`, `Epic`, `Agent`) carry no
 * `observedAt` and must not gain one: they are not observations of anything.
 *
 * Staleness is RENDERED from `observedAt` (DE4.4), never enforced: nothing in the
 * domain, the store, or the port refuses a read because an observation is old. A
 * refresh REPLACES the record wholesale under a new `observedAt` (D7) rather than
 * merging fields into it, so a record always describes one coherent moment.
 *
 * ## The natural key is `(host, owner, name)`
 *
 * {@link RepositoryId} is opaque; {@link RepositoryKey} is what actually identifies a
 * repository, and the store holds that triple UNIQUE. Two records disagreeing about
 * the same repository is therefore unconstructible rather than merely unlikely, which
 * is what makes a code host's resolve deterministic by construction instead of by
 * convention (INV-ENFORCE).
 *
 * This module is a pure description of shape: it references no backing store, no HTTP
 * client and no running instance (INV-PORT). Nothing here knows what GitHub is.
 */
import { Schema } from "effect";
import { BranchName, CommitSha, RepositoryId } from "./ids.ts";
import { Timestamp } from "./time.ts";

/**
 * The code hosts Sprinter can observe a repository on — a CLOSED literal set, not a
 * free string.
 *
 * The set is closed because a host value with no adapter behind it is a lie: it would
 * name a repository nothing can read, resolve, or refresh. Adding a host is therefore
 * two edits that force each other — extend this union AND add the adapter that
 * implements the `CodeHost` port for it — and the type checker's exhaustiveness is
 * what surfaces every place that has to grow. A free string would admit `"gitlab"`
 * today, with nothing to read it, and the failure would arrive at a call site far from
 * the write.
 */
export const RepositoryHost = Schema.Literals(["github"]);
export type RepositoryHost = (typeof RepositoryHost)["Type"];

/**
 * One SEGMENT of a repository's natural key — its `owner` or its `name`.
 *
 * Non-empty, and it may not contain `/`. The slash rule is what keeps the natural key
 * UNAMBIGUOUS: `(host, owner, name)` is a three-part key, and an owner of `"a/b"` with
 * a name of `"c"` would denote the same `owner/name` path as an owner of `"a"` with a
 * name of `"b/c"` — two rows, one repository, and the UNIQUE constraint powerless to
 * see it because the triples genuinely differ. Rejecting the separator at the SCHEMA
 * makes that collision unconstructible (INV-ENFORCE). No code host permits `/` in
 * either segment anyway, so nothing real is excluded.
 */
export const RepositorySegment = Schema.NonEmptyString.check(
  Schema.makeFilter((value: string) => !value.includes("/"), {
    expected: "a repository owner/name segment containing no '/'",
  }),
);
export type RepositorySegment = (typeof RepositorySegment)["Type"];

/**
 * The NATURAL KEY of a repository: the triple that identifies it on a code host,
 * independently of any id Sprinter has minted for it.
 *
 * It exists as ONE value rather than three loose fields because its parts are only
 * meaningful together — an `owner` without a `host` names nothing — and because it is
 * what a caller that has no {@link RepositoryId} can supply: a client composing a
 * `WorkstreamPlan` knows `github / callajd / sprinter`, and the `CodeHost` port turns
 * that into a `Repository` (D6). The store holds this triple UNIQUE.
 */
export const RepositoryKey = Schema.Struct({
  host: RepositoryHost,
  owner: RepositorySegment,
  name: RepositorySegment,
});
export type RepositoryKey = (typeof RepositoryKey)["Type"];

/**
 * One OBSERVED ref: a branch name paired with the commit its tip pointed at when the
 * repository was observed.
 *
 * Both halves are checked values, not strings — a malformed branch name or an
 * abbreviated/uppercased sha cannot be constructed (see {@link BranchName} /
 * {@link CommitSha}).
 */
export const RepositoryRef = Schema.Struct({
  name: BranchName,
  sha: CommitSha,
});
export type RepositoryRef = (typeof RepositoryRef)["Type"];

/** True when no two refs in `refs` share a branch name. */
const hasDistinctNames = (refs: ReadonlyArray<RepositoryRef>): boolean =>
  new Set(refs.map((ref) => ref.name)).size === refs.length;

/**
 * A repository as observed on a code host — `{ id, host, owner, name, refs,
 * observedAt }`.
 *
 * - `id` — the opaque {@link RepositoryId}. Nothing parses it; the natural key
 *   identifies the repository (see the module docstring).
 * - `host` / `owner` / `name` — the natural key, inline. It is spelled out here rather
 *   than nested as a {@link RepositoryKey} because these are the entity's own columns
 *   and the `UNIQUE` constraint is over exactly them.
 * - `refs` — the OBSERVED ref map, `BranchName → CommitSha`, modelled as a LIST of
 *   {@link RepositoryRef} rather than a JSON object keyed by branch name. That is
 *   deliberate: a record schema SELECTS the keys its key-schema matches, so a
 *   malformed branch name would be silently DROPPED on decode instead of rejecting the
 *   observation — the opposite of what {@link BranchName} exists for. As a list, every
 *   entry is decoded and a bad one fails loudly. Ordered by name, and its names are
 *   DISTINCT — checked here, and made unconstructible in the store by the
 *   `repository_ref` composite PRIMARY KEY (INV-ENFORCE). An EMPTY list is VALID: it
 *   means nothing has been observed yet, not that the repository has no branches.
 * - `observedAt` — when this snapshot was taken (INV-OBSERVED). See the module
 *   docstring: rendered as staleness, never enforced.
 *
 * Nothing DERIVED is stored here (INV-DERIVED). Notably a pull request's staleness is
 * NOT a field: it is computed as `tipOf(pr.target) ≠ pr.base` against this `refs` map
 * (DE2.3), so it can never disagree with the observation it is derived from.
 */
export const Repository = Schema.Struct({
  id: RepositoryId,
  host: RepositoryHost,
  owner: RepositorySegment,
  name: RepositorySegment,
  refs: Schema.Array(RepositoryRef).check(
    Schema.makeFilter((refs: ReadonlyArray<RepositoryRef>) => hasDistinctNames(refs), {
      expected: "a ref list with no repeated branch name",
    }),
  ),
  observedAt: Timestamp,
});
export type Repository = (typeof Repository)["Type"];

/** The {@link RepositoryKey} of a stored {@link Repository} — its natural key, extracted. */
export const repositoryKey = (repository: Repository): RepositoryKey => ({
  host: repository.host,
  owner: repository.owner,
  name: repository.name,
});

/**
 * The commit a branch's tip pointed at when `repository` was observed, or `undefined`
 * when that branch was not among the observed refs.
 *
 * This is the ONE reader of `refs`, and it is why `refs` is a map in spirit even
 * though it is a list on the wire: "what is the tip of `main`?" is the question every
 * downstream staleness computation asks (DE2.3's `tipOf(pr.target) ≠ pr.base`).
 * Absence is `undefined` and means exactly "not observed" — never "the branch does not
 * exist", which only the code host can say.
 */
export const tipOf = (repository: Repository, branch: BranchName): CommitSha | undefined =>
  repository.refs.find((ref) => ref.name === branch)?.sha;
