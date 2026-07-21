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
 * ## KNOWN GAP: nothing REFRESHES a record after it is first created
 *
 * Stated plainly rather than implied away. As of DE1.2 the ONLY production caller of
 * `putRepository` is new-plan materialisation (`createWorkstreamFromPlan`), so a
 * repository is observed when a workstream is first anchored to it and NEVER AGAIN:
 * `observedAt` and `refs` freeze at that first sighting. The mechanism a refresh needs
 * exists and is tested — `resolve` re-observes and `putRepository` replaces wholesale
 * (D7) — but nothing TRIGGERS it: no poll, no timer, no invalidation. `tipOf` and
 * `repositoryKey` are exported for the readers that will follow and have no production
 * caller yet.
 *
 * The consequence is concrete and belongs to DE4.4, which renders staleness from
 * `observedAt`: with no refresh trigger every record renders as monotonically ageing,
 * so DE4.4 cannot land honestly until a trigger exists. That constraint is recorded
 * against DE4.4 in `docs/plan/domain-remodel.md`. Building the trigger is deliberately
 * NOT part of this task.
 *
 * ## The natural key is `(host, owner, name)`; the ID is the HOST's own
 *
 * {@link RepositoryKey} is what a human or a client NAMES a repository by, and the
 * store holds that triple UNIQUE, so two records disagreeing about one repository is
 * unconstructible rather than merely unlikely (INV-ENFORCE).
 *
 * But the triple is MUTABLE — a repository can be renamed or transferred — so it
 * cannot be what an id is derived from. {@link RepositoryId} is derived instead from
 * the STABLE identifier the host itself assigns (GitHub's numeric repository id),
 * which a rename does not change. That is what makes identity survive a rename: the
 * row keeps its id and its `host`/`owner`/`name` columns are UPDATED in place, rather
 * than a second row appearing under the new name while every existing
 * `Workstream.repositoryId` still points at the old one. Minting the id from the
 * natural key would fork identity on exactly the event the natural key exists to
 * survive.
 *
 * The UNIQUE constraint can only see a collision between IDENTICAL triples, so the
 * triple that reaches it must still be the HOST's canonical spelling, never the
 * caller's: `CallaJD/Sprinter` and `callajd/sprinter` name one repository on GitHub,
 * and storing each caller's spelling verbatim would produce two triples that genuinely
 * differ. Canonicalisation is therefore the ADAPTER's obligation (INV-PORT): a
 * `CodeHost` builds the record, and mints its id, from the identity the host reports
 * for the key it was asked about.
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
 * The characters a repository owner or name may be spelled with: ASCII letters,
 * digits, `.`, `_` and `-`, one or more of them.
 *
 * This is an ALLOW-list, and deliberately a SUPERSET of what any code host actually
 * permits (GitHub allows exactly this set for a repository NAME and a strict subset of
 * it for an OWNER — no `.`, no leading/trailing `-`). An allow-list is the only form
 * that closes this hole once: a deny-list of "the characters that were dangerous last
 * time" is a list somebody has to keep complete, and the segment is CLIENT-SUPPLIED —
 * it arrives on `WorkstreamPlan` off the RPC surface and is interpolated into an
 * AUTHENTICATED request path by the code-host adapter.
 *
 * The superset is INTENDED, not a gap to be tightened later. This module is a pure
 * description of shape and knows nothing about GitHub (INV-PORT), so encoding GitHub's
 * exact owner grammar here would put a host's rules in the one place that must survive
 * a SECOND host adapter — and the next host's grammar will differ. What this rule owes
 * is that a segment cannot mean anything to the transports it is interpolated into and
 * cannot make the natural key ambiguous; deciding whether `--x` is a legal owner is the
 * HOST's job, and the host answers it with a 404 that {@link Repository} already models
 * as "no such repository". Narrowing per-host would trade a correct rejection the host
 * gives us for a rule the domain would then have to keep in sync with someone else's
 * product.
 */
const REPOSITORY_SEGMENT = /^[A-Za-z0-9._-]+$/;

/**
 * One SEGMENT of a repository's natural key — its `owner` or its `name`.
 *
 * Two rules, both enforced HERE rather than at a call site, because this is the one
 * boundary every natural key crosses (INV-ENFORCE):
 *
 * 1. **{@link REPOSITORY_SEGMENT}** — the segment is one or more of `[A-Za-z0-9._-]`.
 *    Excluding `/` is what keeps the natural key UNAMBIGUOUS: `(host, owner, name)` is
 *    a three-part key, and an owner of `"a/b"` with a name of `"c"` would denote the
 *    same `owner/name` path as an owner of `"a"` with a name of `"b/c"` — two rows, one
 *    repository, and the UNIQUE constraint powerless to see it because the triples
 *    genuinely differ. Excluding everything ELSE outside the set is what keeps a
 *    segment from meaning anything to the transports it is interpolated into: `%`, `?`,
 *    `#`, `\` and a raw control character each carry a syntax somewhere downstream.
 * 2. **`.` and `..` are rejected outright.** They match rule 1's character set, and they
 *    are the reason rule 1 is not sufficient on its own: `.` and `..` are the relative
 *    PATH segments, `encodeURIComponent` does NOT escape `.`, and URL normalisation
 *    resolves them — so a `name` of `".."` interpolated into `/repos/{owner}/{name}`
 *    walks OUT of the repository resource and lets a caller who supplied only a
 *    repository key steer the adapter's authenticated request at an unrelated endpoint
 *    (`owner: "..", name: "user"` → `GET /user`). A segment that denotes a directory
 *    traversal is not a repository name on any host, so nothing real is excluded.
 *
 * Both rules are SCHEMA constraints, so a segment that violates either cannot be
 * CONSTRUCTED — the rejection happens on DECODE, at the RPC boundary, before any
 * adapter sees the value (asserted end-to-end in `packages/daemon/src/acceptance.test.ts`,
 * which drives `owner: ".."` over the REAL serialized socket and asserts the `CodeHost`
 * was never asked). Checking it in the adapter instead would leave the guarantee to
 * whichever call site remembered to look.
 *
 * It is BRANDED for the same reason {@link CommitSha} and {@link BranchName} are: the
 * check is only load-bearing if a plain `string` cannot stand in for a checked one.
 * Without the brand `RepositoryKey["Type"].owner` is `string`, and
 * `resolve({ host: "github", owner: "..", name: "user" })` typechecks anywhere in the
 * tree — the rule above would then be enforced only on the paths that happen to decode.
 * With it, the ONLY way to obtain a segment is to decode one.
 */
export const RepositorySegment = Schema.NonEmptyString.check(
  Schema.makeFilter(
    (value: string) => REPOSITORY_SEGMENT.test(value) && value !== "." && value !== "..",
    {
      expected:
        "a repository owner/name segment: one or more of [A-Za-z0-9._-], and neither '.' nor '..'",
    },
  ),
).pipe(Schema.brand("RepositorySegment"));
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

/**
 * Compare two branch names by Unicode CODE POINT — the ONE order {@link Repository}'s
 * `refs` list is held in, and the one every producer must sort by.
 *
 * It is code-point order because that is what SQLite's default `BINARY` collation
 * yields for the `TEXT` column the store keeps ref names in: `BINARY` is a `memcmp`
 * over the UTF-8 encoding, and UTF-8 preserves code-point order byte-for-byte. The
 * obvious alternative — JavaScript's `<` on strings — is UTF-16 CODE UNIT order, which
 * disagrees for every NON-BMP name (a surrogate pair sorts as `0xD800…` and so lands
 * BEFORE `U+E000…`, while its code point is far above it). A branch name may hold such
 * a character ({@link BranchName} forbids only whitespace, controls and git's own
 * reserved forms), so the disagreement is reachable, and a record whose `refs` order
 * flipped across a store round-trip would make the ordering claim on `refs` false in
 * exactly the way a stale assertion is worst: silently.
 *
 * Comparing code points rather than transcoding to bytes is the same order with no
 * allocation of an encoder, and it states the rule in the units the rule is about.
 */
export const compareBranchNames = (left: string, right: string): number => {
  // `Array.from` on a string iterates CODE POINTS (a surrogate pair is one element),
  // which is the whole point: comparing UTF-16 code units is the order this exists to
  // avoid.
  const leftPoints = Array.from(left);
  const rightPoints = Array.from(right);
  const shared = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < shared; index += 1) {
    const leftPoint = leftPoints[index]?.codePointAt(0) ?? 0;
    const rightPoint = rightPoints[index]?.codePointAt(0) ?? 0;
    if (leftPoint !== rightPoint) return leftPoint < rightPoint ? -1 : 1;
  }
  return leftPoints.length - rightPoints.length;
};

/**
 * True when `refs` is in STRICTLY ascending {@link compareBranchNames} order.
 *
 * This is ONE rule, not two. Strictness (`< 0`, not `<= 0`) means it SUBSUMES
 * distinctness: in a sorted list equal names are adjacent, so a repeated name is
 * exactly an adjacent pair comparing `=== 0`, which this already rejects. A separate
 * "no duplicates" filter beside it would be redundant and, worse, would present as an
 * independent guarantee a reader might rely on if the order rule were ever relaxed.
 * Uniqueness is ALSO unconstructible in the store, by `repository_ref`'s composite
 * PRIMARY KEY — that one is genuinely independent, because it holds against a writer
 * that never went through this schema.
 */
const isSortedByName = (refs: ReadonlyArray<RepositoryRef>): boolean =>
  refs.every(
    (ref, index) => index === 0 || compareBranchNames(refs[index - 1]?.name ?? "", ref.name) < 0,
  );

/**
 * The observed ref LIST of a {@link Repository}: strictly ascending in
 * {@link compareBranchNames} order, hence also free of repeated names.
 *
 * It is BRANDED, and the brand is what makes the check load-bearing rather than
 * decorative. The filter is a runtime rule, and every producer in the tree — the GitHub
 * adapter, both store readers — assembles a `Repository` as a typed OBJECT LITERAL, at
 * which no schema runs. Unbranded, `refs` would be a plain `ReadonlyArray<RepositoryRef>`
 * and a mis-sorted literal would typecheck and construct fine, only failing later on
 * ENCODE — inside a `Snapshot` response, i.e. breaking every connected client rather
 * than the producer. Branded, the only way to obtain the field's type is to DECODE, so a
 * mis-sorted list fails where it is built.
 */
export const RepositoryRefs = Schema.Array(RepositoryRef)
  .check(
    Schema.makeFilter((refs: ReadonlyArray<RepositoryRef>) => isSortedByName(refs), {
      expected:
        "a ref list strictly ascending by branch name (Unicode code point order), hence with no repeated name",
    }),
  )
  .pipe(Schema.brand("RepositoryRefs"));
export type RepositoryRefs = (typeof RepositoryRefs)["Type"];

/**
 * A repository as observed on a code host — `{ id, host, owner, name, refs,
 * observedAt }`.
 *
 * - `id` — the opaque {@link RepositoryId}, minted by the adapter from the host's own
 *   STABLE identifier so a rename does not fork it. Nothing parses it; a human names a
 *   repository by the natural key (see the module docstring).
 * - `host` / `owner` / `name` — the natural key, inline. It is spelled out here rather
 *   than nested as a {@link RepositoryKey} because these are the entity's own columns
 *   and the `UNIQUE` constraint is over exactly them.
 * - `refs` — the OBSERVED ref map, `BranchName → CommitSha`, modelled as a LIST of
 *   {@link RepositoryRef} rather than a JSON object keyed by branch name. That is
 *   deliberate: a record schema SELECTS the keys its key-schema matches, so a
 *   malformed branch name would be silently DROPPED on decode instead of rejecting the
 *   observation — the opposite of what {@link BranchName} exists for. As a list, every
 *   entry is decoded and a bad one fails loudly. It is STRICTLY ORDERED by
 *   {@link compareBranchNames} (Unicode code point, the order the store's `ORDER BY name`
 *   yields), which also makes its names distinct — one rule, CHECKED here rather than
 *   merely documented, and {@link RepositoryRefs}' brand is what forces a producer
 *   through that check instead of past it, so a producer that sorts differently fails to
 *   BUILD the record rather than shipping an order the next round-trip through the store
 *   would silently change. Uniqueness is additionally unconstructible in the store, by
 *   the `repository_ref` composite PRIMARY KEY (INV-ENFORCE). An EMPTY list is VALID: it
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
  refs: RepositoryRefs,
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
 * This is the one place `refs` is LOOKED UP by name (as opposed to built or persisted
 * whole), and it is why `refs` is a map in spirit even though it is a list on the wire:
 * "what is the tip of `main`?" is the question every downstream staleness computation
 * asks (DE2.3's `tipOf(pr.target) ≠ pr.base`). Absence is `undefined` and means exactly
 * "not observed" — never "the branch does not exist", which only the code host can say.
 *
 * It has NO production caller as of DE1.2 — DE2.3's staleness computation is the reader
 * it exists for (see the module docstring's KNOWN GAP).
 */
export const tipOf = (repository: Repository, branch: BranchName): CommitSha | undefined =>
  repository.refs.find((ref) => ref.name === branch)?.sha;
