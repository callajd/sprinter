/**
 * The `Agent` REGISTRY's first PRODUCTION writer (DE2.2, D2).
 *
 * DE1.1 shipped the registry reachable only from tests, so `Snapshot.agents` was empty
 * in a real daemon: nothing on the production path had ever appended a revision. This
 * module is what changes that — when an execution starts, the {@link JobRunner} records
 * the exact {@link Agent} revision it ran, and `Execution.agentId` is a FOREIGN KEY onto
 * it, so a run cannot be stored without its agent being registered first.
 *
 * ## Identity is DERIVED from content, never chosen
 *
 * A writer declares what an agent IS ({@link AgentContent}); the revision's `id` is a
 * function of that content — `agt-<sha256 prefix>` over a canonical encoding of
 * `name` / `model` / `version` / `tools`. That is what makes the registry's two
 * append-only promises hold BY CONSTRUCTION rather than by a writer's discipline:
 *
 * - re-running the SAME agent re-derives the SAME id with byte-identical content, so
 *   `putAgent` is the idempotent no-op it promises (every dispatch, every retry, every
 *   restart), and
 * - CHANGING the agent — a new model, a bumped version, a different tool set — derives
 *   a DIFFERENT id, so it lands as a NEW revision. It can never collide with the old
 *   one under the same id, which is the one thing `putAgent` refuses (an id already
 *   stored with different content). A writer that minted its own stable id would hit
 *   exactly that refusal, at dispatch time, the first time someone edited the agent
 *   definition without remembering to change the id too.
 *
 * The derivation is over the DECODED content in a fixed field order, with `tools` in
 * declaration order — the tool ORDER is part of an agent's content (`registry.ts`), so
 * two orderings are two agents, deliberately.
 *
 * ## What this writer does NOT do
 *
 * It appends ORIGINAL revisions only: it never sets `supersedes` and never retires. An
 * EDIT is a human operation on a lineage — "this agent is now that agent" — and the
 * runner has no standing to assert it: it knows the content it is about to run, not
 * whether that content is a successor to something a person meant to replace. Linking
 * lineages is the registry surface's job, not the dispatcher's; recording the exact
 * revision that ran is this one's, and it is complete on its own (a historical
 * execution resolves to its agent regardless of any lineage link).
 */
import { type Context, Effect, Encoding, Schema } from "effect";
import { type Agent, AgentContent, AgentId } from "@sprinter/domain";
import type { StateStore, StateStoreError } from "@sprinter/state";

/**
 * How many hex characters of the content digest the id carries. 32 hex characters =
 * 128 bits of SHA-256, which is far beyond any collision risk for a registry that
 * grows by human edits, and short enough that an id stays readable in a log line.
 */
const DIGEST_CHARS = 32;

/**
 * The canonical bytes of an agent's content — the input to the digest. A fixed field
 * order and `JSON.stringify` over the ENCODED content: the same content always yields
 * the same bytes, and any difference in any field (including the ORDER of `tools`)
 * yields different ones.
 */
const canonicalBytes = (content: AgentContent): Effect.Effect<Uint8Array<ArrayBuffer>> =>
  Schema.encodeEffect(AgentContent)(content).pipe(
    Effect.map((encoded) => {
      const text = new TextEncoder().encode(
        JSON.stringify([encoded.name, encoded.model, encoded.version, encoded.tools]),
      );
      // Copied into a plain `ArrayBuffer`-backed view: that is the byte source
      // `crypto.subtle.digest` takes, whereas a `TextEncoder` result is typed over the
      // wider `ArrayBufferLike` (shared buffers included). A copy, not a cast.
      const bytes = new Uint8Array(text.byteLength);
      bytes.set(text);
      return bytes;
    }),
    // The content is already a decoded `AgentContent`; re-encoding it cannot fail, so a
    // failure here is a broken invariant rather than an outcome a caller handles.
    Effect.orDie,
  );

/**
 * The content-addressed {@link AgentId} for an agent's content — see the module
 * docstring for why identity is derived rather than chosen.
 *
 * `crypto.subtle` is the Web-standard global (Bun-native, never `node:*`), the same
 * source the store's generation identity is minted from. A SHA-256 digest over
 * well-formed bytes cannot fail, so the promise is taken as total; the derived id is
 * decoded through the owned {@link AgentId} schema rather than asserted (INV-NOCAST).
 */
export const agentIdFor = (content: AgentContent): Effect.Effect<AgentId> =>
  Effect.gen(function* () {
    const bytes = yield* canonicalBytes(content);
    const digest = yield* Effect.promise(() => globalThis.crypto.subtle.digest("SHA-256", bytes));
    const hex = Encoding.encodeHex(new Uint8Array(digest));
    return yield* Schema.decodeUnknownEffect(AgentId)(`agt-${hex.slice(0, DIGEST_CHARS)}`);
  }).pipe(Effect.orDie);

/**
 * Register the agent revision an execution is about to run, and answer the
 * {@link AgentId} the {@link Execution} must reference.
 *
 * Idempotent by construction: the id is derived from the content, so a re-dispatch
 * re-appends a byte-identical revision, which the registry answers as `"unchanged"` —
 * no new row, and (through the daemon's journaling decorator) no redundant
 * `AgentChanged` delta. A CHANGED agent derives a new id and lands as a new revision.
 */
export const registerAgent = (
  store: Context.Service.Shape<typeof StateStore>,
  content: AgentContent,
): Effect.Effect<AgentId, StateStoreError> =>
  Effect.gen(function* () {
    const id = yield* agentIdFor(content);
    const agent: Agent = { id, ...content };
    yield* store.agents.putAgent(agent);
    return id;
  });
