# Swift contract mirror (FE2.4) — mapping & regeneration

The Swift `SprinterContract` module (`apple/Sprinter/Sources/SprinterContract/`)
is a **hand-written mirror** of the merged RPC contract
(`packages/contract` over `packages/domain`). It is a **foreign consumer** (D10):
it cannot share the Effect `Schema` types, so it re-declares the wire message
shapes as Swift `Codable` DTOs and is decode-tested against **golden JSON emitted
by the TypeScript contract itself** (INV-CONTRACT). The module is
**platform-neutral** — Foundation only, no `AppKit`/`UIKit` — so the macOS (and
later iOS) client shells consume it as a plain SwiftPM library.

This is the **divergence gate**: once this mirror decodes the contract, Track A
(daemon) and Track B (UI) proceed against the frozen contract. The goldens are the
only lockstep mechanism — there is no wire handshake;
change the wire shape without re-freezing them and the decode tests fail.

**Scope — message DTOs, not the transport envelope.** The mirror and its goldens
cover the contract's *message bodies* (`Schema.encode` output of each payload),
**not** the `RpcGroup` transport framing around them — request ids, the streaming
envelope, error-vs-success framing. That wire framing is the client's concern and
is built in Track B (the RPC client that speaks `effect/unstable/rpc` over a
transport). So "decodes the same bytes" here means the same *message* bytes, not
the full on-the-wire RPC frame.

## Wire-shape mapping (contract → Swift)

| Contract (`effect/Schema`)        | Swift mirror                                             |
| --------------------------------- | ------------------------------------------------------- |
| branded `NonEmptyString` id       | `struct …Id: StringIdentifier` (codes as a bare string) |
| `Schema.Literals([...])`          | `enum … : String, Codable` (raw value = wire token)     |
| `Schema.Struct`                   | `struct … : Codable` (memberwise + synthesized Codable) |
| `Schema.optionalKey(T)`           | Swift `Optional` (`decodeIfPresent`/`encodeIfPresent`)  |
| `Schema.TaggedUnion({...})`       | `enum` with a custom `Codable` switching on `_tag`      |
| `Schema.TaggedErrorClass`         | `ContractError` enum case (keeps the `_tag`)            |
| `Schema.Unknown` (tool payloads)  | `JSONValue` (null/bool/number/string/array/object)      |
| `Schema.Int`                      | `Int`                                                   |

Notes threaded to the contract's own decisions:

- **Tagged unions** inline a `_tag` discriminant alongside the variant fields,
  e.g. `{ "_tag": "IssueChanged", "issue": { … } }`. An unknown `_tag` is a
  **decode failure**, never a silent drop.
- **`optionalKey` fields are OMITTED when absent** (not `null`). Swift synthesized
  `Codable` maps a missing key to `nil` and omits `nil` on encode, so the wire
  shape matches exactly.
- Wire field `pr` maps to the Swift property `pullRequest` via `CodingKeys`.
- `WorkGraphEvent` is **upsert-only** — there is no `*Removed` variant (contract
  §events); a terminal status is an ordinary change.
- The **registry layer** (`Agent`, mirrored in `Sources/SprinterContract/Registry.swift`)
  rides the same two surfaces as the read model: `Snapshot.agents` and the
  `AgentChanged` delta. It is **append-only** and a stored revision is
  **immutable**, so the upsert-only rule is exact: BOTH mutating operations are an
  append under a **new id** — an edit is a new revision linked by `supersedes`, and
  a retirement is a new revision carrying **`supersedes` AND `retiredAt`** (never
  the same id restamped). There is no delete on the contract and no
  `AgentRemoved`, and a client folds `AgentChanged` as an upsert by id.
  Retired-ness is
  read off `retiredAt`'s presence; there is deliberately no `AgentStatus` enum
  (INV-SUM). A **retirement is lifecycle-only**: the retiring revision repeats the
  retired revision's `name`/`model`/`version`/`tools` verbatim and differs only in
  `id`, `supersedes` and `retiredAt` (the `StateStore` port enforces it), so the
  mirror never sees a retirement that rewrote content (nor one that retires an
  already-retired revision — a lineage goes out of service once). Because a retirement
  is a NEW revision, `Agent.isRetired` is a question about ONE RECORD: the revision it
  retires stays un-stamped forever. "Is this agent still in service" is
  `isLineageRetired(_:in:)`, mirrored on both sides, and it is the one a view wants. `retiredAt` is the domain's
  `Timestamp` — a **canonical** ISO-8601 UTC string (`YYYY-MM-DDTHH:MM:SS.sssZ`,
  always three fractional digits, always `Z`), normalised on decode so string order is
  instant order; the mirror models it as `String` and may compare two stamps directly.
- The streamed `events` **success is the `OffsetEvent` envelope** — `{ "offset":
  12, "event": { "_tag": "IssueChanged", … } }` — not the bare `WorkGraphEvent`
  (CE2.0). Each item pairs the delta with its durable `event_log`
  offset (a `NonNegativeInt`, so a bare JSON integer → Swift `Int`), so a client can
  hand a streamed item's offset back as the request's `sinceOffset` cursor to resume
  strictly after it. Existing consumers that only need the delta unwrap `.event`.
- The **store GENERATION is explicit on the wire.** `Snapshot` carries
  `generation` (a `StoreGenerationId`, a bare JSON string), minted when the daemon's
  schema was created and destroyed with it. A durable offset only means something
  INSIDE the generation it was minted in — the store never migrates, so a
  schema-version bump drops and recreates it and restarts offsets at `1` — so both
  cursor-bearing requests (`events` and `sessionEvents`) carry `generation` ALONGSIDE
  `sinceOffset`, and both keys are optional-and-omitted for an ORIGIN request. A client
  retains the generation with the baseline and hands it back on every resume.
- The streamed `events` **error is `ResyncRequired`**, and `sessionEvents`' error is
  `SessionNotFound | ResyncRequired`. It says the request's cursor does NOT belong to
  the daemon's current store generation. Detection is an IDENTITY comparison, not an
  offset inference: a cursor beyond the log's extent is a symptom, but once a new
  generation's log outgrows a stale cursor the numbers alone look perfectly resumable,
  so an absent or mismatched `generation` is refused whatever the offsets say (the
  extent check remains as a cheap secondary). It carries the rejected `sinceOffset`,
  the log's `maxOffset`, and the daemon's CURRENT `generation`. The client's obligation
  is not to retry the resume but to discard **both** its retained state and its cursor
  and re-hydrate from `snapshot` — `WorkGraphResync` does exactly that. It has to be
  both: the delta model is upsert-only, so no stream of deltas can remove an entity the
  reset destroyed.
- The streamed `sessionEvents` **success is the `OffsetSessionEvent` envelope** — `{
  "offset": 7, "event": { "_tag": "EntryAppended", … } }` — not the bare `SessionEvent`,
  the session-channel mirror of `OffsetEvent`. Each durable, transcript-grade session event
  pairs with its durable per-session offset, so a client can hand it back as the request's
  `sinceOffset` cursor. A SETTLED session's durable transcript replays and the stream
  completes (viewable in the Inspector) rather than the old `SessionNotFound`; the
  `sessionEvents` request gains the same OPTIONAL `sinceOffset` cursor as `events` —
  and the same generation guard on it, since its per-session log is dropped by a schema
  bump too.
  `RpcBackend` unwraps `.event` to the existing `SessionEvent` consumer. Ephemeral live
  deltas ride the same channel offset-less (offset present ⇒ durable/replayable, absent ⇒
  ephemeral).

## What the gate checks

Two gates, each covering what the other cannot.

`make check` (run from `apple/Sprinter/`) **only DECODES the committed goldens** —
there is no `bun`/Node dependency inside the Swift gate. The decode tests
(`Tests/SprinterContractTests/`) cover: every DTO decodes; every tagged-union
variant (and both optional-present / optional-absent forms) decodes; every value
round-trips (decode → encode → decode); and every public initializer builds a
value equal to the decoded golden (the send direction).

`bun run check` (repo root) adds the **golden-freshness** stage
(`check:goldens` → `apple/Sprinter/scripts/check-goldens.ts`): it re-runs the
generator into a temporary directory and diffs against the committed fixtures,
failing on any stale, missing, or orphaned golden. Without it the goldens could go
stale silently — the Swift gate would keep validating the mirror against a wire
shape the contract no longer emits, and INV-MIRROR's guard would guard nothing. The
check never writes into the working tree.

## Regenerating the goldens (the INV-CONTRACT ripple procedure)

When the contract (`packages/contract` or the `packages/domain` schemas it
composes) changes, the Swift mirror and its goldens must ripple:

1. **Regenerate the goldens from the TS contract** (needs `bun`; run from the
   repo root):

   ```sh
   bun run apple/Sprinter/scripts/generate-goldens.ts
   ```

   The script `Schema.encode`s representative values of every contract message
   through the real `@sprinter/contract` / `@sprinter/domain` schemas and writes
   `apple/Sprinter/Tests/SprinterContractTests/Goldens/*.json`. Encoding
   **validates** each representative value, so a drifted fixture fails loudly here.

2. **Update the Swift DTOs** in `Sources/SprinterContract/` to match any changed
   shape, and add representative values to the generator for any new message type
   or variant.

3. **Run the gate** and drive it green:

   ```sh
   cd apple/Sprinter && make check
   ```

The generator does not run inside `make check` (the Swift gate stays `bun`-free), but
forgetting step 1 is no longer silent: the root gate's `check:goldens` stage re-runs it
and fails on any difference.
