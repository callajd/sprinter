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
  shape matches exactly. This is **asserted**, not merely stated: the
  encode-agreement harness below re-encodes each golden and requires an omitted key
  and a `null` key to compare as **different** (#89).
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
  already-retired revision — a lineage goes out of service once). Those rules are
  **order-independent**, because `supersedes` is a **referential** link in the
  daemon's store: a revision may only name an ALREADY-STORED predecessor, so a
  writer cannot skip a rule by appending a successor before the revision it
  supersedes. The same constraint, plus the port's rejection of a self-reference,
  makes the `supersedes` relation **acyclic by construction** — walking it backwards
  from any revision terminates at the original, and every `supersedes` a client
  receives resolves inside the collection it arrived in. Because a retirement
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
  hand a streamed item's offset back as the `sinceOffset` inside the request's `resume`
  context to resume strictly after it. Existing consumers that only need the delta unwrap `.event`.
- The **store GENERATION is explicit on the wire.** `Snapshot` carries
  `generation` (a `StoreGenerationId`, a bare JSON string), minted when the daemon's
  schema was created and destroyed with it. A durable offset only means something
  INSIDE the generation it was minted in — the store never migrates, so a
  schema-version bump drops and recreates it and restarts offsets at `1` — so both
  cursor-bearing requests (`events` and `executionEvents`) carry ONE optional-and-omitted
  `resume` object, `{ "sinceOffset": 12, "generation": "…" }`, in which BOTH fields are
  REQUIRED. The cursor and its generation are a single nested value, not two independent
  optional keys, so "a cursor without its generation" has no wire form at all and the
  daemon has no runtime pairing rule to get wrong. The ABSENCE of `resume` — never a
  particular offset value — is what makes a request an ORIGIN request; in particular
  `sinceOffset: 0` is an ordinary resume whose generation is compared like any other. A
  client retains the generation with the baseline and hands it back on every resume.
- The streamed `events` **error is `ResyncRequired`**, and `executionEvents`' error is
  `ExecutionNotFound | ResyncRequired`. It says the request's cursor does NOT belong to
  the daemon's current store generation. Detection is an IDENTITY comparison, not an
  offset inference: a cursor beyond the log's extent is a symptom, but once a new
  generation's log outgrows a stale cursor the numbers alone look perfectly resumable,
  so a mismatched `generation` is refused whatever the offsets say, offset `0` included
  (the extent check remains as a cheap secondary). An ABSENT generation is not a case
  the daemon handles, because the payload cannot express one. It carries the rejected `sinceOffset`,
  the log's `maxOffset`, and the daemon's CURRENT `generation`. The client's obligation
  is not to retry the resume but to discard **both** its retained state and its cursor
  and re-hydrate from `snapshot` — `WorkGraphResync` does exactly that. It has to be
  both: the delta model is upsert-only, so no stream of deltas can remove an entity the
  reset destroyed.
- The streamed `executionEvents` **success is the `OffsetExecutionEvent` envelope** — `{
  "offset": 7, "event": { "_tag": "EntryAppended", … } }` — not the bare `ExecutionEvent`,
  the execution-channel mirror of `OffsetEvent`. Each durable, transcript-grade execution event
  pairs with its durable per-execution offset, so a client can hand it back inside the
  request's `resume` context. A SETTLED execution's durable transcript replays and the stream
  completes (viewable in the Inspector) rather than the old `ExecutionNotFound`; the
  `executionEvents` request gains the same OPTIONAL `resume` context as `events` — the same
  type, and therefore the same guard, since its per-execution log is dropped by a schema
  bump too.
  `RpcBackend` unwraps `.event` to the existing `ExecutionEvent` consumer. Ephemeral live
  deltas ride the same channel offset-less (offset present ⇒ durable/replayable, absent ⇒
  ephemeral). **The execution feed is ORIGIN-ONLY until a resuming client exists**:
  `RpcBackend` builds the `executionEvents` payload with `executionId` and no `resume`, and
  `InteractiveExecution` has no `ResyncRequired` handling, so the generation guard on this
  feed is enforced TS-side and has no end-to-end path to fire on today. It is
  **latent-but-correct** — defined up front because the per-execution offset genuinely is a
  generation-scoped coordinate, not because it is exercised. The work-graph `events` feed
  is the one that resumes in practice.

## What the gate checks

Two gates, each covering what the other cannot.

`make check` (run from `apple/Sprinter/`) **only reads the committed goldens** —
there is no `bun`/Node dependency inside the Swift gate. The tests
(`Tests/SprinterContractTests/`) cover: every DTO decodes; every tagged-union
variant (and both optional-present / optional-absent forms) decodes; every value
round-trips (decode → encode → decode); every public initializer builds a
value equal to the decoded golden (the send direction); and **the encode direction**,
below.

### Encode agreement — TS golden ≡ Swift re-encode (#89)

The round-trip above is **Swift → Swift**: it decodes with the same conventions it
encoded with, so it cannot see a mirror whose *output* the contract would reject. A
DTO emitting `"supersedes": null` where the contract omits the key passed every test
in the repo and would be refused by the daemon.

`EncodeAgreementTests` closes that. For each golden it decodes the file into its
mirror type, re-encodes it, **parses both**, and requires the normalised structures to
match:

- **The TypeScript side is authoritative.** The golden is the reference; the Swift
  output is compared *to* it, never the reverse.
- **Parsed JSON, not bytes.** Key order and whitespace are serializer detail (Swift's
  encoder does not preserve the schema's declaration order); key **presence** and
  value shape are contract. Array order *is* compared — it carries meaning.
- **An omitted key ≠ a `null` key.** `NormalisedJSON` keeps a `null` as a value
  *present* in its object and an omitted key as simply *absent*, so the two are
  structurally distinct rather than distinct by a rule that could be relaxed.
- **Scope.** Every committed golden. `GoldenCase.all` is required to be *exactly* the
  goldens in the test bundle, so a golden added for a newly mirrored type is checked in
  the encode direction or the gate fails. The *type* each case is paired with is pinned
  from the decode side: `Golden.decode` checks the type its call site asks for against
  that same table, so a case cannot be quietly retyped to something structural (say
  `JSONValue`) that re-encodes to itself and passes vacuously while still counting as
  covered.
- **The absent forms are enforced, not enumerated** (`scripts/golden-coverage.ts`). The
  check above is only decisive for a field some golden actually OMITS, and which goldens
  exist used to be a matter of prose. After every fixture is written, the generator walks
  the schema AST alongside the JSON it produced and requires — per FIELD, not per file —
  that each `Schema.optionalKey` reachable from a golden has one golden that carries it
  **and** one that omits it, and that each tagged-union case appears in some golden.
  Adding an optional field or a union case without a fixture that pins it fails
  `check:goldens` with the field named. The property is read off the contract's own
  schemas, so there is no list to keep up to date.

The guard is proved to fire by a committed **negative fixture**, kept in
`Tests/SprinterContractTests/NegativeFixtures/` — a sibling of `Goldens/`, so the root
gate's `check:goldens` stage never tries to reconcile a file that must stay wrong.
`agent-null-supersedes.json` is `agent-original.json` plus `"supersedes": null`, and a
test asserts the harness **rejects** it. Normalising a missing key to `null` — the one
change that would make this whole suite vacuous while every test still passed — fails
there. See that directory's `README.md`.

#### The other direction: the positive control (a procedure, not a fixture)

The negative fixture proves the harness rejects a **golden** carrying a `null` the
mirror omits. The opposite defect — the **mirror** emitting a key the golden omits, the
actual `Optional`-encoding bug — cannot be a committed fixture, because the only way to
produce that output is to ship a deliberately broken DTO. It is verified instead by
mutating real product code and putting it back, which is stronger evidence anyway (it
runs the true encode path, not a stand-in). Repeat it whenever the harness or
`NormalisedJSON` is touched:

```sh
# 1. Break exactly one optional encode, in the mirror itself.
#    Sources/SprinterContract/TranscriptEntry.swift:
#      try container.encodeIfPresent(reasoning, forKey: .reasoning)
#    → try container.encode(reasoning, forKey: .reasoning)
cd apple/Sprinter && swift test --filter SprinterContractTests
# 2. EXPECT a failure naming the golden, the JSON path and the direction:
#    transcript-entries.json … $[2].reasoning: the Swift re-encode EMITS this key
#    (null); the golden OMITS it
# 3. Revert the mutation and re-run; the suite must be green again.
```

A run in which step 2 passes is the finding: the harness is not watching the encode
direction and the mutation must be diagnosed before the change lands.

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
