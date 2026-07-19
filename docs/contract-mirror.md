# Swift contract mirror (FE2.4) — mapping & regeneration

The Swift `SprinterContract` module (`apple/Sprinter/Sources/SprinterContract/`)
is a **hand-written mirror** of the merged RPC contract v1
(`packages/contract` over `packages/domain`). It is a **foreign consumer** (D10):
it cannot share the Effect `Schema` types, so it re-declares the wire message
shapes as Swift `Codable` DTOs and is decode-tested against **golden JSON emitted
by the TypeScript contract itself** (INV-CONTRACT). The module is
**platform-neutral** — Foundation only, no `AppKit`/`UIKit` — so the macOS (and
later iOS) client shells consume it as a plain SwiftPM library.

This is the **divergence gate**: once this mirror decodes contract v1, Track A
(daemon) and Track B (UI) proceed against the frozen contract.

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
- The contract version is a compile-time marker, not a wire field; the mirror
  tracks it as `SprinterContract.version` (currently `3`).

## What the gate checks

`make check` (run from `apple/Sprinter/`) **only DECODES the committed goldens** —
there is no `bun`/Node dependency inside the Swift gate. The decode tests
(`Tests/SprinterContractTests/`) cover: every DTO decodes; every tagged-union
variant (and both optional-present / optional-absent forms) decodes; every value
round-trips (decode → encode → decode); and every public initializer builds a
value equal to the decoded golden (the send direction).

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

3. **Bump** `SprinterContract.version` if the contract's `CONTRACT_VERSION` bumped.

4. **Run the gate** and drive it green:

   ```sh
   cd apple/Sprinter && make check
   ```

The generator is a one-off developer tool; it is **not** part of `make check` and
never runs in the Swift CI job.
