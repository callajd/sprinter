import Foundation
import Testing

@testable import SprinterContract

/// TS-emitted golden ≡ Swift re-encode (issue #89) — the ENCODE direction, which nothing
/// in this repo asserted before.
///
/// Every other golden test is Swift → Swift: `Golden.roundTrip` encodes with the same
/// conventions it decodes with, so a mirror emitting `"supersedes": null` where the
/// contract OMITS the key passes it and is rejected by the daemon. `SprinterContract`'s
/// documented claim — "`optionalKey` fields are OMITTED when absent (not `null`)"
/// (`docs/contract-mirror.md`) — was therefore asserted nowhere.
///
/// This suite asserts it, against the real contract's own output: decode a golden,
/// re-encode it, parse BOTH, and require the normalised structures to match. The
/// TypeScript side is authoritative (D1); the comparison is over parsed JSON, so key order
/// and whitespace are free while key PRESENCE is not (D2); and it runs over EVERY golden,
/// which covers every mirrored type carrying an optional field and more (D3).
///
/// The guard is proved to fire by ``rejectsNullWhereTheContractOmitsTheKey()`` below.
@Suite("Encode agreement (TS golden ≡ Swift re-encode)")
struct EncodeAgreementTests {
  @Test(
    "re-encoding a golden reproduces the JSON the TypeScript contract emitted",
    arguments: GoldenCase.all
  )
  func reencodeMatchesGolden(_ goldenCase: GoldenCase) throws {
    let divergence = try goldenCase.divergence(Golden.data(goldenCase.name))
    #expect(
      divergence == nil,
      """
      \(goldenCase.name).json: the Swift mirror's encode output is NOT what the \
      TypeScript contract emits — \(divergence ?? "")
      """
    )
  }

  /// The scope guard (D3). Pairing a golden with its mirror type is knowledge only
  /// `GoldenCase.all` holds, so a golden missing from it would simply never be checked in
  /// the encode direction — silently, and exactly for a NEWLY MIRRORED type, which is when
  /// this matters most. Epics DE2–DE4 add `Execution`, `Session`, `Workspace`,
  /// `PullRequest`, `Spec`, `SpecRevision` and the transcript variants under the same
  /// invariant, so the table is required to be EXACTLY the bundle's goldens: add a golden
  /// without adding its case here and this fails. (`Session` here is DE2.4's UNIT OF WORK —
  /// a forward reference to a type that does not exist yet, not the process-level type #103
  /// renamed to `Execution`. Do not rename it.)
  ///
  /// This pins the NAME set only. The TYPE each name is paired with is pinned from the
  /// other side — every `Golden.decode` call site checks the type it asks for against the
  /// table (``GoldenCase/requireDeclaredType(_:for:sourceLocation:)``) — because a case
  /// retyped to something structural like `JSONValue` would re-encode to itself and pass
  /// the encode test vacuously while still counting as covered here.
  @Test("every committed golden is covered in the encode direction — none is skipped")
  func everyGoldenIsCovered() throws {
    let covered = Set(GoldenCase.all.map(\.name))
    let committed = try Golden.allNames()
    #expect(
      committed.subtracting(covered).isEmpty,
      "golden(s) with no encode-agreement case: \(committed.subtracting(covered).sorted())"
    )
    #expect(
      covered.subtracting(committed).isEmpty,
      "encode-agreement case(s) naming no golden: \(covered.subtracting(committed).sorted())"
    )
    // Each case is one FILE, so a duplicated name would quietly shrink the covered set.
    #expect(covered.count == GoldenCase.all.count)
  }

  /// **The whole point, as an automated negative fixture rather than a demonstration.**
  ///
  /// `NegativeFixtures/agent-null-supersedes.json` is `Goldens/agent-original.json` plus
  /// `"supersedes": null` — a shape the contract never emits, and precisely the one a Swift
  /// mirror would produce if `Optional` encoded as an explicit null instead of an omitted
  /// key. The harness must REJECT it.
  ///
  /// A check that only ever passes is not a check (the `SCHEMA_LEDGER` lesson from #85).
  /// The single change that would render this whole suite vacuous — normalising a missing
  /// key to `null` so the two compare equal — is the change this test fails on, forever,
  /// in CI. It also pins the DIRECTION of the report: the golden carries the key, the
  /// Swift re-encode omits it.
  @Test("REJECTS a null where the contract omits the key — omitted ≠ null")
  func rejectsNullWhereTheContractOmitsTheKey() throws {
    let divergence = try Golden.encodeDivergence(
      Agent.self, in: Golden.negativeFixture("agent-null-supersedes"))
    let reported = try #require(
      divergence,
      """
      the encode-agreement harness accepted "supersedes": null against a mirror that \
      omits the key. Omitted and null are DIFFERENT on this contract; a normalisation \
      that conflates them defeats the whole suite while appearing to pass.
      """
    )
    // The DIRECTION, in full. The bare word `OMITS` appears in BOTH of the divergence
    // messages ("the Swift re-encode OMITS it" and "the golden OMITS it"), so matching on
    // it alone would pass whichever side had dropped the key — i.e. it pins nothing. The
    // phrase that distinguishes them is the assertion, and the opposite phrase is
    // asserted ABSENT so a message carrying both could not satisfy this either.
    #expect(reported.contains("supersedes"))
    #expect(reported.contains("the golden CARRIES this key"))
    #expect(reported.contains("the Swift re-encode OMITS it"))
    #expect(!reported.contains("the golden OMITS it"))

    // And the fixture is wrong ONLY in that one way: the same bytes minus the null key
    // are a golden that agrees. So the rejection is attributable to the null, not to some
    // unrelated defect in the fixture.
    #expect(try Golden.encodeDivergence(Agent.self, in: Golden.data("agent-original")) == nil)
  }

  /// The same asymmetry at the level of the comparison itself, independent of any fixture
  /// or DTO: `{}` and `{ "k": null }` are DIFFERENT documents, in both directions, and a
  /// `null` nested inside a value is not the same as that key being absent.
  ///
  /// `NormalisedJSON` represents an omitted key as absent from the object's dictionary and
  /// a `null` as a PRESENT `JSONValue.null`, so the difference is structural rather than a
  /// rule that could be relaxed. This pins it directly.
  @Test("the normalisation distinguishes an omitted key from an explicit null")
  func omittedKeyIsNotNull() throws {
    let absent = try NormalisedJSON.parse(Data(#"{"a":1}"#.utf8))
    let explicitNull = try NormalisedJSON.parse(Data(#"{"a":1,"k":null}"#.utf8))

    #expect(NormalisedJSON.divergence(golden: absent, reencoded: explicitNull) != nil)
    #expect(NormalisedJSON.divergence(golden: explicitNull, reencoded: absent) != nil)
    // Identity still holds in both shapes — the comparison is not simply always failing.
    #expect(NormalisedJSON.divergence(golden: absent, reencoded: absent) == nil)
    #expect(NormalisedJSON.divergence(golden: explicitNull, reencoded: explicitNull) == nil)
    // Nested, where a shallow key-set comparison would miss it.
    let nestedAbsent = try NormalisedJSON.parse(Data(#"{"o":{}}"#.utf8))
    let nestedNull = try NormalisedJSON.parse(Data(#"{"o":{"k":null}}"#.utf8))
    #expect(NormalisedJSON.divergence(golden: nestedAbsent, reencoded: nestedNull) != nil)
  }

  /// What the comparison deliberately does NOT care about (D2): key ORDER and WHITESPACE
  /// are serializer detail, not contract. Were they compared, the suite would fail for
  /// reasons that mean nothing — `JSONEncoder` does not preserve the contract's
  /// declaration order — and the pressure to "fix" that by canonicalising is exactly what
  /// would erase the null-vs-omitted difference above.
  @Test("key order and whitespace are not contract; value shape and presence are")
  func ignoresKeyOrderAndWhitespace() throws {
    let one = try NormalisedJSON.parse(Data(#"{"a":1,"b":[2,3]}"#.utf8))
    let other = try NormalisedJSON.parse(Data("{ \"b\" : [ 2, 3 ],\n  \"a\" : 1 }".utf8))
    #expect(NormalisedJSON.divergence(golden: one, reencoded: other) == nil)

    // ARRAY order, by contrast, IS contract — it carries meaning (stream order, the
    // ref list's sort, a lineage's presentation order), so it is compared positionally.
    let reordered = try NormalisedJSON.parse(Data(#"{"a":1,"b":[3,2]}"#.utf8))
    #expect(NormalisedJSON.divergence(golden: one, reencoded: reordered) != nil)
    // A differing element COUNT is reported rather than silently truncating the walk.
    let shorter = try NormalisedJSON.parse(Data(#"{"a":1,"b":[2]}"#.utf8))
    #expect(NormalisedJSON.divergence(golden: one, reencoded: shorter) != nil)
  }
}
