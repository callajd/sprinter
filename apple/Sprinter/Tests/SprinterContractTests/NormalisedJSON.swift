import Foundation

@testable import SprinterContract

/// The comparison used by the ENCODE-AGREEMENT harness (issue #89): parse two JSON
/// documents into a normalised tree and report the first place they diverge.
///
/// **Parsed JSON, not bytes (D2).** Key ORDER and WHITESPACE are not contract — the
/// TypeScript side writes `JSON.stringify(…, null, 2)` in schema-declaration order while
/// `JSONEncoder` writes its own — so a byte comparison would fail for reasons that carry
/// no meaning, and the obvious "fix" (sorting keys, canonicalising whitespace) invites the
/// same hand to normalise away the one difference this exists to catch. Key PRESENCE and
/// value SHAPE are contract, and that is exactly what a parsed tree preserves.
///
/// **An omitted key and a `null` key are DIFFERENT — the whole point.** The contract's
/// `Schema.optionalKey` fields are OMITTED when absent, never sent as `null`
/// (`docs/contract-mirror.md`), and a Swift mirror that emitted `"supersedes": null` would
/// be rejected by the daemon while passing every other test in this repo. The
/// normalisation keeps that difference REPRESENTABLE by construction: a `null` parses to
/// ``SprinterContract/JSONValue/null``, which is a value PRESENT in the enclosing object's
/// dictionary, whereas an omitted key is simply not a member of it. Nothing here maps one
/// onto the other, and ``objectDivergence(_:_:at:)`` reports each direction of that
/// asymmetry in its own words. `NegativeFixtures/agent-null-supersedes.json` is the
/// automated proof that it does.
///
/// The one thing it deliberately does NOT distinguish is integer-vs-float spelling (`12`
/// vs `12.0`): JSON has a single number type, so that is a serializer detail and not a
/// contract difference. ``SprinterContract/JSONValue`` decodes every number as `Double`,
/// which collapses the two.
enum NormalisedJSON {
  /// Parses JSON bytes into the normalised tree.
  ///
  /// ``SprinterContract/JSONValue`` is the mirror's own JSON model, already golden-tested
  /// across every JSON kind (`JSONValueTests`), so the comparison is not built on a second
  /// hand-rolled parser that could itself be wrong.
  static func parse(_ data: Data) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: data)
  }

  /// The first place `golden` (what the TypeScript contract emitted) and `reencoded` (what
  /// the Swift mirror produced from it) differ, or `nil` when they agree.
  ///
  /// The direction is not symmetric in the wording: the TypeScript side is AUTHORITATIVE
  /// (D1), so a difference is always phrased as the Swift mirror deviating from the golden.
  static func divergence(golden: JSONValue, reencoded: JSONValue) -> String? {
    divergence(golden, reencoded, at: "$")
  }

  private static func divergence(
    _ golden: JSONValue,
    _ mirror: JSONValue,
    at path: String
  ) -> String? {
    switch (golden, mirror) {
    case (.object(let golden), .object(let mirror)):
      return objectDivergence(golden, mirror, at: path)
    case (.array(let golden), .array(let mirror)):
      return arrayDivergence(golden, mirror, at: path)
    default:
      guard golden != mirror else { return nil }
      return "\(path): golden has \(golden.summary), the Swift re-encode has \(mirror.summary)"
    }
  }

  /// Compares two objects over the UNION of their keys — never just the golden's — so a key
  /// only ONE side carries is a divergence rather than something the walk never visits.
  ///
  /// This is where omission-vs-`null` is decided. A key whose value is `null` is `.some`
  /// here (it is in the dictionary); an omitted key is `.none`. The two mismatched cases
  /// below are therefore reached for exactly the pairing the harness exists to catch, and
  /// each says WHICH side carried the key so the failure names the fix.
  private static func objectDivergence(
    _ golden: [String: JSONValue],
    _ mirror: [String: JSONValue],
    at path: String
  ) -> String? {
    for key in Set(golden.keys).union(mirror.keys).sorted() {
      let childPath = "\(path).\(key)"
      switch (golden[key], mirror[key]) {
      case (.some(let golden), .some(let mirror)):
        if let divergence = divergence(golden, mirror, at: childPath) { return divergence }
      case (.some(let golden), .none):
        return "\(childPath): the golden CARRIES this key (\(golden.summary)); "
          + "the Swift re-encode OMITS it"
      case (.none, .some(let mirror)):
        return "\(childPath): the Swift re-encode EMITS this key (\(mirror.summary)); "
          + "the golden OMITS it"
      case (.none, .none):
        continue
      }
    }
    return nil
  }

  private static func arrayDivergence(
    _ golden: [JSONValue],
    _ mirror: [JSONValue],
    at path: String
  ) -> String? {
    guard golden.count == mirror.count else {
      return "\(path): golden has \(golden.count) element(s), "
        + "the Swift re-encode has \(mirror.count)"
    }
    for (index, pair) in zip(golden, mirror).enumerated() {
      if let divergence = divergence(pair.0, pair.1, at: "\(path)[\(index)]") { return divergence }
    }
    return nil
  }
}

extension JSONValue {
  /// A short, one-line rendering used in divergence messages.
  ///
  /// `null` renders as the literal `null` — and is only ever reached for a value that is
  /// PRESENT — while an absent key is reported by the word `OMITS` instead, so the two
  /// never read alike in a failure message.
  fileprivate var summary: String {
    switch self {
    case .null: return "null"
    case .bool(let value): return "\(value)"
    case .number(let value): return "\(value)"
    case .string(let value): return "\"\(value)\""
    case .array(let value): return "an array of \(value.count)"
    case .object(let value): return "an object {\(value.keys.sorted().joined(separator: ", "))}"
    }
  }
}
