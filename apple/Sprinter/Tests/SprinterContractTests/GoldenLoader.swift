import Foundation
import Testing

/// Loads the committed golden JSON files — the wire output the TypeScript contract
/// produced (`scripts/generate-goldens.ts`) — and decodes/round-trips them through
/// the Swift mirror. The gate only DECODES these committed bytes; no bun runs here.
enum Golden {
  /// Reads a golden `.json` from the bundled `Goldens/` resource directory.
  static func data(_ name: String) throws -> Data {
    let url = try #require(
      Bundle.module.url(forResource: name, withExtension: "json", subdirectory: "Goldens"),
      "missing golden: \(name).json"
    )
    return try Data(contentsOf: url)
  }

  /// Every committed golden's name (without the `.json` extension).
  ///
  /// Read off the RESOURCE BUNDLE rather than a hand-kept list, so the completeness check
  /// in `EncodeAgreementTests` sees a new golden the moment one is committed.
  static func allNames() throws -> Set<String> {
    let urls = try #require(
      Bundle.module.urls(forResourcesWithExtension: "json", subdirectory: "Goldens"),
      "the Goldens resource directory is missing from the test bundle"
    )
    return Set(urls.map { $0.deletingPathExtension().lastPathComponent })
  }

  /// Reads a file from the bundled `NegativeFixtures/` resource directory — the
  /// DELIBERATELY WRONG documents that prove a guard fires. See
  /// `NegativeFixtures/README.md`; these are NOT goldens and are never regenerated.
  static func negativeFixture(_ name: String) throws -> Data {
    let url = try #require(
      Bundle.module.url(
        forResource: name, withExtension: "json", subdirectory: "NegativeFixtures"),
      "missing negative fixture: \(name).json"
    )
    return try Data(contentsOf: url)
  }

  /// Decodes a golden into the mirror type it represents.
  static func decode<T: Decodable>(_ type: T.Type, from name: String) throws -> T {
    try JSONDecoder().decode(type, from: data(name))
  }

  /// Encodes a value and decodes it back, proving the mirror's encode path agrees
  /// with its decode path (the wire round-trips).
  ///
  /// - Important: this is Swift → Swift. It cannot see a mirror whose encode output the
  ///   TypeScript contract would REJECT, because it decodes with the same conventions it
  ///   encoded with. ``encodeDivergence(_:in:)`` is what closes that gap (issue #89);
  ///   this one stays for what it does prove — that the two Swift paths are inverses,
  ///   including for values BUILT rather than decoded.
  static func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    let encoded = try JSONEncoder().encode(value)
    return try JSONDecoder().decode(T.self, from: encoded)
  }

  /// The ENCODE-AGREEMENT check (issue #89): decode `data` — wire JSON the TypeScript
  /// contract emitted — into its Swift mirror type, re-encode it, and report the first
  /// place the Swift output DIVERGES from the JSON it came from. `nil` means they agree.
  ///
  /// This is the direction nothing else asserts. ``roundTrip(_:)`` is Swift → Swift, so a
  /// mirror emitting `"supersedes": null` where the contract OMITS the key round-trips
  /// perfectly and is rejected by the daemon. Re-encoding the golden and comparing against
  /// the golden itself makes the TypeScript side AUTHORITATIVE (D1), and comparing PARSED
  /// trees keeps key order and whitespace out of it while keeping key PRESENCE in (D2 —
  /// see ``NormalisedJSON``).
  ///
  /// A `throw` here is a DECODE failure (the mirror cannot read the contract's output at
  /// all); a non-`nil` return is an ENCODE disagreement.
  static func encodeDivergence<T: Codable>(_ type: T.Type, in data: Data) throws -> String? {
    let decoded = try JSONDecoder().decode(type, from: data)
    let reencoded = try JSONEncoder().encode(decoded)
    return NormalisedJSON.divergence(
      golden: try NormalisedJSON.parse(data),
      reencoded: try NormalisedJSON.parse(reencoded)
    )
  }
}
