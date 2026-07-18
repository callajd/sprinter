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

  /// Decodes a golden into the mirror type it represents.
  static func decode<T: Decodable>(_ type: T.Type, from name: String) throws -> T {
    try JSONDecoder().decode(type, from: data(name))
  }

  /// Encodes a value and decodes it back, proving the mirror's encode path agrees
  /// with its decode path (the wire round-trips).
  static func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    let encoded = try JSONEncoder().encode(value)
    return try JSONDecoder().decode(T.self, from: encoded)
  }
}
