import Foundation
import Testing

@testable import SprinterContract

@Suite("JSONValue")
struct JSONValueTests {
  @Test("decodes every JSON kind from the contract Schema.Unknown golden")
  func decodesEveryKind() throws {
    let values = try Golden.decode([JSONValue].self, from: "json-values")
    #expect(values.count == 8)
    #expect(values[0] == .null)
    #expect(values[1] == .bool(true))
    #expect(values[2] == .bool(false))
    #expect(values[3] == .number(42))
    #expect(values[4] == .number(3.5))
    #expect(values[5] == .string("text"))
    #expect(values[6] == .array([.number(1), .number(2), .number(3)]))
    #expect(
      values[7]
        == .object([
          "key": .string("value"),
          "nested": .object(["flag": .null])
        ]))
  }

  @Test("round-trips every JSON kind through encode/decode")
  func roundTripsEveryKind() throws {
    let values = try Golden.decode([JSONValue].self, from: "json-values")
    for value in values {
      #expect(try Golden.roundTrip(value) == value)
    }
  }
}
