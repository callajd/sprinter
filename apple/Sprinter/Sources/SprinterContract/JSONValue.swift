/// A neutral JSON value — the Swift mirror of the contract's `Schema.Unknown`
/// tool-payload fields (`input` / `output` / `partial` on the tool events and
/// transcript entries).
///
/// The session model is deliberately agnostic to a tool's payload shape, so the
/// mirror carries those fields as an arbitrary, losslessly-decodable JSON tree
/// rather than a fixed struct. A small owned enum (rather than a third-party
/// `AnyCodable`) keeps the frozen contract surface dependency-free and auditable
/// — the whole shape is one file.
public enum JSONValue: Codable, Equatable, Sendable {
  case null
  case bool(Bool)
  // All JSON numbers decode to `Double`: the integer/float distinction is not
  // preserved and integers above 2^53 lose precision. These tool payloads are
  // receive-only and opaque (rendered, not re-serialized), so this is acceptable;
  // add an integer case if a consumer ever needs lossless large-integer round-trip.
  case number(Double)
  case string(String)
  case array([JSONValue])
  case object([String: JSONValue])

  public init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() {
      self = .null
    } else if let value = try? container.decode(Bool.self) {
      self = .bool(value)
    } else if let value = try? container.decode(Double.self) {
      self = .number(value)
    } else if let value = try? container.decode(String.self) {
      self = .string(value)
    } else if let value = try? container.decode([JSONValue].self) {
      self = .array(value)
    } else if let value = try? container.decode([String: JSONValue].self) {
      self = .object(value)
    } else {
      throw DecodingError.dataCorruptedError(
        in: container,
        debugDescription: "Value is not representable JSON"
      )
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .null: try container.encodeNil()
    case .bool(let value): try container.encode(value)
    case .number(let value): try container.encode(value)
    case .string(let value): try container.encode(value)
    case .array(let value): try container.encode(value)
    case .object(let value): try container.encode(value)
    }
  }
}
