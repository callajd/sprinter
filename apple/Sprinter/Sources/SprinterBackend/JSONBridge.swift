import Foundation
import SprinterContract

/// Bridges typed `Codable` values to/from the neutral ``JSONValue`` used inside
/// the envelope (payloads and success/error values).
///
/// Both helpers wrap the value in a single-element array before encoding so the
/// top level is always a JSON array — sidestepping top-level JSON *fragment*
/// handling for bare-string values such as a branded `WorkstreamId`.

/// Encodes any `Encodable` payload/value into a ``JSONValue``.
func toJSONValue<Value: Encodable>(_ value: Value) throws -> JSONValue {
  let data = try JSONEncoder().encode([value])
  let wrapped = try JSONDecoder().decode([JSONValue].self, from: data)
  guard let first = wrapped.first else {
    throw BackendError.malformedResponse
  }
  return first
}

/// Decodes a ``JSONValue`` (a success or error payload lifted off an `Exit`) into
/// the expected `Decodable` type.
func fromJSONValue<Value: Decodable>(_ type: Value.Type, _ json: JSONValue) throws -> Value {
  let data = try JSONEncoder().encode([json])
  let decoded = try JSONDecoder().decode([Value].self, from: data)
  guard let first = decoded.first else {
    throw BackendError.malformedResponse
  }
  return first
}
