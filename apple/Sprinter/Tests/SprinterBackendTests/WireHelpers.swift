import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

/// Helpers to build server → client frame bytes and to read the client's
/// outbound frames — kept as literal JSON so the tests exercise the client's
/// decoder against representative on-the-wire bytes.

/// A decoded view of an outbound client frame, for correlation assertions.
struct SentFrame: Decodable {
  let envelopeTag: String
  let id: String?
  let requestId: String?
  let rpcTag: String?
  let payload: JSONValue?

  private enum CodingKeys: String, CodingKey {
    case envelopeTag = "_tag"
    case id
    case requestId
    case rpcTag = "tag"
    case payload
  }
}

enum Wire {
  /// Frames a JSON message with the trailing newline the NDJSON transport uses.
  static func line(_ json: String) -> Data {
    Data((json + "\n").utf8)
  }

  /// Encodes any DTO to a compact JSON string for embedding in a frame.
  static func encoded<Value: Encodable>(_ value: Value) throws -> String {
    let data = try JSONEncoder().encode([value])
    let string = try #require(String(data: data, encoding: .utf8))
    // Strip the single-element array wrapper: `[<json>]` -> `<json>`.
    return String(string.dropFirst().dropLast())
  }

  /// Wraps an already-serialized `exit` object into an `Exit` frame.
  static func exit(requestId: String, exit: String) -> Data {
    line(#"{"_tag":"Exit","requestId":"\#(requestId)","exit":\#(exit)}"#)
  }

  /// Wraps a single serialized cause entry into a `Failure` exit frame.
  static func failureExit(requestId: String, cause: String) -> Data {
    exit(requestId: requestId, exit: #"{"_tag":"Failure","cause":[\#(cause)]}"#)
  }

  static func exitSuccess(requestId: String, value: String) -> Data {
    exit(requestId: requestId, exit: #"{"_tag":"Success","value":\#(value)}"#)
  }

  static func exitSuccessVoid(requestId: String) -> Data {
    exit(requestId: requestId, exit: #"{"_tag":"Success"}"#)
  }

  static func exitFail(requestId: String, error: String) -> Data {
    failureExit(requestId: requestId, cause: #"{"_tag":"Fail","error":\#(error)}"#)
  }

  static func exitDie(requestId: String) -> Data {
    failureExit(requestId: requestId, cause: #"{"_tag":"Die","defect":"boom"}"#)
  }

  static func exitInterrupt(requestId: String) -> Data {
    failureExit(requestId: requestId, cause: #"{"_tag":"Interrupt","fiberId":7}"#)
  }

  static func chunk(requestId: String, values: [String]) -> Data {
    line(
      #"{"_tag":"Chunk","requestId":"\#(requestId)","values":[\#(values.joined(separator: ","))]}"#)
  }

  static func pong() -> Data {
    line(#"{"_tag":"Pong"}"#)
  }

  static func defect() -> Data {
    line(#"{"_tag":"Defect","defect":"kaput"}"#)
  }
}

/// Reads and decodes the next outbound frame the client sent.
func nextSent(_ iterator: inout AsyncStream<Data>.Iterator) async throws -> SentFrame {
  var data = try #require(await iterator.next())
  if data.last == 0x0A {
    data.removeLast()
  }
  return try JSONDecoder().decode(SentFrame.self, from: data)
}

/// Reads the two requests ONE subscribe-around-snapshot attempt issues (`events` +
/// `snapshot`) and indexes them by rpc tag. The two are sent on the connection's
/// serialized path but their ARRIVAL order is not wire-guaranteed, so a test that
/// asserted a fixed order would be asserting an implementation detail; indexing by tag
/// asserts only what the contract fixes — that both were issued.
func requestsByTag(
  _ outbound: inout AsyncStream<Data>.Iterator
) async throws -> [String: SentFrame] {
  var byTag: [String: SentFrame] = [:]
  for _ in 0..<2 {
    let frame = try await nextSent(&outbound)
    if let tag = frame.rpcTag {
      byTag[tag] = frame
    }
  }
  return byTag
}
