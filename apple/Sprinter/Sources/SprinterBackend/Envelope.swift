import Foundation
import SprinterContract

/// The `effect/unstable/rpc` transport **envelope** — the wire framing around the
/// already-mirrored `SprinterContract` message DTOs (BE1.1 / INV-CONTRACT).
///
/// `docs/contract-mirror.md` scopes the `SprinterContract` mirror to message
/// *bodies*; this file adds the envelope the mirror deliberately does **not**
/// cover. The shapes below mirror the transport-encoded messages in
/// `effect/unstable/rpc/RpcMessage.ts` (`FromClientEncoded` / `FromServerEncoded`)
/// as serialized by `RpcSerialization.ndjson`. An unrecognized `_tag` is a decode
/// failure (a frame we do not understand), never a silent drop.

/// A request identifier that correlates a `Request` with its `Chunk`/`Exit`
/// responses. The wire type is `string | number` (`RpcMessage.RequestId`); the
/// client generates string ids and tolerates a numeric id echoed back.
public struct RequestId: Hashable, Sendable, Codable {
  /// The canonical string form used as the correlation key.
  public let value: String

  public init(_ value: String) {
    self.value = value
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if let string = try? container.decode(String.self) {
      value = string
    } else if let integer = try? container.decode(Int.self) {
      value = String(integer)
    } else if let number = try? container.decode(Double.self), let integer = Int(exactly: number) {
      // A JSON number id is normalized to its INTEGER string so it matches the
      // client's minted integer-string keys. A non-integral number (e.g. `1.5`)
      // would stringify to a key that can never correlate — reject it rather than
      // silently drop the response (`Int(exactly:)` is nil for a fractional value).
      value = String(integer)
    } else {
      throw DecodingError.dataCorruptedError(
        in: container, debugDescription: "RequestId is neither a string nor an integer number")
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(value)
  }
}

/// Client → server envelope frames (`RpcMessage.FromClientEncoded`).
enum ClientFrame: Sendable {
  case request(id: RequestId, tag: String, payload: JSONValue?)
  case ack(requestId: RequestId)
  case interrupt(requestId: RequestId)
  case eof
  case ping
}

extension ClientFrame: Encodable {
  private enum CodingKeys: String, CodingKey {
    case envelopeTag = "_tag"
    case id
    case rpcTag = "tag"
    case payload
    case headers
    case requestId
  }

  func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .request(let id, let tag, let payload):
      try container.encode("Request", forKey: .envelopeTag)
      try container.encode(id, forKey: .id)
      try container.encode(tag, forKey: .rpcTag)
      try container.encodeIfPresent(payload, forKey: .payload)
      // `RequestEncoded.headers` is required; this client sends none.
      try container.encode([[String]](), forKey: .headers)
    case .ack(let requestId):
      try container.encode("Ack", forKey: .envelopeTag)
      try container.encode(requestId, forKey: .requestId)
    case .interrupt(let requestId):
      try container.encode("Interrupt", forKey: .envelopeTag)
      try container.encode(requestId, forKey: .requestId)
    case .eof:
      try container.encode("Eof", forKey: .envelopeTag)
    case .ping:
      try container.encode("Ping", forKey: .envelopeTag)
    }
  }
}

/// Server → client envelope frames (`RpcMessage.FromServerEncoded`).
enum ServerFrame: Sendable {
  case chunk(requestId: RequestId, values: [JSONValue])
  case exit(requestId: RequestId, exit: ExitFrame)
  case defect(JSONValue)
  case pong
  case clientProtocolError(JSONValue)
}

extension ServerFrame: Decodable {
  private enum CodingKeys: String, CodingKey {
    case envelopeTag = "_tag"
    case requestId
    case values
    case exit
    case defect
    case error
  }

  init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .envelopeTag)
    switch tag {
    case "Chunk":
      self = .chunk(
        requestId: try container.decode(RequestId.self, forKey: .requestId),
        values: try container.decode([JSONValue].self, forKey: .values))
    case "Exit":
      self = .exit(
        requestId: try container.decode(RequestId.self, forKey: .requestId),
        exit: try container.decode(ExitFrame.self, forKey: .exit))
    case "Defect":
      self = .defect(try container.decode(JSONValue.self, forKey: .defect))
    case "Pong":
      self = .pong
    case "ClientProtocolError":
      self = .clientProtocolError(try container.decode(JSONValue.self, forKey: .error))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .envelopeTag, in: container,
        debugDescription: "Unknown server frame tag: \(tag)")
    }
  }
}

/// The terminal `Exit` payload (`RpcMessage.ExitEncoded`): a success value or a
/// failure cause built of `Fail` / `Die` / `Interrupt` entries.
enum ExitFrame: Sendable {
  /// `value` is absent for a void-success RPC (the key is omitted on the wire).
  case success(value: JSONValue?)
  case failure(cause: [CauseEntry])
}

extension ExitFrame: Decodable {
  private enum CodingKeys: String, CodingKey {
    case exitTag = "_tag"
    case value
    case cause
  }

  init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .exitTag)
    switch tag {
    case "Success":
      self = .success(value: try container.decodeIfPresent(JSONValue.self, forKey: .value))
    case "Failure":
      self = .failure(cause: try container.decode([CauseEntry].self, forKey: .cause))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .exitTag, in: container,
        debugDescription: "Unknown exit tag: \(tag)")
    }
  }
}

/// One entry of a failure `cause` (`RpcMessage.ExitEncoded` failure union).
enum CauseEntry: Sendable {
  case fail(error: JSONValue)
  case die(defect: JSONValue)
  case interrupt(fiberId: Int?)
}

extension CauseEntry: Decodable {
  private enum CodingKeys: String, CodingKey {
    case causeTag = "_tag"
    case error
    case defect
    case fiberId
  }

  init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let tag = try container.decode(String.self, forKey: .causeTag)
    switch tag {
    case "Fail":
      self = .fail(error: try container.decode(JSONValue.self, forKey: .error))
    case "Die":
      self = .die(defect: try container.decode(JSONValue.self, forKey: .defect))
    case "Interrupt":
      self = .interrupt(fiberId: try container.decodeIfPresent(Int.self, forKey: .fiberId))
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .causeTag, in: container,
        debugDescription: "Unknown cause tag: \(tag)")
    }
  }
}
