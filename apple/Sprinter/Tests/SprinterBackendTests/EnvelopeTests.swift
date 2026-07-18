import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("RPC envelope framing")
struct EnvelopeTests {
  private func encode(_ frame: ClientFrame) throws -> [String: JSONValue] {
    let data = try JSONEncoder().encode(frame)
    guard case .object(let object) = try JSONDecoder().decode(JSONValue.self, from: data) else {
      throw BackendError.malformedResponse
    }
    return object
  }

  @Test("a Request encodes the envelope tag, id, rpc tag, payload and headers")
  func encodesRequest() throws {
    let object = try encode(
      .request(id: RequestId("3"), tag: "control", payload: .object(["k": .string("v")])))
    #expect(object["_tag"] == .string("Request"))
    #expect(object["id"] == .string("3"))
    #expect(object["tag"] == .string("control"))
    #expect(object["payload"] == .object(["k": .string("v")]))
    #expect(object["headers"] == .array([]))
  }

  @Test("a void-payload Request omits the payload key")
  func encodesVoidPayloadRequest() throws {
    let object = try encode(.request(id: RequestId("0"), tag: "snapshot", payload: nil))
    #expect(object["payload"] == nil)
    #expect(object["tag"] == .string("snapshot"))
  }

  @Test("Ack and Interrupt carry the requestId")
  func encodesAckAndInterrupt() throws {
    #expect(try encode(.ack(requestId: RequestId("1")))["requestId"] == .string("1"))
    #expect(try encode(.ack(requestId: RequestId("1")))["_tag"] == .string("Ack"))
    #expect(try encode(.interrupt(requestId: RequestId("2")))["_tag"] == .string("Interrupt"))
  }

  @Test("Eof and Ping carry only the envelope tag")
  func encodesEofAndPing() throws {
    #expect(try encode(.eof)["_tag"] == .string("Eof"))
    #expect(try encode(.ping)["_tag"] == .string("Ping"))
  }

  @Test("RequestId decodes from a string or a number")
  func decodesRequestId() throws {
    #expect(
      try JSONDecoder().decode(RequestId.self, from: Data(#""abc""#.utf8)) == RequestId("abc"))
    #expect(try JSONDecoder().decode(RequestId.self, from: Data("42".utf8)) == RequestId("42"))
  }

  @Test("a non string/number RequestId is a decode failure")
  func rejectsNonScalarRequestId() {
    #expect(throws: DecodingError.self) {
      try JSONDecoder().decode(RequestId.self, from: Data("true".utf8))
    }
  }

  @Test("a Chunk frame decodes its requestId and values")
  func decodesChunk() throws {
    let data = Data(#"{"_tag":"Chunk","requestId":"5","values":[1,2]}"#.utf8)
    guard
      case .chunk(let requestId, let values) =
        try JSONDecoder().decode(ServerFrame.self, from: data)
    else {
      Issue.record("expected a chunk frame")
      return
    }
    #expect(requestId == RequestId("5"))
    #expect(values == [.number(1), .number(2)])
  }

  @Test("an Exit failure decodes each cause variant")
  func decodesExitCauses() throws {
    let cause =
      #"[{"_tag":"Die","defect":"x"},"#
      + #"{"_tag":"Interrupt","fiberId":3},"#
      + #"{"_tag":"Fail","error":{"_tag":"PlanRejected","reason":"no"}}]"#
    let json = #"{"_tag":"Exit","requestId":"9","exit":{"_tag":"Failure","cause":\#(cause)}}"#
    guard
      case .exit(_, let exit) = try JSONDecoder().decode(ServerFrame.self, from: Data(json.utf8)),
      case .failure(let causes) = exit
    else {
      Issue.record("expected an exit failure")
      return
    }
    #expect(causes.count == 3)
  }

  @Test("Defect, Pong and ClientProtocolError frames decode")
  func decodesProtocolFrames() throws {
    #expect(throws: Never.self) {
      _ = try JSONDecoder().decode(ServerFrame.self, from: Data(#"{"_tag":"Pong"}"#.utf8))
      _ = try JSONDecoder().decode(
        ServerFrame.self, from: Data(#"{"_tag":"Defect","defect":1}"#.utf8))
      _ = try JSONDecoder().decode(
        ServerFrame.self, from: Data(#"{"_tag":"ClientProtocolError","error":"x"}"#.utf8))
    }
  }

  @Test("an unknown server frame tag is a decode failure")
  func rejectsUnknownServerTag() {
    #expect(throws: DecodingError.self) {
      try JSONDecoder().decode(ServerFrame.self, from: Data(#"{"_tag":"Mystery"}"#.utf8))
    }
  }

  @Test("an unknown exit tag is a decode failure")
  func rejectsUnknownExitTag() {
    let data = Data(#"{"_tag":"Exit","requestId":"1","exit":{"_tag":"Huh"}}"#.utf8)
    #expect(throws: DecodingError.self) {
      try JSONDecoder().decode(ServerFrame.self, from: data)
    }
  }

  @Test("an unknown cause tag is a decode failure")
  func rejectsUnknownCauseTag() {
    let data = Data(
      #"{"_tag":"Exit","requestId":"1","exit":{"_tag":"Failure","cause":[{"_tag":"Nope"}]}}"#.utf8)
    #expect(throws: DecodingError.self) {
      try JSONDecoder().decode(ServerFrame.self, from: data)
    }
  }
}
