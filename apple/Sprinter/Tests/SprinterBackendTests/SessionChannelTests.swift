import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Session channel verbs")
struct SessionChannelTests {
  @Test("sessionEvents unwraps the OffsetSessionEvent envelope and streams events until Exit")
  func sessionEventsStream() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task { () -> [SessionEvent] in
      var events: [SessionEvent] = []
      for try await event in backend.sessionEvents(sessionId: Fixtures.sessionId) {
        events.append(event)
      }
      return events
    }

    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "sessionEvents")
    // The request sends a PRESENT payload with the `sinceOffset` key OMITTED (→ origin
    // replay of the durable transcript), matching the canonical Effect client.
    let expectedPayload = try toJSONValue(SessionEventsPayload(sessionId: Fixtures.sessionId))
    #expect(request.payload == expectedPayload)
    let id = try #require(request.id)

    // The wire carries the OffsetSessionEvent envelope: the backend UNWRAPS
    // `.event` and yields it to the existing SessionEvent consumer.
    transport.emit(
      Wire.chunk(
        requestId: id,
        values: [
          try Wire.encoded(OffsetSessionEvent(offset: 1, event: .turnStarted)),
          try Wire.encoded(OffsetSessionEvent(offset: 2, event: Fixtures.uiRequestEvent))
        ]))
    transport.emit(Wire.exitSuccessVoid(requestId: id))
    #expect(try await collector.value == [.turnStarted, Fixtures.uiRequestEvent])
    transport.close()
  }

  @Test("sessionEvents surfaces the mirrored SessionNotFound off a failure Exit")
  func sessionEventsSessionNotFound() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    // The contract declares `error: SessionNotFound` on the sessionEvents STREAM —
    // a failure Exit must surface the mirrored ContractError through the stream.
    let collector = Task { () -> [SessionEvent] in
      var events: [SessionEvent] = []
      for try await event in backend.sessionEvents(sessionId: Fixtures.sessionId) {
        events.append(event)
      }
      return events
    }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(
      Wire.exitFail(requestId: id, error: #"{"_tag":"SessionNotFound","id":"sess-1"}"#))

    await #expect(throws: ContractError.sessionNotFound(id: Fixtures.sessionId)) {
      _ = try await collector.value
    }
    transport.close()
  }

  @Test("sessionSend sends the input payload and resolves on void success")
  func sessionSend() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task {
      try await backend.sessionSend(sessionId: Fixtures.sessionId, input: Fixtures.sessionInput)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "sessionSend")
    let expected = try toJSONValue(
      SessionSendPayload(sessionId: Fixtures.sessionId, input: Fixtures.sessionInput))
    #expect(try #require(request.payload) == expected)
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(request.id)))
    try await task.value
    transport.close()
  }

  @Test("interrupt sends its payload and resolves on void success")
  func interrupt() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.interrupt(sessionId: Fixtures.sessionId) }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "interrupt")
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(request.id)))
    try await task.value
    transport.close()
  }

  @Test("answerUiRequest sends the UiResponse and resolves on void success")
  func answerUiRequest() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let response = UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true))
    let task = Task {
      try await backend.answerUiRequest(sessionId: Fixtures.sessionId, response: response)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "answerUiRequest")
    let expected = try toJSONValue(
      AnswerUiRequestPayload(sessionId: Fixtures.sessionId, response: response))
    #expect(try #require(request.payload) == expected)
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(request.id)))
    try await task.value
    transport.close()
  }

  @Test("each out verb surfaces the mirrored SessionNotFound off a Fail cause")
  func sessionNotFound() async throws {
    let notFound = #"{"_tag":"SessionNotFound","id":"sess-1"}"#
    let expected = ContractError.sessionNotFound(id: Fixtures.sessionId)

    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.sessionSend(sessionId: Fixtures.sessionId, input: Fixtures.sessionInput)
    }
    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.interrupt(sessionId: Fixtures.sessionId)
    }
    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.answerUiRequest(
        sessionId: Fixtures.sessionId,
        response: UiResponse(requestId: "req-1", answer: .cancelled))
    }
  }

  /// Runs one out verb against a `Fail` exit and asserts the mirrored error.
  private func expectFailure(
    _ expected: ContractError,
    forFail error: String,
    _ verb: @escaping @Sendable (RpcBackend) async throws -> Void
  ) async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await verb(backend) }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.exitFail(requestId: id, error: error))
    await #expect(throws: expected) { try await task.value }
    transport.close()
  }
}
