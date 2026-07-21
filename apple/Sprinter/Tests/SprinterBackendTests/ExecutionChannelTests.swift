import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Execution channel verbs")
struct ExecutionChannelTests {
  @Test("executionEvents unwraps the dual-modality envelope (durable + ephemeral) until Exit")
  func executionEventsStream() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task { () -> [ExecutionEvent] in
      var events: [ExecutionEvent] = []
      for try await event in backend.executionEvents(executionId: Fixtures.executionId) {
        events.append(event)
      }
      return events
    }

    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "executionEvents")
    // The request sends a PRESENT payload with the `sinceOffset` key OMITTED (→ origin
    // replay of the durable transcript), matching the canonical Effect client.
    let expectedPayload = try toJSONValue(ExecutionEventsPayload(executionId: Fixtures.executionId))
    #expect(request.payload == expectedPayload)
    let id = try #require(request.id)

    // The wire carries the OffsetExecutionEvent envelope — ONE channel with BOTH
    // modalities: a DURABLE entry offset-STAMPED, EPHEMERAL deltas offset-LESS. The backend
    // UNWRAPS `.event` and yields EVERY event (offset dropped) to the ExecutionEvent consumer.
    let durableEntry: ExecutionEvent = .entryAppended(
      entry: .assistantMessage(id: "a1", text: "on it", reasoning: nil))
    transport.emit(
      Wire.chunk(
        requestId: id,
        values: [
          try Wire.encoded(OffsetExecutionEvent(event: .turnStarted)),  // ephemeral, offset-less
          try Wire.encoded(OffsetExecutionEvent(offset: 2, event: durableEntry)),  // durable
          try Wire.encoded(OffsetExecutionEvent(event: Fixtures.uiRequestEvent))  // ephemeral
        ]))
    transport.emit(Wire.exitSuccessVoid(requestId: id))
    #expect(try await collector.value == [.turnStarted, durableEntry, Fixtures.uiRequestEvent])
    transport.close()
  }

  @Test("executionEvents surfaces the mirrored ExecutionNotFound off a failure Exit")
  func executionEventsExecutionNotFound() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    // The contract declares `error: ExecutionNotFound` on the executionEvents STREAM —
    // a failure Exit must surface the mirrored ContractError through the stream.
    let collector = Task { () -> [ExecutionEvent] in
      var events: [ExecutionEvent] = []
      for try await event in backend.executionEvents(executionId: Fixtures.executionId) {
        events.append(event)
      }
      return events
    }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(
      Wire.exitFail(requestId: id, error: #"{"_tag":"ExecutionNotFound","id":"sess-1"}"#))

    await #expect(throws: ContractError.executionNotFound(id: Fixtures.executionId)) {
      _ = try await collector.value
    }
    transport.close()
  }

  @Test("executionSend sends the input payload and resolves on void success")
  func executionSend() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task {
      try await backend.executionSend(
        executionId: Fixtures.executionId, input: Fixtures.executionInput)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "executionSend")
    let expected = try toJSONValue(
      ExecutionSendPayload(executionId: Fixtures.executionId, input: Fixtures.executionInput))
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

    let task = Task { try await backend.interrupt(executionId: Fixtures.executionId) }
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
      try await backend.answerUiRequest(executionId: Fixtures.executionId, response: response)
    }
    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "answerUiRequest")
    let expected = try toJSONValue(
      AnswerUiRequestPayload(executionId: Fixtures.executionId, response: response))
    #expect(try #require(request.payload) == expected)
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(request.id)))
    try await task.value
    transport.close()
  }

  @Test("each out verb surfaces the mirrored ExecutionNotFound off a Fail cause")
  func executionNotFound() async throws {
    let notFound = #"{"_tag":"ExecutionNotFound","id":"sess-1"}"#
    let expected = ContractError.executionNotFound(id: Fixtures.executionId)

    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.executionSend(
        executionId: Fixtures.executionId, input: Fixtures.executionInput)
    }
    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.interrupt(executionId: Fixtures.executionId)
    }
    try await expectFailure(expected, forFail: notFound) { backend in
      try await backend.answerUiRequest(
        executionId: Fixtures.executionId,
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
