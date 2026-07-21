import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Interactive execution — UI round-trip")
@MainActor
struct InteractiveExecutionTests {
  /// The `extension_ui_request` round-trip: a `UiRequestRaised` surfacing on the
  /// feed becomes an outstanding request (correlated by id); answering it sends
  /// the neutral `UiResponse` the fake transport observes, and clears it.
  @Test("a raised UI request is answered and cleared, keyed by request id")
  func uiRoundTrip() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    let execution = InteractiveExecution(backend: backend, executionId: Fixtures.executionId)
    var outbound = transport.outbound.makeAsyncIterator()

    execution.start()
    #expect(execution.isRunning)

    let subscribe = try await nextRequest(&outbound)
    #expect(subscribe.rpcTag == "executionEvents")
    let feedId = try #require(subscribe.id)

    // The daemon raises a confirm prompt on the feed — the wire carries the
    // OffsetExecutionEvent envelope, which RpcBackend unwraps to `.event`.
    transport.emit(
      Wire.chunk(
        requestId: feedId,
        values: [try Wire.encoded(OffsetExecutionEvent(offset: 1, event: Fixtures.uiRequestEvent))])
    )
    try await settle { execution.outstandingRequests.count == 1 }
    let outstanding = try #require(execution.outstandingRequests.first)
    #expect(outstanding.id == "req-1")
    #expect(outstanding.kind == .confirm)
    #expect(outstanding.prompt == "Merge the PR?")

    // Answer it; the fake observes the correlated UiResponse.
    let answering = Task {
      try await execution.answer(requestId: "req-1", .confirmed(confirmed: true))
    }
    let answer = try await nextRequest(&outbound)
    #expect(answer.rpcTag == "answerUiRequest")
    let expected = try toJSONValue(
      AnswerUiRequestPayload(
        executionId: Fixtures.executionId,
        response: UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true))))
    #expect(try #require(answer.payload) == expected)
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(answer.id)))
    try await answering.value

    #expect(execution.outstandingRequests.isEmpty)
    #expect(execution.events == [Fixtures.uiRequestEvent])

    execution.stop()
    #expect(!execution.isRunning)
    transport.close()
  }

  @Test("send and interrupt drive input out through the execution channel")
  func sendAndInterrupt() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    let execution = InteractiveExecution(backend: backend, executionId: Fixtures.executionId)
    var outbound = transport.outbound.makeAsyncIterator()

    let sending = Task { try await execution.send(Fixtures.executionInput) }
    let sendRequest = try await nextRequest(&outbound)
    #expect(sendRequest.rpcTag == "executionSend")
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(sendRequest.id)))
    try await sending.value

    let interrupting = Task { try await execution.interrupt() }
    let interruptRequest = try await nextRequest(&outbound)
    #expect(interruptRequest.rpcTag == "interrupt")
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(interruptRequest.id)))
    try await interrupting.value

    transport.close()
  }

  /// Polls `condition` on the main actor until it holds or a bounded number of
  /// ticks elapse (the feed ingests on a main-actor task the test must yield to).
  private func settle(_ condition: () -> Bool) async throws {
    for _ in 0..<200 where !condition() {
      try await Task.sleep(for: .milliseconds(5))
    }
    #expect(condition())
  }
}

/// Reads outbound frames until the next `Request` (skipping flow-control `Ack`s).
func nextRequest(_ iterator: inout AsyncStream<Data>.Iterator) async throws -> SentFrame {
  while true {
    let frame = try await nextSent(&iterator)
    if frame.envelopeTag == "Request" {
      return frame
    }
  }
}
