import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("Interactive session — UI round-trip")
@MainActor
struct InteractiveSessionTests {
  /// The `extension_ui_request` round-trip: a `UiRequestRaised` surfacing on the
  /// feed becomes an outstanding request (correlated by id); answering it sends
  /// the neutral `UiResponse` the fake transport observes, and clears it.
  @Test("a raised UI request is answered and cleared, keyed by request id")
  func uiRoundTrip() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    let session = InteractiveSession(backend: backend, sessionId: Fixtures.sessionId)
    var outbound = transport.outbound.makeAsyncIterator()

    session.start()
    #expect(session.isRunning)

    let subscribe = try await nextRequest(&outbound)
    #expect(subscribe.rpcTag == "sessionEvents")
    let feedId = try #require(subscribe.id)

    // The daemon raises a confirm prompt on the feed.
    transport.emit(
      Wire.chunk(requestId: feedId, values: [try Wire.encoded(Fixtures.uiRequestEvent)]))
    try await settle { session.outstandingRequests.count == 1 }
    let outstanding = try #require(session.outstandingRequests.first)
    #expect(outstanding.id == "req-1")
    #expect(outstanding.kind == .confirm)
    #expect(outstanding.prompt == "Merge the PR?")

    // Answer it; the fake observes the correlated UiResponse.
    let answering = Task {
      try await session.answer(requestId: "req-1", .confirmed(confirmed: true))
    }
    let answer = try await nextRequest(&outbound)
    #expect(answer.rpcTag == "answerUiRequest")
    let expected = try toJSONValue(
      AnswerUiRequestPayload(
        sessionId: Fixtures.sessionId,
        response: UiResponse(requestId: "req-1", answer: .confirmed(confirmed: true))))
    #expect(try #require(answer.payload) == expected)
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(answer.id)))
    try await answering.value

    #expect(session.outstandingRequests.isEmpty)
    #expect(session.events == [Fixtures.uiRequestEvent])

    session.stop()
    #expect(!session.isRunning)
    transport.close()
  }

  @Test("send and interrupt drive input out through the session channel")
  func sendAndInterrupt() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    let session = InteractiveSession(backend: backend, sessionId: Fixtures.sessionId)
    var outbound = transport.outbound.makeAsyncIterator()

    let sending = Task { try await session.send(Fixtures.sessionInput) }
    let sendRequest = try await nextRequest(&outbound)
    #expect(sendRequest.rpcTag == "sessionSend")
    transport.emit(Wire.exitSuccessVoid(requestId: try #require(sendRequest.id)))
    try await sending.value

    let interrupting = Task { try await session.interrupt() }
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
