import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

@Suite("RPC streaming subscriptions and keepalives")
struct StreamTests {
  @Test("events streams chunk values until Exit, acking each batch")
  func eventsStream() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task { () -> [WorkGraphEvent] in
      var events: [WorkGraphEvent] = []
      for try await event in backend.events() {
        events.append(event)
      }
      return events
    }

    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "events")
    let id = try #require(request.id)

    transport.emit(
      Wire.chunk(
        requestId: id,
        values: [
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.issueEvent, at: 3)),
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 4))
        ]))

    let ack = try await nextSent(&outbound)
    #expect(ack.envelopeTag == "Ack")
    #expect(ack.requestId == id)

    transport.emit(Wire.exitSuccessVoid(requestId: id))
    #expect(try await collector.value == [Fixtures.issueEvent, Fixtures.workstreamEvent])
    transport.close()
  }

  @Test("an unknown event _tag inside the envelope is a decode failure, never a silent drop")
  func unknownEventTagFails() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task {
      for try await _ in backend.events() {}
    }
    let id = try #require(try await nextSent(&outbound).id)
    // A well-formed OffsetEvent envelope whose INNER event carries an unknown `_tag`.
    transport.emit(
      Wire.chunk(requestId: id, values: [#"{"offset":5,"event":{"_tag":"MysteryChanged"}}"#]))

    await #expect(throws: (any Error).self) { try await collector.value }
    transport.close()
  }

  @Test("a stream failure Exit surfaces the mirrored ContractError")
  func streamFailureExit() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task {
      for try await _ in backend.events() {}
    }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(
      Wire.exitFail(requestId: id, error: #"{"_tag":"WorkstreamNotFound","id":"ws-1"}"#))

    await #expect(throws: ContractError.workstreamNotFound(id: WorkstreamId(rawValue: "ws-1"))) {
      try await collector.value
    }
    transport.close()
  }

  @Test("early stream termination interrupts the request")
  func earlyTerminationInterrupts() async throws {
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let stream = await connection.stream(tag: "events", payload: nil)
    let consume = Task {
      for try await _ in stream {}
    }
    let request = try await nextSent(&outbound)
    #expect(request.envelopeTag == "Request")

    consume.cancel()
    let interrupt = try await nextSent(&outbound)
    #expect(interrupt.envelopeTag == "Interrupt")
    #expect(interrupt.requestId == request.id)
    transport.close()
  }

  @Test("a Pong keepalive is ignored and does not resolve a query")
  func pongIsIgnored() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let task = Task { try await backend.snapshot() }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.pong())
    transport.emit(Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot)))
    #expect(try await task.value == Fixtures.snapshot)
    transport.close()
  }

  @Test("the client sends Ping and Eof keepalive frames")
  func pingAndEof() async throws {
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    await connection.ping()
    #expect(try await nextSent(&outbound).envelopeTag == "Ping")
    await connection.eof()
    #expect(try await nextSent(&outbound).envelopeTag == "Eof")
    transport.close()
  }
}
