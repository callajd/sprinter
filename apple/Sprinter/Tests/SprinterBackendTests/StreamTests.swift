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

  /// Regression for the v3 end-to-end decode bug (CE2.0 re-review): the daemon's
  /// `events` payload schema is a `Struct` under v3, so an OMITTED `payload` key
  /// decodes to `undefined` and the stream errors on connect. The canonical Effect
  /// client sends `{}` for `.events({})`; the Swift client must match by sending a
  /// PRESENT empty ``EventsPayload`` (INV-CONTRACT). Asserted on the REAL outbound
  /// wire bytes (Backend → envelope encode → transport), not the RpcTest shortcut.
  @Test("events() encodes a present empty payload object, not an omitted key")
  func eventsRequestSendsPresentEmptyPayload() async throws {
    let transport = FakeTransport()
    let backend = RpcBackend(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let collector = Task {
      for try await _ in backend.events() {}
    }

    let request = try await nextSent(&outbound)
    #expect(request.rpcTag == "events")
    // A present empty object (`"payload": {}`) — no `sinceOffset` → origin replay —
    // NOT an absent key (which would decode to `nil` here and to `undefined` on the
    // wire, breaking the v3 `Struct` decode).
    #expect(request.payload == .object([:]))
    #expect(request.payload != nil)

    let id = try #require(request.id)
    transport.emit(Wire.exitSuccessVoid(requestId: id))
    collector.cancel()
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

  /// Demand-gated backpressure (CE2.2): the per-batch `Ack` is DEFERRED until the
  /// consumer drains that batch — it is the "ready for more" signal, not an on-receipt
  /// handshake. Two batches, drained in turn, yield exactly two acks, each emitted only
  /// once its batch has been fully consumed and the consumer has asked for more.
  @Test("each batch's ack is deferred until the consumer drains it")
  func ackDeferredUntilDrain() async throws {
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport)
    var outbound = transport.outbound.makeAsyncIterator()

    let stream = await connection.stream(tag: "events", payload: nil)
    let request = try await nextSent(&outbound)
    #expect(request.envelopeTag == "Request")
    let id = try #require(request.id)

    // Two separate batches (chunks).
    transport.emit(Wire.chunk(requestId: id, values: [#""a""#]))
    transport.emit(Wire.chunk(requestId: id, values: [#""b""#]))

    let collector = Task { () -> [JSONValue] in
      var values: [JSONValue] = []
      for try await value in stream {
        values.append(value)
      }
      return values
    }

    // Draining batch 1 (and requesting more) sends ack 1; draining batch 2 sends ack 2.
    let ack1 = try await nextSent(&outbound)
    #expect(ack1.envelopeTag == "Ack")
    #expect(ack1.requestId == id)
    let ack2 = try await nextSent(&outbound)
    #expect(ack2.envelopeTag == "Ack")
    #expect(ack2.requestId == id)

    transport.emit(Wire.exitSuccessVoid(requestId: id))
    #expect(try await collector.value == [.string("a"), .string("b")])
    transport.close()
  }

  /// The bounded backlog (CE2.2 / the CE2.1-F4 carried constraint): an un-drained
  /// subscription does NOT ack-and-buffer unbounded — the demand-gated buffer is bounded,
  /// and a batch past the bound surfaces an ``AckGate/Overflow`` the consumer sees as a
  /// failure (→ resync upstream), never a silent drop. A single over-limit chunk trips it
  /// deterministically.
  @Test("an over-limit batch overflows the bounded buffer instead of buffering unbounded")
  func boundedBufferOverflows() async throws {
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport, streamBufferLimit: 2)
    var outbound = transport.outbound.makeAsyncIterator()

    let stream = await connection.stream(tag: "events", payload: nil)
    let id = try #require(try await nextSent(&outbound).id)

    // One chunk of three values exceeds the limit of two → overflow at push time.
    transport.emit(Wire.chunk(requestId: id, values: [#""a""#, #""b""#, #""c""#]))

    await #expect(throws: AckGate.Overflow.self) {
      for try await _ in stream {}
    }
    transport.close()
  }

  /// FIX4 — overflow is self-cleaning. An ``AckGate/Overflow`` is a LOCAL failure: the
  /// request stays `pending` in ``RpcConnection`` and the daemon keeps streaming. A
  /// consumer that abandons the stream on overflow (no terminal `Exit`, no explicit
  /// `close()`) must NOT leak that pending request — abandoning after overflow sends an
  /// `Interrupt`, cancelling the request and telling the daemon to stop. Proven on the real
  /// outbound wire: after the overflow throws and the iterator is dropped, an `Interrupt`
  /// frame for the request appears.
  @Test("abandoning the stream after an overflow interrupts the request (no pending leak)")
  func overflowInterruptsPendingRequest() async throws {
    let transport = FakeTransport()
    let connection = RpcConnection(transport: transport, streamBufferLimit: 2)
    var outbound = transport.outbound.makeAsyncIterator()

    let stream = await connection.stream(tag: "events", payload: nil)
    let request = try await nextSent(&outbound)
    let id = try #require(request.id)

    // One over-limit chunk trips the overflow.
    transport.emit(Wire.chunk(requestId: id, values: [#""a""#, #""b""#, #""c""#]))

    // Consume-then-abandon: the for-loop's iterator is released when this scope exits,
    // driving the drop path (deinit → cancelFromConsumer) — the leak scenario FIX4 closes.
    do {
      for try await _ in stream {}
      Issue.record("expected the over-limit batch to overflow")
    } catch is AckGate.Overflow {
      // Expected: the bounded gate surfaced the overflow.
    }

    // Self-cleaning: an Interrupt for the request is sent (pending cleared, daemon stopped).
    let interrupt = try await nextSent(&outbound)
    #expect(interrupt.envelopeTag == "Interrupt")
    #expect(interrupt.requestId == id)
    transport.close()
  }
}
