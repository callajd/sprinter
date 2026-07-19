import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Exercises the concrete ``UnixSocketTransport`` over REAL kernel sockets (CE2.1):
/// real NDJSON envelope frames round-trip in both directions, the endpoint-selection
/// path (`DaemonTransports` → `BackendConnector`) dials a live socket, and the
/// teardown/error seams behave — all deterministic and offline (no live daemon).
@Suite("Unix-domain socket transport")
struct UnixSocketTransportTests {
  /// Decodes a client-sent frame line into its correlated fields.
  private func decodeSent(_ line: Data) throws -> SentFrame {
    try JSONDecoder().decode(SentFrame.self, from: line)
  }

  @Test("a snapshot query round-trips real NDJSON frames over a real socket")
  func snapshotRoundTripOverSocket() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    let backend = RpcBackend(transport: transport)

    let task = Task { try await backend.snapshot() }

    let request = try decodeSent(try await peer.nextLine())
    #expect(request.envelopeTag == "Request")
    #expect(request.rpcTag == "snapshot")
    let id = try #require(request.id)

    let exit = Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot))
    try await peer.write(exit)
    #expect(try await task.value == Fixtures.snapshot)

    await backend.close()
    peer.close()
  }

  @Test("the v3 OffsetEvent events stream decodes real frames and acks each batch")
  func eventsStreamOverSocket() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    let backend = RpcBackend(transport: transport)

    let collector = Task { () -> [WorkGraphEvent] in
      var events: [WorkGraphEvent] = []
      for try await event in backend.events() {
        events.append(event)
      }
      return events
    }

    let request = try decodeSent(try await peer.nextLine())
    #expect(request.rpcTag == "events")
    // INV-CONTRACT: a present empty payload object (`sinceOffset`-capable, origin replay).
    #expect(request.payload == .object([:]))
    let id = try #require(request.id)

    try await peer.write(
      Wire.chunk(
        requestId: id,
        values: [
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.issueEvent, at: 3)),
          try Wire.encoded(Fixtures.offsetEvent(Fixtures.workstreamEvent, at: 4))
        ]))

    // The client acks the batch on receipt (real Ack frame back over the socket).
    let ack = try decodeSent(try await peer.nextLine())
    #expect(ack.envelopeTag == "Ack")
    #expect(ack.requestId == id)

    try await peer.write(Wire.exitSuccessVoid(requestId: id))
    #expect(try await collector.value == [Fixtures.issueEvent, Fixtures.workstreamEvent])

    await backend.close()
    peer.close()
  }

  @Test("an Exit frame split across two socket writes is reassembled")
  func partialFrameReassembledOverSocket() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    let backend = RpcBackend(transport: transport)

    let task = Task { try await backend.snapshot() }
    let id = try #require(try decodeSent(try await peer.nextLine()).id)

    // Split the daemon's Exit frame across two writes: the NdjsonReassembler must
    // buffer the partial line until the delimiter arrives in the second chunk.
    let frame = Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot))
    let midpoint = frame.index(frame.startIndex, offsetBy: frame.count / 2)
    try await peer.write(frame[frame.startIndex..<midpoint])
    try await peer.write(frame[midpoint..<frame.endIndex])

    #expect(try await task.value == Fixtures.snapshot)
    await backend.close()
    peer.close()
  }

  @Test("a local endpoint dials a live socket via DaemonTransports + BackendConnector")
  func endpointSelectionDialsLiveSocket() async throws {
    let server = try LoopbackSocketServer()
    defer { server.stop() }

    let connector = BackendConnector(provider: DaemonTransports())
    let backend = try await connector.connect(to: .localDaemon(socketPath: server.path))
    let peer = server.acceptPeer()

    let task = Task { try await backend.snapshot() }
    let id = try #require(try decodeSent(try await peer.nextLine()).id)
    let exit = Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot))
    try await peer.write(exit)

    #expect(try await task.value == Fixtures.snapshot)
    await backend.close()
    peer.close()
  }

  @Test("the daemon closing the socket fails an in-flight query with connectionClosed")
  func peerCloseFailsInflightQuery() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    let backend = RpcBackend(transport: transport)

    let task = Task { try await backend.snapshot() }
    _ = try await peer.nextLine()  // ensure the Request is registered as pending
    peer.close()  // daemon drops the connection: the read loop hits EOF

    await #expect(throws: BackendError.connectionClosed) { try await task.value }
    await backend.close()
  }

  @Test("send after close() is rejected with connectionClosed")
  func sendAfterCloseRejected() async throws {
    let (transport, peer) = try RawSocketPeer.pair()
    transport.close()
    transport.close()  // idempotent — a second close is a no-op
    await #expect(throws: BackendError.connectionClosed) {
      try await transport.send(Data("{}\n".utf8))
    }
    peer.close()
  }

  @Test("close() racing a queued write never lands a write on a freed fd, and fails cleanly")
  func closeRacingQueuedWrite() async throws {
    // A write that loses the race to `shutdown` may see EPIPE; it must not crash the
    // test. What FIX1 guarantees is that a queued write never reaches a CLOSED fd
    // (which would surface as EBADF) — the real `close(2)` is serialized behind writes
    // on `writeQueue` and `send` re-checks `isClosed` there. NOTE: there is deliberately
    // NO `signal(SIGPIPE, SIG_IGN)` guard here — FIX B1 sets `SO_NOSIGPIPE` on the
    // transport descriptor, so a broken-pipe write returns EPIPE without a signal. If
    // production ever regressed to needing the process-wide guard, this test would crash.
    for _ in 0..<32 {
      let (transport, peer) = try RawSocketPeer.pair()
      let frame = Data("{\"k\":\"v\"}\n".utf8)

      // Enqueue a write, then close concurrently, twice (idempotent). Whichever wins
      // the race, the write either lands on the still-valid fd or is skipped — it can
      // never write to the closed/reused fd.
      async let outcome: Void = transport.send(frame)
      transport.close()
      transport.close()  // idempotent — a second close is a no-op.

      do {
        try await outcome
      } catch is CancellationError {
        // not expected, but harmless
      } catch let error as UnixSocketTransportError {
        guard case .writeFailed(let errno) = error else { throw error }
        #expect(errno != EBADF, "a queued write reached a closed fd (EBADF)")
      } catch BackendError.connectionClosed {
        // Skipped because close won the race — the clean, expected outcome.
      }

      // A send issued strictly after close() always fails cleanly with
      // connectionClosed, never writing to (or crashing on) the closed fd.
      await #expect(throws: BackendError.connectionClosed) { try await transport.send(frame) }
      peer.close()
    }
  }

  @Test("connect to a path with no listener throws connectionFailed")
  func connectWithNoListenerThrows() async throws {
    let path = "/tmp/sprinter-absent-\(UUID().uuidString.prefix(8)).sock"
    do {
      _ = try await UnixSocketTransport.connect(toUnixSocketPath: path)
      Issue.record("expected connect to throw")
    } catch let error as UnixSocketTransportError {
      guard case .connectionFailed = error else {
        Issue.record("expected connectionFailed, got \(error)")
        return
      }
    }
  }

  @Test("connect to an over-long socket path throws socketPathTooLong")
  func connectWithOverLongPathThrows() async throws {
    let path = String(repeating: "a", count: unixSocketPathCapacity + 16)
    do {
      _ = try await UnixSocketTransport.connect(toUnixSocketPath: path)
      Issue.record("expected connect to throw")
    } catch let error as UnixSocketTransportError {
      guard case .socketPathTooLong = error else {
        Issue.record("expected socketPathTooLong, got \(error)")
        return
      }
    }
  }

  @Test("a remote endpoint has no transport yet and fails with remoteEndpointUnsupported")
  func remoteEndpointUnsupported() async throws {
    await #expect(throws: UnixSocketTransportError.remoteEndpointUnsupported) {
      _ = try await DaemonTransports().makeTransport(
        for: .remoteDaemon(host: "daemon.internal", port: 8443))
    }
  }

  @Test("a transport write failure surfaces to the caller as BackendError.connectionClosed")
  func writeFailureSurfacesAsConnectionClosed() async throws {
    // NB1: a mid-write transport failure (UnixSocketTransportError.writeFailed) must reach
    // feature code as a BackendError, like every other terminal Backend failure — not as a
    // transport-specific error. The stub's inbound stream never ends, so the ONLY failure
    // an in-flight query can hit is the write path (not an inbound EOF).
    let backend = RpcBackend(transport: WriteFailingTransport())
    await #expect(throws: BackendError.connectionClosed) { try await backend.snapshot() }
    await backend.close()
  }
}

/// An ``RpcTransport`` whose `send` always fails with a transport-level write error and
/// whose inbound stream never terminates — so a request can only fail on the WRITE path,
/// isolating the transmit-boundary error mapping (NB1).
private final class WriteFailingTransport: RpcTransport {
  private let inbound: AsyncThrowingStream<Data, any Error>
  // Retained so the never-finishing inbound stream stays open for the test's lifetime.
  private let continuation: AsyncThrowingStream<Data, any Error>.Continuation

  init() {
    (inbound, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
  }

  func send(_ bytes: Data) async throws {
    throw UnixSocketTransportError.writeFailed(errno: EPIPE)
  }

  func receive() -> AsyncThrowingStream<Data, any Error> {
    inbound
  }

  func close() {
    continuation.finish()
  }
}
