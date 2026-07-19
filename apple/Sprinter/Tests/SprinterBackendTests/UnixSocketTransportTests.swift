import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

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

  @Test("connect to a path with no listener throws connectionFailed")
  func connectWithNoListenerThrows() async throws {
    let path = "/tmp/sprinter-absent-\(UUID().uuidString.prefix(8)).sock"
    do {
      _ = try UnixSocketTransport.connect(toUnixSocketPath: path)
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
      _ = try UnixSocketTransport.connect(toUnixSocketPath: path)
      Issue.record("expected connect to throw")
    } catch let error as UnixSocketTransportError {
      guard case .socketPathTooLong = error else {
        Issue.record("expected socketPathTooLong, got \(error)")
        return
      }
    }
  }

  @Test("a remote endpoint has no transport yet and fails loudly")
  func remoteEndpointUnsupported() async throws {
    await #expect(throws: (any Error).self) {
      _ = try await DaemonTransports().makeTransport(
        for: .remoteDaemon(host: "daemon.internal", port: 8443))
    }
  }
}
