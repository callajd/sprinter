import Foundation
import SprinterContract
import Testing

@testable import SprinterBackend

/// Records the endpoints a connector selected, proving local vs. remote is only
/// an adapter-selection axis (INV-PORT).
actor EndpointRecorder {
  private(set) var endpoints: [DaemonEndpoint] = []
  func record(_ endpoint: DaemonEndpoint) {
    endpoints.append(endpoint)
  }
}

/// A fake provider that hands back an in-memory transport for any endpoint.
struct RecordingProvider: DaemonTransportProvider {
  let recorder: EndpointRecorder
  let transport: FakeTransport

  func makeTransport(for endpoint: DaemonEndpoint) async throws -> any RpcTransport {
    await recorder.record(endpoint)
    return transport
  }
}

@Suite("Backend port resolution")
struct BackendPortTests {
  private func resolveSnapshot(through endpoint: DaemonEndpoint) async throws -> (
    snapshot: Snapshot, recorded: [DaemonEndpoint]
  ) {
    let transport = FakeTransport()
    let recorder = EndpointRecorder()
    let connector = BackendConnector(
      provider: RecordingProvider(recorder: recorder, transport: transport))

    let backend: any Backend = try await connector.connect(to: endpoint)
    var outbound = transport.outbound.makeAsyncIterator()
    let task = Task { try await backend.snapshot() }
    let id = try #require(try await nextSent(&outbound).id)
    transport.emit(Wire.exitSuccess(requestId: id, value: try Wire.encoded(Fixtures.snapshot)))
    let snapshot = try await task.value
    transport.close()
    return (snapshot, await recorder.endpoints)
  }

  @Test("a local-daemon endpoint resolves the port through a fake adapter")
  func localDaemonResolves() async throws {
    let result = try await resolveSnapshot(through: .localDaemon(socketPath: "/tmp/sprinter.sock"))
    #expect(result.snapshot == Fixtures.snapshot)
    #expect(result.recorded == [.localDaemon(socketPath: "/tmp/sprinter.sock")])
  }

  @Test("a remote-daemon endpoint resolves the same port surface")
  func remoteDaemonResolves() async throws {
    let result = try await resolveSnapshot(
      through: .remoteDaemon(host: "daemon.internal", port: 8443))
    #expect(result.snapshot == Fixtures.snapshot)
    #expect(result.recorded == [.remoteDaemon(host: "daemon.internal", port: 8443)])
  }
}
