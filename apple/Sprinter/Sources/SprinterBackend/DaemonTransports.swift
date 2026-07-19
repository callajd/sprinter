import Foundation

/// The live ``DaemonTransportProvider``: resolves a ``DaemonEndpoint`` to a concrete
/// ``RpcTransport`` that dials the running daemon (CE2.1).
///
/// This is the seam where "which transport / where the daemon runs" becomes a real
/// connection — the only place localness is named. A ``BackendConnector`` built over
/// it yields an ``RpcBackend`` whose feature-facing surface is identical regardless
/// of endpoint (INV-PORT); selecting `.localDaemon` dials CE1.2's Unix-domain socket
/// via ``UnixSocketTransport`` — the blocking dial is hopped off the cooperative
/// executor by that type's `async` ``UnixSocketTransport/connect(toUnixSocketPath:)``.
///
/// The returned ``UnixSocketTransport`` holds a real OS thread and file descriptor, so
/// `close()` is LOAD-BEARING: the owning ``BackendConnector``/``RpcConnection`` teardown
/// must call it or both leak (the parked read thread retains the transport, so no
/// `deinit` fires on its own).
///
/// `.remoteDaemon` has no counterpart yet: CE1/CE2's daemon serves ONLY a local
/// Unix-domain socket (per `docs/architecture.md`, a remote daemon is reached over an
/// authenticated HTTP/WS control plane, never a raw remote socket — later cutover
/// work). Rather than silently mis-dial, this raises
/// ``UnixSocketTransportError/remoteEndpointUnsupported`` (a dedicated "no adapter"
/// case, not a dial failure) so the unsupported selection surfaces loudly; the endpoint
/// *axis* is delivered, and the remote adapter slots in here unchanged.
public struct DaemonTransports: DaemonTransportProvider {
  public init() {}

  public func makeTransport(for endpoint: DaemonEndpoint) async throws -> any RpcTransport {
    switch endpoint {
    case .localDaemon(let socketPath):
      return try await UnixSocketTransport.connect(toUnixSocketPath: socketPath)
    case .remoteDaemon:
      // No remote transport exists yet (CE1/CE2 serve a local Unix socket only). Fail
      // loudly with a dedicated case — this is "no adapter exists", NOT a dial that
      // failed to connect — so callers can tell the two apart. The remote adapter lands
      // behind this seam.
      throw UnixSocketTransportError.remoteEndpointUnsupported
    }
  }
}
