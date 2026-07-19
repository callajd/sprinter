import Foundation

/// The live ``DaemonTransportProvider``: resolves a ``DaemonEndpoint`` to a concrete
/// ``RpcTransport`` that dials the running daemon (CE2.1).
///
/// This is the seam where "which transport / where the daemon runs" becomes a real
/// connection — the only place localness is named. A ``BackendConnector`` built over
/// it yields an ``RpcBackend`` whose feature-facing surface is identical regardless
/// of endpoint (INV-PORT); selecting `.localDaemon` dials CE1.2's Unix-domain socket
/// via ``UnixSocketTransport``.
///
/// `.remoteDaemon` has no counterpart yet: CE1/CE2's daemon serves ONLY a local
/// Unix-domain socket (per `docs/architecture.md`, a remote daemon is reached over an
/// authenticated HTTP/WS control plane, never a raw remote socket — later cutover
/// work). Rather than silently mis-dial, this raises a typed
/// ``UnixSocketTransportError`` so the unsupported selection surfaces loudly; the
/// endpoint *axis* is delivered, and the remote adapter slots in here unchanged.
public struct DaemonTransports: DaemonTransportProvider {
  public init() {}

  public func makeTransport(for endpoint: DaemonEndpoint) async throws -> any RpcTransport {
    switch endpoint {
    case .localDaemon(let socketPath):
      return try UnixSocketTransport.connect(toUnixSocketPath: socketPath)
    case .remoteDaemon:
      // No remote transport exists yet (CE1/CE2 serve a local Unix socket only). Fail
      // loudly instead of dialing nothing — the remote adapter lands behind this seam.
      throw UnixSocketTransportError.connectionFailed(errno: ENOTSUP)
    }
  }
}
