import Foundation

/// Where a daemon lives — the sole axis on which a local vs. remote ``Backend``
/// differs (INV-PORT / D14).
///
/// This distinction is confined to the adapter module: feature code depends on
/// the ``Backend`` protocol and never names a location. Selecting an endpoint
/// picks which transport a ``DaemonTransportProvider`` builds; the resulting
/// ``Backend`` is otherwise identical.
public enum DaemonEndpoint: Sendable, Equatable {
  /// A daemon reachable on this host (e.g. a Unix-domain socket path).
  case localDaemon(socketPath: String)
  /// A daemon reachable over the network.
  case remoteDaemon(host: String, port: Int)
}

/// Builds the ``RpcTransport`` for a chosen ``DaemonEndpoint``.
///
/// This is the seam where "which transport / where the daemon runs" is resolved.
/// The live Unix-socket / TCP providers are convergence/cutover work; the client
/// is built against this protocol and exercised with an in-memory fake.
public protocol DaemonTransportProvider: Sendable {
  func makeTransport(for endpoint: DaemonEndpoint) async throws -> any RpcTransport
}

/// Resolves a ``DaemonEndpoint`` to a connected ``Backend`` by pairing the
/// provider's transport with the RPC adapter.
///
/// Local-daemon and remote-daemon are two endpoints through this one seam: both
/// yield an ``RpcBackend``, differing only by the provider's transport (INV-PORT).
public struct BackendConnector: Sendable {
  private let provider: any DaemonTransportProvider

  public init(provider: any DaemonTransportProvider) {
    self.provider = provider
  }

  /// Connects to `endpoint` and returns the port-typed backend.
  public func connect(to endpoint: DaemonEndpoint) async throws -> any Backend {
    let transport = try await provider.makeTransport(for: endpoint)
    return RpcBackend(transport: transport)
  }
}
