import SprinterBackend
import SprinterContract

/// Resolves the ``DaemonEndpoint`` the app dials from the process environment
/// (CE3.1). The running app talks to a **local** daemon over a Unix-domain socket,
/// so this reads the same `SPRINTER_SOCKET` variable the daemon serves on (see
/// `packages/daemon/src/main.ts`), defaulting to the daemon's own default path.
///
/// It is a pure function of the environment — no I/O — so the endpoint selection is
/// directly testable (INV-COV) and the executable target only has to hand it
/// `ProcessInfo.processInfo.environment`.
public enum DaemonEndpointResolver {
  /// The environment variable naming the daemon's Unix-socket path — the same key
  /// the daemon reads when it binds the socket, so the app and daemon agree without
  /// a second source of truth.
  public static let socketEnvVar = "SPRINTER_SOCKET"

  /// The default socket path when the environment names none — matching the daemon's
  /// own default (`./sprinter.sock`), so a locally-launched daemon + app rendezvous
  /// with no configuration.
  public static let defaultSocketPath = "./sprinter.sock"

  /// Resolves the endpoint from `environment`: the `SPRINTER_SOCKET` path when set,
  /// else the default. Always a `.localDaemon` (CE1/CE2 serve a local socket only;
  /// a remote endpoint is later cutover work — see ``DaemonTransports``).
  public static func resolve(environment: [String: String]) -> DaemonEndpoint {
    let socketPath = environment[socketEnvVar] ?? defaultSocketPath
    return .localDaemon(socketPath: socketPath)
  }
}

/// The app's live-backend connection seam (CE3.1): it pairs a ``DaemonEndpoint`` with
/// the concrete transport stack — `BackendConnector` → ``DaemonTransports`` →
/// ``UnixSocketTransport`` → ``RpcBackend`` — behind a single `connect` closure, so
/// the app never names a transport (INV-PORT). Feature view models are handed either a
/// connected ``Backend`` (session channel) or a fresh ``WorkGraphResync`` feed built on
/// the same seam (the board / inspector PR pane).
///
/// The seam is injectable: the live initializer wires the real socket stack, while a
/// test constructs it from an in-memory `connect` closure, so the gate exercises the
/// wiring with no daemon, network, or socket.
public struct DaemonConnection: Sendable {
  /// Yields a freshly connected ``Backend`` for one connect (or reconnect) attempt —
  /// the same shape ``WorkGraphResync`` consumes for its reconnect loop.
  public typealias Connect = @Sendable () async throws -> any Backend

  private let connectSeam: Connect

  /// The injectable path: build directly over a `connect` closure (the test seam, and
  /// the composition primitive the live initializer delegates to).
  public init(connect: @escaping Connect) {
    self.connectSeam = connect
  }

  /// The live path: resolve `endpoint` through a `BackendConnector` over `provider`
  /// (the real ``DaemonTransports`` by default), so `connect` dials the daemon's Unix
  /// socket and returns an ``RpcBackend``. Local vs. remote stays the provider's
  /// choice — the app names only the endpoint.
  public init(
    endpoint: DaemonEndpoint,
    provider: any DaemonTransportProvider = DaemonTransports()
  ) {
    let connector = BackendConnector(provider: provider)
    self.connectSeam = { try await connector.connect(to: endpoint) }
  }

  /// Connects a ``Backend`` for the session-channel view models (inbox / session /
  /// planner / inspector transcript), which drive input and subscribe session feeds
  /// over a persistent connection.
  public func connect() async throws -> any Backend {
    try await connectSeam()
  }

  /// Builds a FRESH work-graph feed for a board or an inspector PR pane. Each consumer
  /// needs its own feed — ``WorkGraphResync`` is single-consumer — so this mints a new
  /// engine per call over the shared connect seam (it owns its own reconnect loop).
  ///
  /// **First-connect catch-up (CE2 carried constraint — conscious choice).** The
  /// engine's first connect is subscribe-around-snapshot with an ORIGIN `events` replay
  /// (`sinceOffset: nil`), because the mirrored ``Snapshot`` DTO carries **no** resume
  /// offset — there is no field to subscribe live-tail-only from. Per the constraint we
  /// therefore keep the current behavior; the snapshot + full-origin-replay redundancy
  /// is fine at cutover scale and its efficiency is tracked separately (issue #56). If a
  /// future contract adds a snapshot resume offset, subscribing from it is a one-line
  /// change here + in ``WorkGraphResync``.
  public func makeWorkGraphFeed() -> WorkGraphResync {
    WorkGraphResync(connect: connectSeam)
  }
}
