import SprinterBackend
import SprinterContract
import Testing

@testable import SprinterAppSupport
@testable import SprinterMissionControl

@Suite("Daemon connection wiring")
struct DaemonConnectionTests {
  /// The endpoint resolves to the `SPRINTER_SOCKET` path when the environment names it.
  @Test("resolve reads SPRINTER_SOCKET")
  func resolveReadsEnvironment() {
    let endpoint = DaemonEndpointResolver.resolve(environment: ["SPRINTER_SOCKET": "/run/s.sock"])
    #expect(endpoint == .localDaemon(socketPath: "/run/s.sock"))
  }

  /// With no `SPRINTER_SOCKET`, the endpoint falls back to the daemon's default path —
  /// so a locally-launched daemon + app rendezvous with no configuration.
  @Test("resolve defaults to the daemon's default socket path")
  func resolveDefaults() {
    let endpoint = DaemonEndpointResolver.resolve(environment: [:])
    #expect(endpoint == .localDaemon(socketPath: DaemonEndpointResolver.defaultSocketPath))
  }

  /// A set-but-empty `SPRINTER_SOCKET` is treated as "unset" — an exported-but-empty
  /// variable must not resolve to the literal path `""`, so it falls back to the default.
  @Test("resolve treats an empty SPRINTER_SOCKET as unset")
  func resolveEmptyDefaults() {
    let endpoint = DaemonEndpointResolver.resolve(environment: ["SPRINTER_SOCKET": ""])
    #expect(endpoint == .localDaemon(socketPath: DaemonEndpointResolver.defaultSocketPath))
  }

  /// A whitespace-only `SPRINTER_SOCKET` is likewise blank — it falls back to the default
  /// rather than dialing a path made of spaces.
  @Test("resolve treats a whitespace-only SPRINTER_SOCKET as unset")
  func resolveWhitespaceDefaults() {
    let endpoint = DaemonEndpointResolver.resolve(environment: ["SPRINTER_SOCKET": "  \t\n"])
    #expect(endpoint == .localDaemon(socketPath: DaemonEndpointResolver.defaultSocketPath))
  }

  /// A real (non-blank) `SPRINTER_SOCKET` resolves to exactly that path — the empty-guard
  /// leaves a genuine value untouched.
  @Test("resolve keeps a real SPRINTER_SOCKET value")
  func resolveRealValue() {
    let endpoint = DaemonEndpointResolver.resolve(environment: ["SPRINTER_SOCKET": "/run/s.sock"])
    #expect(endpoint == .localDaemon(socketPath: "/run/s.sock"))
  }

  /// The injected-seam initializer hands `connect()` straight through to the closure,
  /// and `makeWorkGraphFeed()` yields a real feed a board consumes end to end.
  @Test("connect() and makeWorkGraphFeed() drive off the injected seam")
  @MainActor
  func injectedSeamDrivesBoard() async throws {
    let fake = FakeBackend(snapshot: AppSupportFixtures.snapshot)
    let daemon = DaemonConnection(connect: { fake })

    let backend = try await daemon.connect()
    let snapshot = try await backend.snapshot()
    #expect(snapshot == AppSupportFixtures.snapshot)

    let board = MissionControlBoard()
    board.start(daemon.makeWorkGraphFeed())
    #expect(await waitUntil { !board.workstreams.isEmpty })
    #expect(board.workstreams.first?.repo == "callajd/sprinter")

    board.stop()
    await fake.close()
  }

  /// The LIVE initializer resolves the endpoint through a `BackendConnector` over the
  /// provider and returns an `RpcBackend` — asserted offline with a fake provider, so no
  /// socket is dialed in the gate.
  @Test("live initializer builds a backend over the transport provider")
  func liveInitializerBuildsBackend() async throws {
    let provider = FakeTransportProvider()
    let endpoint = DaemonEndpoint.localDaemon(socketPath: "/tmp/sprinter-test.sock")
    let daemon = DaemonConnection(endpoint: endpoint, provider: provider)

    let backend = try await daemon.connect()
    #expect(provider.requestedEndpoint == endpoint)
    await backend.close()
  }
}
