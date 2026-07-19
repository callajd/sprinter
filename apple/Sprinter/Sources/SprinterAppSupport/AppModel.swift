import Observation
import SprinterBackend
import SprinterContract
import SprinterMissionControl

/// The app's top-level composition root (CE3.1): the `@Observable @MainActor` model the
/// SwiftUI shell renders. It owns the live Mission Control board (fed by a
/// ``WorkGraphResync`` over the ``DaemonConnection`` seam) and the connection lifecycle
/// that yields the ``Backend`` the session-channel Views (inbox / session / planner /
/// inspector) drive.
///
/// It is platform-neutral (Foundation + Observation, no AppKit/UIKit, no `#if os(...)`):
/// all rendering logic stays in the feature view models (already tested); this composes
/// them and manages the connect lifecycle, so the testable wiring is verified by
/// `make check` (INV-COV) while the SwiftUI Views stay thin at the platform edge.
@Observable
@MainActor
public final class AppModel {
  /// The daemon-connection state the shell reflects while the app dials the socket.
  /// A `.failed` carries a rendered description (kept `String` so the state stays
  /// `Sendable`/`Equatable` for the view and tests); the underlying error need not
  /// escape the connect step.
  public enum ConnectionState: Equatable, Sendable {
    /// The connect attempt is in flight (the initial state, before ``start()`` resolves).
    case connecting
    /// A ``Backend`` is connected; the session-channel Views can be built from it.
    case connected
    /// The connect attempt failed — the rendered reason to surface.
    case failed(reason: String)
  }

  /// The live Mission Control board, consuming a ``WorkGraphResync`` feed. Observed by
  /// the board View, which re-renders as snapshots and live deltas fold in.
  public let board: MissionControlBoard

  /// The daemon-connection state, observed by the shell chrome.
  public private(set) var connectionState: ConnectionState = .connecting

  /// The connected ``Backend`` once ``start()`` resolves, else `nil`. The
  /// session-channel Views (inbox / session / planner / inspector transcript) are
  /// constructed from it. Not `Equatable`; observation tracks the reference identity.
  public private(set) var backend: (any Backend)?

  @ObservationIgnored private let daemon: DaemonConnection
  @ObservationIgnored private var connectTask: Task<Void, Never>?

  public init(daemon: DaemonConnection) {
    self.daemon = daemon
    self.board = MissionControlBoard()
  }

  /// Starts the app: subscribes the board to a fresh live feed and dials a ``Backend``
  /// for the session-channel Views. Idempotent — a second call does not re-dial while a
  /// connect attempt is already outstanding (the board's own `start` is idempotent too).
  public func start() {
    board.start(daemon.makeWorkGraphFeed())
    guard connectTask == nil else { return }
    connectTask = Task { [weak self] in
      await self?.connect()
    }
  }

  private func connect() async {
    do {
      let connected = try await daemon.connect()
      backend = connected
      connectionState = .connected
    } catch {
      connectionState = .failed(reason: String(describing: error))
    }
  }

  /// A fresh work-graph feed for an inspector PR pane (each single-consumer feed is its
  /// own engine), built on the same connection seam as the board.
  public func makeWorkGraphFeed() -> WorkGraphResync {
    daemon.makeWorkGraphFeed()
  }

  /// Tears the app down: stops the board feed, cancels an in-flight connect, and closes
  /// the connected backend. The live ``UnixSocketTransport`` holds a real thread + fd that
  /// `close()` releases, but today this explicit teardown is exercised only by tests: the
  /// `WindowGroup` scene wires `start()` with no matching lifecycle hook, so the running
  /// single-process app reclaims the thread + fd at process exit. Wiring `stop()` to the
  /// scene lifecycle is deferred to CE4. Idempotent.
  public func stop() {
    connectTask?.cancel()
    connectTask = nil
    board.stop()
    if let backend {
      Task { await backend.close() }
    }
    backend = nil
    connectionState = .connecting
  }
}
