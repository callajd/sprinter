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
///
/// **Self-healing session backend (CE4.2 / CE3.1-F2 restart watch-item).** The board feed
/// already reconnects on its own (``WorkGraphResync`` owns a reconnect loop); before
/// CE4.2 the session-channel ``Backend`` was dialed exactly ONCE, so a daemon RESTART left
/// the shell stuck — either on `backend == nil` (a first connect that failed was never
/// retried) or on a DEAD backend (a drop after a successful connect was never noticed). The
/// connect lifecycle is now a supervised RECONNECT LOOP: it retries a failed connect with a
/// widening ``ReconnectBackoff`` (so a daemon that starts LATE is still reached), and — once
/// connected — watches the connection's liveness (the `events` feed ends/throws when the
/// daemon goes away) and RE-DIALS on a drop, publishing a fresh ``Backend`` so the
/// session-channel Views rebuild against the restarted daemon. So ``RootView`` recovers
/// across a restart instead of staying stuck.
@Observable
@MainActor
public final class AppModel {
  /// The daemon-connection state the shell reflects while the app dials the socket.
  /// A `.failed` carries a rendered description (kept `String` so the state stays
  /// `Sendable`/`Equatable` for the view and tests); the underlying error need not
  /// escape the connect step.
  public enum ConnectionState: Equatable, Sendable {
    /// The connect attempt is in flight (the initial state, before ``start()`` resolves,
    /// and again between a drop and the next successful re-dial).
    case connecting
    /// A ``Backend`` is connected; the session-channel Views can be built from it.
    case connected
    /// The most recent connect attempt failed — the rendered reason to surface. The loop
    /// keeps retrying underneath (with backoff), so this is not a terminal dead-end.
    case failed(reason: String)
  }

  /// The live Mission Control board, consuming a ``WorkGraphResync`` feed. Observed by
  /// the board View, which re-renders as snapshots and live deltas fold in.
  public let board: MissionControlBoard

  /// The daemon-connection state, observed by the shell chrome.
  public private(set) var connectionState: ConnectionState = .connecting

  /// The connected ``Backend`` once a connect resolves, else `nil`. The session-channel
  /// Views (inbox / session / planner / inspector transcript) are constructed from it and
  /// are rebuilt when a restart re-dials a fresh backend. Not `Equatable`; observation
  /// tracks the reference identity, so a re-dial swapping the instance re-renders the Views.
  public private(set) var backend: (any Backend)?

  @ObservationIgnored private let daemon: DaemonConnection
  @ObservationIgnored private var backoff: ReconnectBackoff
  @ObservationIgnored private var connectTask: Task<Void, Never>?

  public init(daemon: DaemonConnection, reconnectBackoff: ReconnectBackoff = ReconnectBackoff()) {
    self.daemon = daemon
    self.backoff = reconnectBackoff
    self.board = MissionControlBoard()
  }

  /// Starts the app: subscribes the board to a fresh live feed and launches the supervised
  /// backend reconnect loop for the session-channel Views. Idempotent — a second call does
  /// not launch a second loop while one is already running (the board's own `start` is
  /// idempotent too).
  public func start() {
    board.start(daemon.makeWorkGraphFeed())
    guard connectTask == nil else { return }
    connectTask = Task { [weak self] in
      await self?.runConnectionLoop()
    }
  }

  /// The supervised connect lifecycle: dial the daemon, publish the ``Backend``, watch it
  /// for a drop, and RE-DIAL — retrying a failed connect with a widening backoff. Runs until
  /// ``stop()`` cancels it, so the session channel self-heals across a daemon restart.
  private func runConnectionLoop() async {
    while !Task.isCancelled {
      do {
        let connected = try await daemon.connect()
        // A dial that completes AFTER `stop()` cancelled this loop still yields a LIVE
        // connection — the dial's continuation is not cancellation-aware — and `stop()` has
        // already inspected `backend`, so nothing else would ever close this one. Close it
        // here: an abandoned `UnixSocketTransport` keeps its read thread parked in `read(2)`
        // and its fd open for the life of the process. `close()` is idempotent, so racing
        // `stop()`'s own close is harmless.
        guard !Task.isCancelled else {
          await connected.close()
          break
        }
        backend = connected
        connectionState = .connected
        // Watch the connection's liveness: `events` ends/throws when the daemon goes away
        // (a restart). Returns whether it delivered a read — a healthy connection resets
        // the backoff (so a healthy drop re-dials promptly); an accept-then-flap does not.
        let wasHealthy = await awaitDrop(connected)
        // The connection dropped (or this loop was cancelled): retire the dead backend and
        // close it HERE, on EVERY exit, rather than leaving the cancelled path to `stop()`.
        backend = nil
        connectionState = .connecting
        await connected.close()
        if Task.isCancelled { break }
        if wasHealthy { backoff.reset() }
      } catch {
        // The mirror of the success case above: a dial that FAILS after `stop()` cancelled
        // this loop must not publish that failure. `stop()` has already reset the model to
        // `.connecting`, and there is no retry coming, so writing `.failed` here would leave
        // a STOPPED model reporting a connection error forever. Check before the write, not
        // after it.
        guard !Task.isCancelled else { break }
        // The dial itself failed (daemon not up yet): surface the reason and retry.
        connectionState = .failed(reason: String(describing: error))
      }
      if Task.isCancelled { break }
      try? await Task.sleep(for: backoff.next())
    }
  }

  /// Awaits the connection dropping by consuming its `events` feed until it ends or throws
  /// — the push-based liveness signal (a restarted daemon closes the socket, ending the
  /// stream). Returns whether the connection delivered at least one event (it was live and
  /// functioning), the health signal the loop resets its backoff on.
  private func awaitDrop(_ backend: any Backend) async -> Bool {
    var sawRead = false
    do {
      for try await _ in backend.events() {
        sawRead = true
      }
    } catch {
      // A transport drop surfaces here; the loop re-dials.
    }
    return sawRead
  }

  /// A fresh work-graph feed for an inspector PR pane (each single-consumer feed is its
  /// own engine), built on the same connection seam as the board.
  public func makeWorkGraphFeed() -> WorkGraphResync {
    daemon.makeWorkGraphFeed()
  }

  /// Tears the app down: stops the board feed, cancels the reconnect loop, and closes the
  /// connected backend — releasing the live ``UnixSocketTransport``'s real thread + fd
  /// deterministically. CE4.2 wires this to the scene lifecycle (`SprinterApp`'s `.task`
  /// cancellation), so a closed window releases the transport promptly rather than only at
  /// process exit. Idempotent.
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
