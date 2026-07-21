import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// ``UnixSocketTransport``'s DIAL — how a connected transport is obtained (CE1.2's
/// `SPRINTER_SOCKET`).
///
/// Split from the transport's own file because it is a different concern with a different
/// hazard: everything here is about getting a connected, `SO_NOSIGPIPE`-armed descriptor off
/// the cooperative executor, whereas the transport file is about that descriptor's LIFETIME
/// once owned. Neither needs the other's internals.
extension UnixSocketTransport {
  /// Off-executor home for the blocking dial (`socket(2)`/`connect(2)`): a full listen backlog
  /// can park `connect(2)`, so it must never run on a cooperative executor thread. Concurrent
  /// so independent dials don't serialize behind each other.
  private static let dialQueue = DispatchQueue(
    label: "sprinter.UnixSocketTransport.dial", attributes: .concurrent)

  /// Dials the daemon listening on `path` (CE1.2's `SPRINTER_SOCKET`) and returns a
  /// connected transport. Throws a typed ``UnixSocketTransportError`` on a failed
  /// `socket(2)`/`connect(2)`, a failed `SO_NOSIGPIPE`, or an over-long path — never a
  /// force-unwrap.
  ///
  /// `internal` (NB3): the blocking dial must never run on a cooperative executor, so
  /// the public dial is the `async` overload below, which hops this onto ``dialQueue``.
  /// This synchronous form only backs that overload (and the tests reach it through it).
  static func connect(toUnixSocketPath path: String) throws -> UnixSocketTransport {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw UnixSocketTransportError.socketCreationFailed(errno: errno)
    }
    // Disable SIGPIPE per-socket BEFORE any write can race a daemon drop: on Darwin a
    // `write(2)` to a socket whose peer closed its read end otherwise raises SIGPIPE, whose
    // default disposition TERMINATES the process. The daemon closing/restarting is EXPECTED
    // (WorkGraphResync reconnects), so that write must return -1/EPIPE, never a signal.
    let noSigPipe = UnixSocketPosix.setNoSigPipe(descriptor)
    guard noSigPipe == 0 else {
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketOptionFailed(errno: noSigPipe)
    }
    let connectResult = UnixSocketPosix.withAddress(path: path) { addr, length in
      UnixSocketPosix.connectSocket(descriptor, addr, length)
    }
    guard let outcome = connectResult else {
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketPathTooLong(
        maxBytes: UnixSocketPosix.pathCapacity - 1)
    }
    guard outcome == 0 else {
      let failure = errno
      UnixSocketPosix.closeDescriptor(descriptor)
      throw UnixSocketTransportError.connectionFailed(errno: failure)
    }
    return UnixSocketTransport(connectedDescriptor: descriptor)
  }

  /// `async` dial that runs the blocking `socket(2)`/`connect(2)` on a dedicated off-executor
  /// thread (never a cooperative one), so a full listen backlog cannot stall the shared
  /// cooperative pool. Throws the same typed ``UnixSocketTransportError`` as the sync form.
  ///
  /// **NOT CANCELLATION-AWARE — tracked as issue #101.** Cancelling the caller does not abort
  /// the in-flight `connect(2)`; this continuation still resumes with a LIVE transport (or the
  /// dial's error) once the kernel answers. Making it cancellable means closing the fd from the
  /// cancel handler, which races the dial itself — real design work, deliberately out of #94's
  /// scope. Both call sites close what they are handed instead (``WorkGraphResync`` on every
  /// loop exit, `AppModel`'s connect loop on every exit including the post-`stop()` one), each
  /// pinned by a regression test. A NEW call site must do the same, or wait for #101.
  public static func connect(toUnixSocketPath path: String) async throws -> UnixSocketTransport {
    try await withCheckedThrowingContinuation { resume in
      dialQueue.async {
        do {
          resume.resume(returning: try connect(toUnixSocketPath: path))
        } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }
}
