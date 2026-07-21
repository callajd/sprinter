import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// The thin POSIX seam ``UnixSocketTransport`` is written against: address construction,
/// the full-write retry loop, and named wrappers for the raw syscalls.
///
/// A caseless `enum` NAMESPACE, deliberately: these are the module's POSIX seam, and names
/// like `closeDescriptor`/`writeAll` at module scope would be a collision waiting to happen
/// against any other adapter that grows its own. Qualifying every call site
/// (`UnixSocketPosix.closeDescriptor(fd)`) keeps the seam legible and keeps the module's
/// global namespace free of generically-named helpers. The members are `internal` so the
/// socket fixtures in the test target can reuse the SAME address construction and
/// `SO_NOSIGPIPE` setup as production — test behaviour cannot drift from it.
enum UnixSocketPosix {
  /// The capacity of `sockaddr_un.sun_path` on this platform (bytes, incl. the NUL).
  static let pathCapacity = MemoryLayout.size(ofValue: sockaddr_un().sun_path)

  /// Fills a `sockaddr_un` for `path` and invokes `body` with a pointer to it (rebound
  /// to `sockaddr`) and its length. Returns `nil` — without calling `body` — when the
  /// path does not fit `sun_path` (leaving room for the terminating NUL).
  static func withAddress<Result>(
    path: String,
    _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
  ) -> Result? {
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    let pathBytes = Array(path.utf8)
    guard pathBytes.count < pathCapacity else { return nil }
    withUnsafeMutablePointer(to: &addr.sun_path) { rawPointer in
      rawPointer.withMemoryRebound(to: CChar.self, capacity: pathCapacity) { destination in
        for (index, byte) in pathBytes.enumerated() {
          destination[index] = CChar(bitPattern: byte)
        }
        destination[pathBytes.count] = 0
      }
    }
    let length = socklen_t(MemoryLayout<sockaddr_un>.size)
    return withUnsafePointer(to: &addr) { pointer in
      pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockPointer in
        body(sockPointer, length)
      }
    }
  }

  /// Writes `data` in full, retrying short writes and `EINTR`.
  static func writeAll(_ descriptor: Int32, _ data: Data) throws {
    try data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) in
      guard var pointer = raw.baseAddress else { return }
      var remaining = raw.count
      while remaining > 0 {
        let written = write(descriptor, pointer, remaining)
        if written > 0 {
          pointer = pointer.advanced(by: written)
          remaining -= written
        } else if written < 0 && errno == EINTR {
          continue
        } else {
          throw UnixSocketTransportError.writeFailed(errno: errno)
        }
      }
    }
  }

  /// Sets `SO_NOSIGPIPE` on a stream socket so a `write(2)` to a peer that has closed its
  /// read end returns `-1`/`EPIPE` instead of raising `SIGPIPE` (whose default disposition
  /// TERMINATES the whole process). `SO_NOSIGPIPE` is the Darwin per-socket option and this
  /// codebase targets macOS/Darwin, so it is the correct mechanism here — the transport is
  /// self-safe and never relies on a process-wide `SIG_IGN`. Returns `0` on success, or the
  /// failing `errno`. Both dial paths and the `socketpair(2)` test fixture set it
  /// identically, keeping test behavior matched to production.
  static func setNoSigPipe(_ descriptor: Int32) -> Int32 {
    var one: Int32 = 1
    let result = setsockopt(
      descriptor, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    return result == 0 ? 0 : errno
  }

  // Thin wrappers so the POSIX syscalls are never shadowed by the transport type's own
  // `close()`/`connect`-style members and read cleanly at the call sites. Each is named
  // distinctly from the syscall it wraps so the unqualified call below still resolves to
  // the libc function, never recursively to the wrapper.
  static func connectSocket(
    _ descriptor: Int32, _ addr: UnsafePointer<sockaddr>, _ length: socklen_t
  ) -> Int32 {
    connect(descriptor, addr, length)
  }

  static func shutdownDescriptor(_ descriptor: Int32) {
    _ = shutdown(descriptor, Int32(SHUT_RDWR))
  }

  /// A single NON-BLOCKING `recv(2)` (`MSG_DONTWAIT` — per-call, so the descriptor's blocking
  /// mode is left alone and ``writeAll(_:_:)`` keeps its simple blocking form). Returns the
  /// byte count, `0` on EOF, or `-1` with `errno` set.
  static func receiveWithoutBlocking(
    _ descriptor: Int32, into buffer: UnsafeMutableRawBufferPointer
  ) -> Int {
    recv(descriptor, buffer.baseAddress, buffer.count, MSG_DONTWAIT)
  }

  static func closeDescriptor(_ descriptor: Int32) {
    _ = close(descriptor)
  }
}

/// Transport-level failures raised while dialing or writing the Unix-domain socket,
/// distinct from the daemon's owned ``ContractError`` channel and the envelope-level
/// ``BackendError``.
///
/// Housed WITH the POSIX seam above rather than with ``UnixSocketTransport``: every case is a
/// raw `errno` (or a `sun_path` capacity) raised by one of these wrappers — ``writeAll`` throws
/// ``writeFailed`` directly — and the transport file is at SwiftLint's 400-line cap now that
/// its teardown reasoning is documented in place.
public enum UnixSocketTransportError: Error, Equatable, Sendable {
  /// `socket(2)` failed to allocate a descriptor.
  case socketCreationFailed(errno: Int32)
  /// `connect(2)` failed (no daemon listening, permission denied, …).
  case connectionFailed(errno: Int32)
  /// `setsockopt(2)` could not set a required socket option (e.g. `SO_NOSIGPIPE`).
  case socketOptionFailed(errno: Int32)
  /// The socket path is too long for the platform's `sun_path` buffer.
  case socketPathTooLong(maxBytes: Int)
  /// A `write(2)` failed before the whole frame was flushed.
  case writeFailed(errno: Int32)
  /// The bounded inbound receive buffer overflowed: the read loop produced chunks faster
  /// than the connection consumed them, past the bound. Surfaced instead of silently
  /// dropping bytes (which would corrupt NDJSON framing); the reconnect/resync loop
  /// recovers with a fresh incremental resume.
  case receiveBufferOverflow
  /// A `.remoteDaemon` endpoint was selected, but no remote transport adapter exists yet
  /// (CE1/CE2 serve only a local Unix-domain socket). Distinct from a dial failure — this
  /// is "no adapter", not "the dial did not connect".
  case remoteEndpointUnsupported
}
