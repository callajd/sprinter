import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// The thin POSIX seam ``UnixSocketTransport`` is written against: address construction,
/// the full-write retry loop, and named wrappers for the raw syscalls.
///
/// Split out of `UnixSocketTransport.swift` so that file stays within the module's
/// file-length budget while keeping the transport's teardown/lifetime reasoning documented
/// in place. These are `internal` (not `private`) purely because they now live in a
/// separate file from their only production caller — plus the socket fixtures in the test
/// target, which deliberately reuse the SAME address construction and `SO_NOSIGPIPE` setup
/// as production so test behaviour cannot drift from it.

/// The capacity of `sockaddr_un.sun_path` on this platform (bytes, incl. the NUL).
let unixSocketPathCapacity = MemoryLayout.size(ofValue: sockaddr_un().sun_path)

/// Fills a `sockaddr_un` for `path` and invokes `body` with a pointer to it (rebound
/// to `sockaddr`) and its length. Returns `nil` — without calling `body` — when the
/// path does not fit `sun_path` (leaving room for the terminating NUL). Internal so
/// the loopback test server reuses the exact same address construction.
func withUnixSocketAddress<Result>(
  path: String,
  _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
) -> Result? {
  var addr = sockaddr_un()
  addr.sun_family = sa_family_t(AF_UNIX)
  let pathBytes = Array(path.utf8)
  guard pathBytes.count < unixSocketPathCapacity else { return nil }
  withUnsafeMutablePointer(to: &addr.sun_path) { rawPointer in
    rawPointer.withMemoryRebound(to: CChar.self, capacity: unixSocketPathCapacity) { destination in
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
func writeAll(_ descriptor: Int32, _ data: Data) throws {
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
/// failing `errno`. Internal so both dial paths and the `socketpair(2)` test fixture set
/// it identically, keeping test behavior matched to production.
func setNoSigPipe(_ descriptor: Int32) -> Int32 {
  var one: Int32 = 1
  let result = setsockopt(
    descriptor, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
  return result == 0 ? 0 : errno
}

// Thin file-scope wrappers so the POSIX syscalls are never shadowed by the type's own
// `close()`/`connect`-style members and read cleanly at the call sites.
func connectSocket(
  _ descriptor: Int32, _ addr: UnsafePointer<sockaddr>, _ length: socklen_t
) -> Int32 {
  connect(descriptor, addr, length)
}

func shutdownDescriptor(_ descriptor: Int32) {
  _ = shutdown(descriptor, Int32(SHUT_RDWR))
}

func closeDescriptor(_ descriptor: Int32) {
  _ = close(descriptor)
}
