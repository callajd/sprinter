import Foundation

#if canImport(Darwin)
  import Darwin
#endif

/// The concrete ``RpcTransport`` that dials the daemon's **Unix-domain socket**
/// (CE2.1 / CE1.2's served wire).
///
/// This is the ONE new adapter behind the ``Backend`` port (INV-PORT): a live
/// byte duplex over an `AF_UNIX` `SOCK_STREAM` socket. It is *only* a byte pipe —
/// the `effect/unstable/rpc` **envelope** encode/decode (``Envelope``) and the
/// NDJSON line framing (``NdjsonFraming``) live one layer up in ``RpcConnection``
/// and are reused UNCHANGED. `send` writes an already-NDJSON-framed frame;
/// `receive()` yields raw inbound chunks (split arbitrarily relative to line
/// boundaries) that the connection reassembles. So a local-socket and any future
/// remote connection differ only by which ``RpcTransport`` is supplied — nothing
/// above the port learns about sockets or localness.
///
/// Concurrency: inbound bytes are pumped on a dedicated blocking read thread that
/// feeds the `receive()` stream; outbound writes are serialized on a private
/// `writeQueue` so a blocking `write(2)` never occupies a cooperative executor
/// thread; the blocking dial (`socket(2)`/`connect(2)`) is likewise hopped off the
/// cooperative executor by ``connect(toUnixSocketPath:)``'s `async` form.
///
/// SIGPIPE self-safety: every descriptor this transport writes to has `SO_NOSIGPIPE`
/// set (``setNoSigPipe(_:)``) the moment it is created — the dial path in
/// ``connect(toUnixSocketPath:)`` and the `socketpair(2)` test fixture alike — so a
/// `write(2)` racing an expected daemon drop returns `EPIPE` (``UnixSocketTransportError/writeFailed``)
/// instead of raising a process-terminating `SIGPIPE`. The transport never depends on a
/// process-wide `signal(SIGPIPE, SIG_IGN)`.
///
/// It is the fd's *lifetime* — not just the `descriptor` variable — that makes a
/// `close()` racing a `send` safe. The lock alone does NOT protect the fd across the
/// blocking `write(2)`: were `close()` to `close(2)` the descriptor while a queued
/// write is mid-flight, that fd number could be reused by a concurrent dial (a
/// reconnect, or a second backend) and the write would land RPC bytes on an
/// unrelated connection. So the real `close(2)` is serialized onto the SAME
/// `writeQueue` as writes, running strictly after the last queued write on that fd,
/// and `send` re-checks `isClosed` on that queue and skips a closed fd. The lock
/// guards the `descriptor`/`isClosed` *variables*; `writeQueue` guards the fd's
/// *lifetime* (`@unchecked Sendable` is discharged by both together).
///
/// `close()` is LOAD-BEARING for this conformer: it owns a real OS thread (the read
/// loop, parked in `read(2)`) and a file descriptor. The parked read thread retains
/// `self`, so `deinit` can never fire while it runs — skipping `close()` leaks BOTH
/// the thread and the fd. The connection's teardown must call it.
public final class UnixSocketTransport: RpcTransport, @unchecked Sendable {
  private let inbound: AsyncThrowingStream<Data, any Error>
  private let continuation: AsyncThrowingStream<Data, any Error>.Continuation
  private let writeQueue = DispatchQueue(label: "sprinter.UnixSocketTransport.write")

  /// Off-executor home for the blocking dial (`socket(2)`/`connect(2)`): a full listen
  /// backlog can park `connect(2)` for a while, so it must never run on a cooperative
  /// executor thread. Concurrent so independent dials don't serialize behind each other.
  private static let dialQueue = DispatchQueue(
    label: "sprinter.UnixSocketTransport.dial", attributes: .concurrent)

  /// Guards ``descriptor`` and ``isClosed`` across the read thread, the write
  /// queue, and a caller's `close()`.
  private let lock = NSLock()
  /// The connected socket file descriptor; `-1` once closed.
  private var descriptor: Int32
  private var isClosed = false

  /// Wraps an already-connected socket descriptor and starts pumping inbound bytes.
  /// Internal so tests can drive the framing seam over a `socketpair(2)` peer without
  /// a real `connect` — production goes through ``connect(toUnixSocketPath:)``.
  init(connectedDescriptor descriptor: Int32) {
    self.descriptor = descriptor
    (inbound, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    startReadLoop()
  }

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
    // `write(2)` to a socket whose peer closed its read end otherwise raises SIGPIPE,
    // whose default disposition TERMINATES the process. The daemon closing/restarting is
    // EXPECTED (WorkGraphResync reconnects on drops), so the broken-pipe write must
    // return -1/EPIPE (→ ``UnixSocketTransportError/writeFailed``), never a signal.
    let noSigPipe = setNoSigPipe(descriptor)
    guard noSigPipe == 0 else {
      closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketOptionFailed(errno: noSigPipe)
    }
    let connectResult = withUnixSocketAddress(path: path) { addr, length in
      connectSocket(descriptor, addr, length)
    }
    guard let outcome = connectResult else {
      closeDescriptor(descriptor)
      throw UnixSocketTransportError.socketPathTooLong(maxBytes: unixSocketPathCapacity - 1)
    }
    guard outcome == 0 else {
      let failure = errno
      closeDescriptor(descriptor)
      throw UnixSocketTransportError.connectionFailed(errno: failure)
    }
    return UnixSocketTransport(connectedDescriptor: descriptor)
  }

  /// `async` dial that runs the blocking `socket(2)`/`connect(2)` on a dedicated
  /// off-executor thread (never a cooperative one), so a full listen backlog cannot
  /// stall the shared cooperative pool. Throws the same typed
  /// ``UnixSocketTransportError`` as the synchronous form.
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

  /// Writes one already-NDJSON-framed frame to the socket. A closed connection surfaces
  /// as ``BackendError/connectionClosed`` (the fd is gone — nothing is written); a
  /// mid-write `write(2)` failure surfaces as ``UnixSocketTransportError/writeFailed``.
  public func send(_ bytes: Data) async throws {
    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Void, any Error>) in
      writeQueue.async {
        // Read the descriptor ON the write queue — the same serial queue onto which
        // `close()` defers the real `close(2)`. So between this check and `write(2)`
        // the fd cannot be released (and its number reused by a concurrent dial): a
        // closed connection is skipped here, never written to a freed/reused fd.
        let descriptor: Int32 = self.lock.withLock { self.isClosed ? -1 : self.descriptor }
        guard descriptor >= 0 else {
          resume.resume(throwing: BackendError.connectionClosed)
          return
        }
        do {
          try writeAll(descriptor, bytes)
          resume.resume()
        } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }

  public func receive() -> AsyncThrowingStream<Data, any Error> {
    inbound
  }

  public func close() {
    let descriptor: Int32 = lock.withLock {
      guard !isClosed else { return -1 }
      isClosed = true
      let current = self.descriptor
      self.descriptor = -1
      return current
    }
    guard descriptor >= 0 else { return }  // already closed — idempotent no-op.

    // `shutdown` immediately unblocks a read parked in the read loop (it returns EOF),
    // so the loop exits promptly. The real `close(2)` is DEFERRED onto `writeQueue`, so
    // it runs strictly AFTER any writes already enqueued on this fd — the fd number can
    // never be released (and reused by a concurrent dial) while a queued write is
    // mid-flight. `send` re-checks `isClosed` on the same queue, so a write enqueued
    // after this point is skipped rather than landing on the closed fd.
    shutdownDescriptor(descriptor)
    writeQueue.async { closeDescriptor(descriptor) }
    continuation.finish()
  }

  // MARK: - Inbound pump

  private func startReadLoop() {
    let thread = Thread { [weak self] in
      self?.readLoop()
    }
    thread.name = "sprinter.UnixSocketTransport.read"
    thread.start()
  }

  private func readLoop() {
    let bufferSize = 64 * 1024
    var buffer = [UInt8](repeating: 0, count: bufferSize)
    while true {
      lock.lock()
      let descriptor = self.descriptor
      let closed = isClosed
      lock.unlock()
      guard !closed, descriptor >= 0 else { break }

      let count = buffer.withUnsafeMutableBytes { raw in
        read(descriptor, raw.baseAddress, bufferSize)
      }
      if count > 0 {
        continuation.yield(Data(buffer[0..<count]))
      } else if count == 0 {
        break  // EOF: the daemon closed the connection.
      } else if errno == EINTR {
        continue  // interrupted syscall — retry.
      } else {
        break  // read error (incl. the descriptor closed under us) — end the stream.
      }
    }
    continuation.finish()
  }
}

/// Transport-level failures raised while dialing or writing the Unix-domain socket,
/// distinct from the daemon's owned ``ContractError`` channel and the envelope-level
/// ``BackendError``.
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
  /// A `.remoteDaemon` endpoint was selected, but no remote transport adapter exists yet
  /// (CE1/CE2 serve only a local Unix-domain socket). Distinct from a dial failure — this
  /// is "no adapter", not "the dial did not connect".
  case remoteEndpointUnsupported
}

// MARK: - POSIX helpers

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
private func writeAll(_ descriptor: Int32, _ data: Data) throws {
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
private func connectSocket(
  _ descriptor: Int32, _ addr: UnsafePointer<sockaddr>, _ length: socklen_t
) -> Int32 {
  connect(descriptor, addr, length)
}

private func shutdownDescriptor(_ descriptor: Int32) {
  _ = shutdown(descriptor, Int32(SHUT_RDWR))
}

private func closeDescriptor(_ descriptor: Int32) {
  _ = close(descriptor)
}
