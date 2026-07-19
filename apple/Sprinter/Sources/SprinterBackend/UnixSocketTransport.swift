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
/// feeds the `receive()` stream; outbound writes are serialized on a private queue
/// so a blocking `write(2)` never occupies a cooperative executor thread. All
/// mutable descriptor state is guarded by a lock, so `close()` may race a `send`
/// or the read loop safely (`@unchecked Sendable` is discharged by that lock).
public final class UnixSocketTransport: RpcTransport, @unchecked Sendable {
  private let inbound: AsyncThrowingStream<Data, any Error>
  private let continuation: AsyncThrowingStream<Data, any Error>.Continuation
  private let writeQueue = DispatchQueue(label: "sprinter.UnixSocketTransport.write")

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
  /// `socket(2)`/`connect(2)` or an over-long path — never a force-unwrap.
  public static func connect(toUnixSocketPath path: String) throws -> UnixSocketTransport {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw UnixSocketTransportError.socketCreationFailed(errno: errno)
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

  public func send(_ bytes: Data) async throws {
    let (descriptor, closed) = lock.withLock { (self.descriptor, isClosed) }
    guard !closed, descriptor >= 0 else { throw BackendError.connectionClosed }

    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Void, any Error>) in
      writeQueue.async {
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
    lock.lock()
    if isClosed {
      lock.unlock()
      return
    }
    isClosed = true
    let descriptor = self.descriptor
    self.descriptor = -1
    lock.unlock()

    if descriptor >= 0 {
      // `shutdown` unblocks a read blocked in the read loop (it returns EOF), so the
      // loop exits promptly; then release the descriptor.
      shutdownDescriptor(descriptor)
      closeDescriptor(descriptor)
    }
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
  /// The socket path is too long for the platform's `sun_path` buffer.
  case socketPathTooLong(maxBytes: Int)
  /// A `write(2)` failed before the whole frame was flushed.
  case writeFailed(errno: Int32)
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
