import Foundation

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Deterministic, in-process socket fixtures for exercising ``UnixSocketTransport``
/// against REAL kernel sockets — no live daemon, no network. Two shapes:
///
/// - ``RawSocketPeer/pair()`` — a `socketpair(2)`: one end wrapped in the transport,
///   the other a raw peer the test drives as the daemon, so real NDJSON frames
///   round-trip over a real socket without a listen/accept dance.
/// - ``LoopbackSocketServer`` — a bound/listening Unix-domain socket at a temp path,
///   so the transport's real ``UnixSocketTransport/connect(toUnixSocketPath:)`` +
///   ``DaemonTransports`` endpoint-selection path is exercised end-to-end.
enum LoopbackError: Error, Equatable {
  case setupFailed(String)
  case eof
}

/// The "daemon" side of a socket: raw blocking I/O hopped onto a private queue so a
/// test never blocks a cooperative executor. Reads are line-buffered (NDJSON).
final class RawSocketPeer: @unchecked Sendable {
  private let descriptor: Int32
  private let queue = DispatchQueue(label: "sprinter.test.peer")
  private var buffer = Data()  // confined to `queue`

  init(descriptor: Int32) {
    self.descriptor = descriptor
  }

  /// A `socketpair`: the client transport plus the raw daemon-side peer.
  static func pair() throws -> (transport: UnixSocketTransport, peer: RawSocketPeer) {
    var descriptors: [Int32] = [-1, -1]
    let result = socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors)
    guard result == 0 else { throw LoopbackError.setupFailed("socketpair errno \(errno)") }
    // Mirror production: the client-side descriptor gets `SO_NOSIGPIPE` (exactly as the
    // real dial does), so a broken-pipe `write(2)` surfaces as EPIPE/`writeFailed` instead
    // of a process-terminating SIGPIPE — the test proves production needs no SIG_IGN.
    let noSigPipe = setNoSigPipe(descriptors[0])
    guard noSigPipe == 0 else {
      _ = Darwin.close(descriptors[0])
      _ = Darwin.close(descriptors[1])
      throw LoopbackError.setupFailed("SO_NOSIGPIPE errno \(noSigPipe)")
    }
    return (
      transport: UnixSocketTransport(connectedDescriptor: descriptors[0]),
      peer: RawSocketPeer(descriptor: descriptors[1])
    )
  }

  /// Reads the next newline-delimited frame the client sent (without the newline).
  func nextLine() async throws -> Data {
    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Data, any Error>) in
      queue.async {
        do { resume.resume(returning: try self.blockingReadLine()) } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }

  /// Writes raw bytes (an already-NDJSON-framed daemon frame) to the client.
  func write(_ data: Data) async throws {
    try await withCheckedThrowingContinuation { (resume: CheckedContinuation<Void, any Error>) in
      queue.async {
        do {
          try self.blockingWrite(data)
          resume.resume()
        } catch {
          resume.resume(throwing: error)
        }
      }
    }
  }

  func close() {
    queue.sync { _ = Darwin.close(descriptor) }
  }

  private func blockingReadLine() throws -> Data {
    let newline: UInt8 = 0x0A
    while true {
      if let index = buffer.firstIndex(of: newline) {
        let line = buffer[buffer.startIndex..<index]
        buffer.removeSubrange(buffer.startIndex...index)
        return Data(line)
      }
      var chunk = [UInt8](repeating: 0, count: 4096)
      let count = chunk.withUnsafeMutableBytes { read(descriptor, $0.baseAddress, 4096) }
      if count > 0 {
        buffer.append(contentsOf: chunk[0..<count])
      } else {
        throw LoopbackError.eof
      }
    }
  }

  private func blockingWrite(_ data: Data) throws {
    try data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) in
      guard var pointer = raw.baseAddress else { return }
      var remaining = raw.count
      while remaining > 0 {
        let written = Darwin.write(descriptor, pointer, remaining)
        if written > 0 {
          pointer = pointer.advanced(by: written)
          remaining -= written
        } else if written < 0 && errno == EINTR {
          continue
        } else {
          throw LoopbackError.setupFailed("write errno \(errno)")
        }
      }
    }
  }
}

/// A bound/listening Unix-domain socket at a temp path — the fixture the transport's
/// real `connect` dials, so the endpoint-selection path (``DaemonTransports`` →
/// ``BackendConnector``) is exercised against a live socket, deterministically.
final class LoopbackSocketServer: @unchecked Sendable {
  let path: String
  private let listenDescriptor: Int32
  private let accepted = DispatchSemaphore(value: 0)
  private let lock = NSLock()
  private var acceptedDescriptor: Int32 = -1

  /// A SHORT absolute socket path derived under `NSTemporaryDirectory()` — so the fixture
  /// is not `/tmp`-only and works on hosts (incl. CI) with a relocated temp dir. Guarded
  /// against the `sockaddr_un.sun_path` limit (~104 bytes, NUL included): if the derived
  /// path would not fit, fall back to a short `/tmp` name so the bind always stays valid.
  static func makeSocketPath() -> String {
    let name = "sprinter-\(UUID().uuidString.prefix(8)).sock"
    let candidate = (NSTemporaryDirectory() as NSString).appendingPathComponent(name)
    // `unixSocketPathCapacity` includes the terminating NUL, so require a strict `<`.
    return candidate.utf8.count < unixSocketPathCapacity ? candidate : "/tmp/\(name)"
  }

  init() throws {
    path = Self.makeSocketPath()
    let listenDescriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard listenDescriptor >= 0 else { throw LoopbackError.setupFailed("socket errno \(errno)") }
    self.listenDescriptor = listenDescriptor
    guard let bound = withUnixSocketAddress(path: path, { bind(listenDescriptor, $0, $1) }),
      bound == 0
    else {
      _ = Darwin.close(listenDescriptor)
      throw LoopbackError.setupFailed("bind errno \(errno)")
    }
    guard listen(listenDescriptor, 1) == 0 else {
      _ = Darwin.close(listenDescriptor)
      throw LoopbackError.setupFailed("listen errno \(errno)")
    }
    let thread = Thread { [weak self] in
      guard let self else { return }
      let descriptor = accept(self.listenDescriptor, nil, nil)
      self.lock.lock()
      self.acceptedDescriptor = descriptor
      self.lock.unlock()
      self.accepted.signal()
    }
    thread.name = "sprinter.test.accept"
    thread.start()
  }

  /// Blocks until the client connects, then returns the accepted daemon-side peer.
  func acceptPeer() -> RawSocketPeer {
    accepted.wait()
    lock.lock()
    let descriptor = acceptedDescriptor
    lock.unlock()
    return RawSocketPeer(descriptor: descriptor)
  }

  func stop() {
    _ = Darwin.close(listenDescriptor)
    unlink(path)
  }
}
