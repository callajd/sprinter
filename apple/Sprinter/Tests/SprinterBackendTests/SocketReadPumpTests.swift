import Foundation
import Testing

@testable import SprinterBackend

#if canImport(Darwin)
  import Darwin
#endif

/// Direct cover for ``SocketReadPump`` — one turn of ``UnixSocketTransport``'s read loop.
///
/// Tested as a unit because two of its four outcomes are otherwise unreachable from the
/// transport: a `recv` that finds nothing (`EAGAIN`, the readiness that did not survive) and
/// the spin guard that outcome feeds. Those are exactly the paths that would burn a real OS
/// thread at 100% if they were ever taken in a loop, so "unreachable today" is not a reason to
/// leave them unexercised.
@Suite("Socket read pump")
struct SocketReadPumpTests {
  private static func socketPair() throws -> (local: Int32, peer: Int32) {
    var descriptors: [Int32] = [-1, -1]
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0 else {
      throw LoopbackError.setupFailed("socketpair errno \(errno)")
    }
    return (descriptors[0], descriptors[1])
  }

  @Test("bytes on the socket are yielded to the inbound stream")
  func yieldsReceivedBytes() async throws {
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let (stream, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    let frame = Data("{\"k\":\"v\"}\n".utf8)
    try frame.withUnsafeBytes { raw in
      guard Darwin.write(peer, raw.baseAddress, raw.count) == raw.count else {
        throw LoopbackError.setupFailed("short write")
      }
    }
    #expect(pump.pumpOnce(descriptor: local, yieldingTo: continuation) == .again)
    var chunks = stream.makeAsyncIterator()
    #expect(try await chunks.next() == frame)
  }

  @Test("a peer EOF ends the stream")
  func peerEofEndsTheStream() throws {
    let (local, peer) = try Self.socketPair()
    defer { _ = Darwin.close(local) }
    let (_, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    _ = Darwin.close(peer)
    #expect(pump.pumpOnce(descriptor: local, yieldingTo: continuation) == .endOfStream)
  }

  @Test("a readiness that does not survive to the read is retried, not parked on")
  func lapsedReadinessRetries() throws {
    // `MSG_DONTWAIT` is what makes this survivable: an empty socket returns `EAGAIN` instead
    // of blocking the read thread until bytes that may never come.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let (_, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    #expect(pump.pumpOnce(descriptor: local, yieldingTo: continuation) == .again)
  }

  @Test("a run of lapsed readiness is rate-limited rather than spun on")
  func repeatedLapsesArePaused() throws {
    // FINDING 5 of #108's review: `poll` says readable → `recv` says `EAGAIN` → straight back
    // to `poll` is a 100%-CPU spin on a REAL OS thread. Unreachable with a single reader on a
    // `SOCK_STREAM` socket, but nothing ENFORCED that, so the pump now pauses after a run of
    // fruitless turns. Enough turns to cross the threshold must therefore take measurably
    // longer than the syscalls alone; without the guard the whole loop is microseconds.
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let (_, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    let started = DispatchTime.now()
    for _ in 0..<64 {
      #expect(pump.pumpOnce(descriptor: local, yieldingTo: continuation) == .again)
    }
    let elapsed = Double(DispatchTime.now().uptimeNanoseconds - started.uptimeNanoseconds) / 1e6
    #expect(elapsed >= 1.0, "64 fruitless turns never paused (\(elapsed)ms): the spin guard is off")
  }

  @Test("a non-retryable read error ends the stream")
  func hardReadErrorEndsTheStream() throws {
    // The descriptor closed under us, or any other errno: not a lapse, so it must END the
    // loop rather than feed the retry path forever. `-1` rather than a just-closed fd on
    // purpose: a real number can be recycled by a parallel test between the close and the
    // read, which would make this assertion race. `EBADF` is `EBADF` either way.
    let (_, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    #expect(pump.pumpOnce(descriptor: -1, yieldingTo: continuation) == .endOfStream)
  }

  @Test("a stream already finished by its consumer stops the pump")
  func terminatedStreamStopsThePump() throws {
    let (local, peer) = try Self.socketPair()
    defer {
      _ = Darwin.close(local)
      _ = Darwin.close(peer)
    }
    let (_, continuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    var pump = SocketReadPump()
    var byte: UInt8 = 0x7B
    #expect(withUnsafeBytes(of: &byte) { Darwin.write(peer, $0.baseAddress, 1) } == 1)
    continuation.finish()
    #expect(pump.pumpOnce(descriptor: local, yieldingTo: continuation) == .streamAlreadyFinished)
  }
}
