import Foundation

@testable import SprinterBackend

/// A deterministic, offline ``RpcTransport`` for the gate: the test scripts the
/// server side by emitting inbound frames, and inspects the client's outbound
/// frames — no live daemon or network. `receive` and `send` are wired to two
/// in-memory streams.
final class FakeTransport: RpcTransport {
  private let inbound: AsyncThrowingStream<Data, any Error>
  private let inboundContinuation: AsyncThrowingStream<Data, any Error>.Continuation
  /// The client's outbound frames, in send order, for assertions.
  let outbound: AsyncStream<Data>
  private let outboundContinuation: AsyncStream<Data>.Continuation

  init() {
    (inbound, inboundContinuation) = AsyncThrowingStream<Data, any Error>.makeStream()
    (outbound, outboundContinuation) = AsyncStream<Data>.makeStream()
  }

  func send(_ bytes: Data) async throws {
    outboundContinuation.yield(bytes)
  }

  func receive() -> AsyncThrowingStream<Data, any Error> {
    inbound
  }

  /// Pushes one server → client frame (raw bytes, already newline-framed).
  func emit(_ data: Data) {
    inboundContinuation.yield(data)
  }

  /// Ends the inbound stream (server closed the connection).
  func close() {
    inboundContinuation.finish()
  }
}
