import Foundation

/// A duplex byte transport the RPC client speaks over (BE1.1 / D4 / D14).
///
/// The client frames the `effect/unstable/rpc` envelope as NDJSON
/// (`RpcSerialization.ndjson`) and moves those bytes over an **injected**
/// transport — never a hardcoded socket. "Where the daemon runs / which
/// transport" is an adapter choice (a concrete conformer), so a local-daemon and
/// a remote-daemon connection differ only by which `RpcTransport` is supplied
/// (INV-PORT). The live socket / TCP conformers are convergence/cutover work; the
/// client is built and tested against this seam and an in-memory fake.
///
/// `send` accepts an already-serialized NDJSON frame (one JSON message plus its
/// trailing newline). `receive` yields raw inbound byte chunks — arbitrarily
/// split relative to message boundaries — which the client reassembles with the
/// NDJSON framer.
public protocol RpcTransport: Sendable {
  /// Writes one serialized outbound frame to the transport.
  func send(_ bytes: Data) async throws

  /// The inbound byte stream. Each element is an arbitrary chunk (not necessarily
  /// a whole line); the client buffers and splits on the newline delimiter.
  func receive() -> AsyncThrowingStream<Data, any Error>

  /// Closes the transport: ends the inbound `receive()` stream and releases the
  /// underlying resource (a real socket; a no-op once already closed). Called by
  /// the connection's teardown so a dropped ``Backend`` does not leak the transport.
  func close()

  /// Suspends until an initiated ``close()`` has FULLY drained — the read loop has left
  /// `read(2)` and the write queue has flushed, so the underlying resource (fd) is
  /// released. The reconnect path awaits this so the OLD transport is fully torn down
  /// BEFORE the new socket is dialed (the CE2.1 carried teardown constraint): no
  /// in-flight frame crosses connections and the old fd is reclaimed first. An in-memory
  /// transport has no such resource, so the default is a no-op; the live socket
  /// conformer overrides it.
  func awaitClosed() async
}

extension RpcTransport {
  /// Default: nothing to drain (an in-memory transport holds no OS resource). The live
  /// ``UnixSocketTransport`` overrides this to await its read-loop exit and deferred
  /// `close(2)`.
  public func awaitClosed() async {}
}
