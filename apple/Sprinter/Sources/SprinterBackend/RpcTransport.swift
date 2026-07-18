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
}
