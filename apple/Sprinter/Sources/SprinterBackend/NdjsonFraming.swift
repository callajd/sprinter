import Foundation

/// NDJSON framing for the RPC envelope (`RpcSerialization.ndjson`).
///
/// Outbound: one JSON message terminated by a single `\n`. Inbound: a stateful
/// reassembler that buffers arbitrary byte chunks and yields one decoded
/// ``ServerFrame`` per completed line — mirroring the reference `ndjson`
/// serializer, which accumulates bytes and splits on the newline delimiter.

/// The newline byte (`\n`) that frames each NDJSON message.
private let newline: UInt8 = 0x0A

/// Serializes an outbound ``ClientFrame`` to a newline-terminated NDJSON line.
func encodeFrame(_ frame: ClientFrame) throws -> Data {
  var data = try JSONEncoder().encode(frame)
  data.append(newline)
  return data
}

/// A stateful newline-delimited decoder. Not thread-safe by itself; the client
/// confines it to its connection actor.
struct NdjsonReassembler {
  private var buffer = Data()

  /// Appends `chunk` and returns every ``ServerFrame`` now completed by a newline.
  /// A trailing partial line is retained for the next chunk.
  mutating func push(_ chunk: Data) throws -> [ServerFrame] {
    buffer.append(chunk)
    var frames: [ServerFrame] = []
    let decoder = JSONDecoder()
    while let newlineIndex = buffer.firstIndex(of: newline) {
      let line = buffer[buffer.startIndex..<newlineIndex]
      buffer.removeSubrange(buffer.startIndex...newlineIndex)
      if line.isEmpty { continue }
      frames.append(try decoder.decode(ServerFrame.self, from: Data(line)))
    }
    return frames
  }
}
