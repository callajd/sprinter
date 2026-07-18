import Foundation
import Testing

@testable import SprinterBackend

@Suite("NDJSON framing")
struct NdjsonFramingTests {
  @Test("encodes a frame with a single trailing newline")
  func encodesWithNewline() throws {
    let data = try encodeFrame(.ping)
    #expect(data.last == 0x0A)
    #expect(data.filter { $0 == 0x0A }.count == 1)
  }

  @Test("reassembles a whole line into one frame")
  func decodesWholeLine() throws {
    var reassembler = NdjsonReassembler()
    let frames = try reassembler.push(Data((#"{"_tag":"Pong"}"# + "\n").utf8))
    #expect(frames.count == 1)
  }

  @Test("splits multiple frames delivered in one chunk")
  func decodesMultipleFrames() throws {
    var reassembler = NdjsonReassembler()
    let bytes = Data((#"{"_tag":"Pong"}"# + "\n" + #"{"_tag":"Pong"}"# + "\n").utf8)
    #expect(try reassembler.push(bytes).count == 2)
  }

  @Test("buffers a partial line until its newline arrives")
  func buffersPartialLine() throws {
    var reassembler = NdjsonReassembler()
    #expect(try reassembler.push(Data(#"{"_tag":"Po"#.utf8)).isEmpty)
    #expect(try reassembler.push(Data(#"ng"}"#.utf8)).isEmpty)
    #expect(try reassembler.push(Data("\n".utf8)).count == 1)
  }

  @Test("skips empty lines")
  func skipsEmptyLines() throws {
    var reassembler = NdjsonReassembler()
    let bytes = Data(("\n" + #"{"_tag":"Pong"}"# + "\n").utf8)
    #expect(try reassembler.push(bytes).count == 1)
  }

  @Test("a malformed line is a decode failure")
  func rejectsMalformedLine() {
    var reassembler = NdjsonReassembler()
    #expect(throws: (any Error).self) {
      _ = try reassembler.push(Data("not json\n".utf8))
    }
  }
}
