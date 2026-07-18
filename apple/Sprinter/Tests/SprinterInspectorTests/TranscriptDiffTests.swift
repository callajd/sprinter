import SprinterContract
import SprinterSession
import Testing

@testable import SprinterInspector

@Suite("Transcript diff transform")
struct TranscriptDiffTests {
  private func toolCall(name: String, input: JSONValue) -> TranscriptToolCall {
    TranscriptToolCall(
      id: "tool-1", name: name, input: input, output: nil, isError: false, isComplete: true)
  }

  /// An `Edit` tool call renders `old_string` as removed lines and `new_string` as
  /// added lines, in that order, with the file path surfaced.
  @Test("an Edit tool call renders removed-then-added lines")
  func editRendersDiff() {
    let call = toolCall(
      name: "Edit",
      input: .object([
        "file_path": .string("/src/main.swift"),
        "old_string": .string("let a = 1\nlet b = 2"),
        "new_string": .string("let a = 1\nlet b = 3")
      ]))

    let diff = call.fileDiff
    #expect(diff?.filePath == "/src/main.swift")
    #expect(
      diff?.lines == [
        DiffLine(kind: .removed, text: "let a = 1"),
        DiffLine(kind: .removed, text: "let b = 2"),
        DiffLine(kind: .added, text: "let a = 1"),
        DiffLine(kind: .added, text: "let b = 3")
      ])
  }

  /// A `Write` tool call renders `content` as all-added lines (a whole-file write).
  @Test("a Write tool call renders content as added lines")
  func writeRendersAdditions() {
    let call = toolCall(
      name: "Write",
      input: .object([
        "file_path": .string("/src/new.swift"),
        "content": .string("line one\nline two\n")
      ]))

    let diff = call.fileDiff
    #expect(diff?.filePath == "/src/new.swift")
    // The single trailing newline does not produce a spurious blank line.
    #expect(
      diff?.lines == [
        DiffLine(kind: .added, text: "line one"),
        DiffLine(kind: .added, text: "line two")
      ])
  }

  /// Recognition is case-insensitive on the tool name.
  @Test("tool-name matching is case-insensitive")
  func matchingIsCaseInsensitive() {
    let call = toolCall(
      name: "edit",
      input: .object([
        "old_string": .string("x"),
        "new_string": .string("y")
      ]))
    #expect(call.fileDiff?.filePath == nil)
    #expect(
      call.fileDiff?.lines == [
        DiffLine(kind: .removed, text: "x"),
        DiffLine(kind: .added, text: "y")
      ])
  }

  /// A pure-insertion `Edit` (empty `old_string`) renders only added lines — the
  /// empty side yields NO lines, not a spurious blank removed line.
  @Test("a pure-insertion Edit renders no blank line for the empty side")
  func pureInsertionHasNoBlankLine() {
    let call = toolCall(
      name: "Edit",
      input: .object([
        "file_path": .string("/src/main.swift"),
        "old_string": .string(""),
        "new_string": .string("let added = 1")
      ]))
    #expect(
      call.fileDiff?.lines == [DiffLine(kind: .added, text: "let added = 1")])
  }

  /// A `Write` of empty `content` renders no lines at all — not one blank added line.
  @Test("an empty Write renders no lines")
  func emptyWriteHasNoLines() {
    let call = toolCall(
      name: "Write",
      input: .object([
        "file_path": .string("/src/empty.swift"),
        "content": .string("")
      ]))
    #expect(call.fileDiff?.lines.isEmpty == true)
  }

  /// A non-edit tool call is not a diff (so the transcript renders it plainly).
  @Test("a non-edit tool call yields no diff")
  func nonEditYieldsNil() {
    let call = toolCall(name: "Bash", input: .object(["command": .string("ls")]))
    #expect(call.fileDiff == nil)
  }

  /// A malformed edit payload (missing/mistyped fields) degrades to an empty diff
  /// rather than trapping (INV-NOFORCE).
  @Test("a malformed edit payload degrades to an empty diff")
  func malformedEditIsEmpty() {
    let call = toolCall(name: "Edit", input: .array([.string("not an object")]))
    let diff = call.fileDiff
    #expect(diff?.filePath == nil)
    #expect(diff?.lines.isEmpty == true)
  }
}
