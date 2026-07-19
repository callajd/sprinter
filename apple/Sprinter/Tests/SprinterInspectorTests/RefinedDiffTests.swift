import SprinterContract
import SprinterSession
import Testing

@testable import SprinterInspector

@Suite("Refined (intraline LCS) diff")
struct RefinedDiffTests {
  private func editCall(old: String, new: String, path: String? = nil) -> TranscriptToolCall {
    var fields: [String: JSONValue] = ["old_string": .string(old), "new_string": .string(new)]
    if let path { fields["file_path"] = .string(path) }
    return TranscriptToolCall(
      id: "t1", name: "Edit", input: .object(fields), output: nil, isError: false, isComplete: true)
  }

  /// The LCS pass collapses lines common to both sides to `.unchanged` context, so
  /// only the genuinely changed line renders as removed+added churn.
  @Test("unchanged lines around a change render as context, not churn")
  func unchangedLinesBecomeContext() {
    let lines = DiffRefinement.align(removed: ["a", "b", "c"], added: ["a", "X", "c"])
    #expect(
      lines == [
        RefinedDiffLine(kind: .unchanged, text: "a"),
        RefinedDiffLine(kind: .removed, text: "b"),
        RefinedDiffLine(kind: .added, text: "X"),
        RefinedDiffLine(kind: .unchanged, text: "c")
      ])
  }

  /// An edit whose old and new sides are identical shows NO churn at all — every line
  /// is context (the core CE3.2 goal: unchanged lines aren't churn).
  @Test("an identical old/new edit shows only context lines")
  func identicalEditIsAllContext() {
    let refined = editCall(old: "x\ny", new: "x\ny").fileDiff?.refined
    #expect(
      refined?.lines == [
        RefinedDiffLine(kind: .unchanged, text: "x"),
        RefinedDiffLine(kind: .unchanged, text: "y")
      ])
  }

  /// A pure-addition side (a `Write`, or a new block) yields only `.added` lines.
  @Test("a pure addition yields only added lines")
  func pureAdditionAllAdded() {
    let lines = DiffRefinement.align(removed: [], added: ["one", "two"])
    #expect(
      lines == [
        RefinedDiffLine(kind: .added, text: "one"),
        RefinedDiffLine(kind: .added, text: "two")
      ])
  }

  /// A pure removal yields only `.removed` lines.
  @Test("a pure removal yields only removed lines")
  func pureRemovalAllRemoved() {
    let lines = DiffRefinement.align(removed: ["gone"], added: [])
    #expect(lines == [RefinedDiffLine(kind: .removed, text: "gone")])
  }

  /// An empty hunk refines to no lines.
  @Test("an empty hunk refines to no lines")
  func emptyHunkEmpty() {
    #expect(DiffRefinement.align(removed: [], added: []).isEmpty)
  }

  /// `refined` reads the file path and realigns off the coarse `TranscriptDiff`: a
  /// shared prefix line stays context while the changed tail is removed+added.
  @Test("TranscriptDiff.refined preserves the path and realigns off the coarse hunk")
  func refinedFromTranscriptDiff() {
    let call = editCall(old: "keep\nold", new: "keep\nnew", path: "/src/a.swift")
    let refined = call.fileDiff?.refined
    #expect(refined?.filePath == "/src/a.swift")
    #expect(
      refined?.lines == [
        RefinedDiffLine(kind: .unchanged, text: "keep"),
        RefinedDiffLine(kind: .removed, text: "old"),
        RefinedDiffLine(kind: .added, text: "new")
      ])
  }
}
