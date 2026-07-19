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

  /// The memoized refinement (``RefinedDiffCache``) equals a fresh `TranscriptDiff.refined`
  /// compute — the cache is a pure performance layer, never changing the result — and a
  /// re-read at the same content returns the identical cached value.
  @Test("the cached refinement equals a fresh compute and re-reads are stable")
  func cacheEqualsFreshCompute() {
    let call = editCall(old: "a\nb\nc", new: "a\nX\nc", path: "/src/a.swift")
    let cache = RefinedDiffCache()

    let cached = cache.refined(for: call)
    #expect(cached != nil)
    #expect(cached == call.fileDiff?.refined)
    // A re-read of unchanged content returns the identical cached refinement.
    #expect(cache.refined(for: call) == cached)
  }

  /// A non-edit tool call has no diff, so the cache returns `nil` (and never traps).
  @Test("the cache returns nil for a non-edit tool call")
  func cacheNilForNonEdit() {
    let read = TranscriptToolCall(
      id: "r1", name: "Read", input: .object(["file_path": .string("/x")]), output: nil,
      isError: false, isComplete: true)
    #expect(RefinedDiffCache().refined(for: read) == nil)
  }

  /// The cache is keyed on the diff's STABLE identity (id + coarse content): re-issuing
  /// the SAME tool-call id with changed `old_string`/`new_string` invalidates the entry
  /// and re-refines, rather than serving the stale prior refinement.
  @Test("the cache invalidates when the diff content changes at the same id")
  func cacheInvalidatesOnContentChange() {
    let cache = RefinedDiffCache()
    let first = editCall(old: "a\nb", new: "a\nX")
    let firstRefined = cache.refined(for: first)
    #expect(firstRefined == first.fileDiff?.refined)

    // Same id "t1", different content — must NOT return the stale first refinement.
    let second = editCall(old: "a\nb", new: "a\nY")
    let secondRefined = cache.refined(for: second)
    #expect(secondRefined == second.fileDiff?.refined)
    #expect(secondRefined != firstRefined)
  }

  /// A hunk whose `removed × added` exceeds the matrix-cell budget takes the COARSE
  /// fallback (removed-then-added, no LCS) instead of allocating the DP matrix — even
  /// when the sides share lines that the LCS would otherwise collapse to context. Uses
  /// a tiny budget so the fallback is exercised without building a giant input.
  @Test("a hunk over the matrix-cell budget takes the coarse fallback")
  func hugeHunkTakesCoarseFallback() {
    // Sides share every line: the full LCS would render all four as `.unchanged`.
    let removed = ["a", "b"]
    let added = ["a", "b"]
    // Budget of 1 < 2×2 = 4 forces the coarse path.
    let capped = DiffRefinement.align(removed: removed, added: added, maxMatrixCells: 1)
    #expect(
      capped == [
        RefinedDiffLine(kind: .removed, text: "a"),
        RefinedDiffLine(kind: .removed, text: "b"),
        RefinedDiffLine(kind: .added, text: "a"),
        RefinedDiffLine(kind: .added, text: "b")
      ])
    // No `.unchanged` collapsing happened — proof the DP was skipped for the fallback.
    #expect(!capped.contains { $0.kind == .unchanged })

    // Under the budget, the same hunk gets the exact LCS refinement (all context).
    let exact = DiffRefinement.align(removed: removed, added: added, maxMatrixCells: 4)
    #expect(exact.allSatisfy { $0.kind == .unchanged })
  }
}
