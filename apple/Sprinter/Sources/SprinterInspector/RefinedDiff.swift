/// One line of a **refined** file diff (CE3.2): the intra-hunk LCS pass over BE4's
/// coarse ``DiffLine``s, so a line present unchanged on both sides renders once as
/// context instead of as a removed+added churn pair.
public struct RefinedDiffLine: Equatable, Sendable {
  /// Whether the line is common to both sides (context), removed by the edit, or
  /// added by it.
  public enum Kind: Equatable, Sendable {
    /// A line present on BOTH sides — shown once as context, not as churn.
    case unchanged
    case removed
    case added
  }

  public let kind: Kind
  public let text: String

  public init(kind: Kind, text: String) {
    self.kind = kind
    self.text = text
  }
}

/// A refined file diff: the file path plus the ordered ``RefinedDiffLine``s after the
/// intraline LCS pass. It is what the SwiftUI diff view renders — unchanged lines as
/// context, only genuine removals/additions as churn.
public struct RefinedDiff: Equatable, Sendable {
  public let filePath: String?
  public let lines: [RefinedDiffLine]

  public init(filePath: String?, lines: [RefinedDiffLine]) {
    self.filePath = filePath
    self.lines = lines
  }
}

extension TranscriptDiff {
  /// This diff refined by the intraline LCS pass: the coarse removed-then-added block
  /// (``DiffLine``s, BE4.1) is realigned so lines common to both sides collapse to a
  /// single ``RefinedDiffLine/Kind/unchanged`` context line rather than a
  /// removed+added pair, leaving only the genuine churn highlighted.
  ///
  /// The coarse projection stays line-level (BE4 emits the whole `old_string` as
  /// removed and the whole `new_string` as added); this **view/refinement layer** does
  /// the LCS realignment, so the projection is unchanged and only the render refines.
  public var refined: RefinedDiff {
    let removed = lines.filter { $0.kind == .removed }.map(\.text)
    let added = lines.filter { $0.kind == .added }.map(\.text)
    return RefinedDiff(
      filePath: filePath, lines: DiffRefinement.align(removed: removed, added: added))
  }
}

/// The intraline LCS realignment (CE3.2). A pure, `Sendable`, offline-testable
/// transform: given the removed and added line sequences of a coarse diff hunk, it
/// computes their longest common subsequence and emits the classic diff script —
/// common lines as context, the rest as removed/added — so unchanged lines are never
/// shown as churn.
public enum DiffRefinement {
  /// Realigns `removed`/`added` line sequences into an ordered diff script via LCS.
  /// Lines in the LCS (present on both sides, in order) become
  /// ``RefinedDiffLine/Kind/unchanged`` context; the rest stay removed/added.
  public static func align(removed: [String], added: [String]) -> [RefinedDiffLine] {
    let rows = removed.count
    let cols = added.count

    // lengths[r][a] = LCS length of removed[r...] and added[a...]. One extra row/col
    // of zeros is the base case (an empty suffix has no common subsequence).
    var lengths = Array(repeating: Array(repeating: 0, count: cols + 1), count: rows + 1)
    if rows > 0 && cols > 0 {
      for removedIndex in stride(from: rows - 1, through: 0, by: -1) {
        for addedIndex in stride(from: cols - 1, through: 0, by: -1) {
          if removed[removedIndex] == added[addedIndex] {
            lengths[removedIndex][addedIndex] = lengths[removedIndex + 1][addedIndex + 1] + 1
          } else {
            lengths[removedIndex][addedIndex] = max(
              lengths[removedIndex + 1][addedIndex], lengths[removedIndex][addedIndex + 1])
          }
        }
      }
    }

    var refined: [RefinedDiffLine] = []
    refined.reserveCapacity(rows + cols)
    var removedIndex = 0
    var addedIndex = 0
    while removedIndex < rows && addedIndex < cols {
      if removed[removedIndex] == added[addedIndex] {
        refined.append(RefinedDiffLine(kind: .unchanged, text: removed[removedIndex]))
        removedIndex += 1
        addedIndex += 1
      } else if lengths[removedIndex + 1][addedIndex] >= lengths[removedIndex][addedIndex + 1] {
        refined.append(RefinedDiffLine(kind: .removed, text: removed[removedIndex]))
        removedIndex += 1
      } else {
        refined.append(RefinedDiffLine(kind: .added, text: added[addedIndex]))
        addedIndex += 1
      }
    }
    while removedIndex < rows {
      refined.append(RefinedDiffLine(kind: .removed, text: removed[removedIndex]))
      removedIndex += 1
    }
    while addedIndex < cols {
      refined.append(RefinedDiffLine(kind: .added, text: added[addedIndex]))
      addedIndex += 1
    }
    return refined
  }
}
