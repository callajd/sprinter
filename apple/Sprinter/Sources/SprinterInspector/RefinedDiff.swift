import SprinterSession

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
  ///
  /// This runs the O(removed×added) LCS DP, so it must NOT be called on every SwiftUI
  /// re-render of a large Edit — read it through a ``RefinedDiffCache`` so the DP runs
  /// once per distinct diff content, not once per render.
  public var refined: RefinedDiff {
    let removed = lines.filter { $0.kind == .removed }.map(\.text)
    let added = lines.filter { $0.kind == .added }.map(\.text)
    return RefinedDiff(
      filePath: filePath, lines: DiffRefinement.align(removed: removed, added: added))
  }
}

/// A per-view memo for ``TranscriptDiff/refined`` (CE3.2): the intraline LCS pass is
/// O(removed×added) and allocates an (rows+1)×(cols+1) matrix, so recomputing it on
/// every SwiftUI re-render of a large Edit reintroduces exactly the quadratic-on-re-read
/// cost the transcript memo (deliverable 4) exists to kill. This caches each tool call's
/// refined diff keyed on the call's **stable identity** — its id plus the coarse diff
/// content — recomputing only when the content actually changes, so the diff view reads
/// a cached value across renders and the DP runs once per distinct edit.
///
/// Not `Sendable` / not thread-safe by design: a view holds one instance and drives it
/// on the main actor (its own render loop), the same single-owner contract
/// ``TranscriptProjection.Memo`` has.
public final class RefinedDiffCache {
  /// One cached refinement: the coarse diff it was computed from (the invalidation key's
  /// content half) and the refined result.
  private struct Entry {
    let diff: TranscriptDiff
    let refined: RefinedDiff
  }

  private var entries: [String: Entry] = [:]

  public init() {}

  /// The refined diff for `call`, or `nil` when the call is not a recognized edit/write.
  ///
  /// Returns the cached refinement when the call's id AND its coarse diff content are
  /// unchanged since the last lookup for that id; otherwise it recomputes the LCS
  /// refinement (capped — see ``DiffRefinement/align(removed:added:maxMatrixCells:)``)
  /// and caches it. Keying on the coarse diff content (not object identity) means a
  /// re-issued id whose `old_string`/`new_string` changed re-refines, while a pure
  /// re-render of unchanged content is served from the cache.
  public func refined(for call: TranscriptToolCall) -> RefinedDiff? {
    guard let diff = call.fileDiff else { return nil }
    if let cached = entries[call.id], cached.diff == diff {
      return cached.refined
    }
    let refined = diff.refined
    entries[call.id] = Entry(diff: diff, refined: refined)
    return refined
  }
}

/// The intraline LCS realignment (CE3.2). A pure, `Sendable`, offline-testable
/// transform: given the removed and added line sequences of a coarse diff hunk, it
/// computes their longest common subsequence and emits the classic diff script —
/// common lines as context, the rest as removed/added — so unchanged lines are never
/// shown as churn.
public enum DiffRefinement {
  /// The LCS DP's matrix-cell budget: a hunk whose `removed.count × added.count`
  /// exceeds this skips the DP and renders **coarsely** (removed-then-added, no context
  /// collapsing) rather than allocating the giant (rows+1)×(cols+1) matrix. This bounds
  /// the worst case so a pathologically large Edit degrades gracefully to the BE4 coarse
  /// rendering instead of quadratic time/space. One million cells is far above any
  /// realistic edit hunk yet a fixed, small ceiling.
  public static let defaultMaxMatrixCells = 1_000_000

  /// Realigns `removed`/`added` line sequences into an ordered diff script via LCS.
  /// Lines in the LCS (present on both sides, in order) become
  /// ``RefinedDiffLine/Kind/unchanged`` context; the rest stay removed/added.
  ///
  /// Bounded worst case: when `removed.count × added.count` exceeds `maxMatrixCells`
  /// (or overflows), the LCS DP is skipped and the coarse removed-then-added script is
  /// returned — no matrix is allocated. Below the cap the exact LCS refinement runs.
  public static func align(
    removed: [String],
    added: [String],
    maxMatrixCells: Int = defaultMaxMatrixCells
  ) -> [RefinedDiffLine] {
    let rows = removed.count
    let cols = added.count

    // Safety cap: skip the DP for a hunk whose matrix would exceed the cell budget (or
    // whose product overflows `Int`), falling back to the coarse rendering so the worst
    // case is bounded and no giant matrix is allocated.
    let product = rows.multipliedReportingOverflow(by: cols)
    if product.overflow || product.partialValue > maxMatrixCells {
      return coarse(removed: removed, added: added)
    }

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

  /// The coarse (BE4) diff script: every removed line then every added line, with NO
  /// context collapsing. The bounded fallback for a hunk too large for the LCS DP — it
  /// is O(rows+cols) and allocates no matrix, so an enormous Edit still renders (just
  /// without the intraline refinement) instead of blowing time/space.
  static func coarse(removed: [String], added: [String]) -> [RefinedDiffLine] {
    removed.map { RefinedDiffLine(kind: .removed, text: $0) }
      + added.map { RefinedDiffLine(kind: .added, text: $0) }
  }
}
