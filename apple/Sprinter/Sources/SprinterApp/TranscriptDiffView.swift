import SprinterInspector
import SwiftUI

/// Thin diff View (CE3.2): renders a `RefinedDiff` — the intraline-LCS-refined lines
/// of an edit/write tool call — as a colored, monospaced hunk. Context (unchanged)
/// lines render muted so they read as background, and only genuine removals/additions
/// are highlighted; the LCS refinement itself lives in the tested `SprinterInspector`
/// layer (`TranscriptDiff.refined`), so this View holds no diff logic.
struct TranscriptDiffView: View {
  let diff: RefinedDiff

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      if let filePath = diff.filePath {
        Text(filePath)
          .font(.caption.monospaced())
          .foregroundStyle(.secondary)
          .padding(.bottom, 2)
      }
      ForEach(Array(diff.lines.enumerated()), id: \.offset) { _, line in
        row(line)
      }
    }
    .textSelection(.enabled)
  }

  private func row(_ line: RefinedDiffLine) -> some View {
    Text("\(marker(line.kind)) \(line.text)")
      .font(.caption.monospaced())
      .foregroundStyle(color(line.kind))
      .frame(maxWidth: .infinity, alignment: .leading)
  }

  private func marker(_ kind: RefinedDiffLine.Kind) -> String {
    switch kind {
    case .unchanged: return " "
    case .removed: return "-"
    case .added: return "+"
    }
  }

  private func color(_ kind: RefinedDiffLine.Kind) -> Color {
    switch kind {
    case .unchanged: return .secondary
    case .removed: return .red
    case .added: return .green
    }
  }
}
