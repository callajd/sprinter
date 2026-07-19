import SprinterContract
import SprinterMissionControl
import SwiftUI

/// Thin Mission Control board View (CE3.1): renders `MissionControlBoard.workstreams` —
/// the already-projected `Workstream ⊃ Epic ⊃ Issue` tree — as a list and forwards a
/// session selection. All projection logic lives in the tested view model.
///
/// It reads the tree declaratively and makes NO monotonic assumption about status: the
/// live tail can momentarily deliver a stale value that self-corrects (the CE2 carried
/// out-of-order constraint), so the View simply re-renders whatever the model holds now.
struct MissionControlBoardView: View {
  let board: MissionControlBoard
  let onSelectSession: (SessionId) -> Void

  var body: some View {
    List {
      ForEach(board.workstreams) { workstream in
        Section(workstream.name) {
          ForEach(workstream.epics) { epic in
            epicRow(epic)
          }
        }
      }
    }
  }

  private func epicRow(_ epic: BoardEpic) -> some View {
    DisclosureGroup {
      ForEach(epic.issues) { issue in
        issueRow(issue)
      }
    } label: {
      statusRow(epic.name, status: epic.status)
    }
  }

  @ViewBuilder
  private func issueRow(_ issue: BoardIssue) -> some View {
    let title = "#\(issue.number) \(issue.title)"
    if let session = issue.activity?.sessionId {
      Button {
        onSelectSession(session)
      } label: {
        statusRow(title, status: issue.status)
      }
      .buttonStyle(.plain)
    } else {
      statusRow(title, status: issue.status)
    }
  }

  private func statusRow(_ text: String, status: BoardStatus) -> some View {
    HStack {
      Text(text)
      Spacer()
      Text(status.rawValue)
        .font(.caption)
        .foregroundStyle(.secondary)
    }
  }
}
