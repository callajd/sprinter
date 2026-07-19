import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SprinterMissionControl
import SwiftUI

/// Thin Mission Control inbox View (CE3.1): renders `MissionControlInbox.entries` — the
/// outstanding `extension_ui_request`s across sessions — and answers through the model.
/// The round-trip lives in the tested `MissionControlInbox`; this only lays it out.
struct MissionControlInboxView: View {
  let inbox: MissionControlInbox

  var body: some View {
    Group {
      if inbox.entries.isEmpty {
        ContentUnavailableView("No agents waiting", systemImage: "tray")
      } else {
        List(inbox.entries) { entry in
          entryRow(entry)
        }
      }
    }
    .frame(minWidth: 360, minHeight: 280)
  }

  private func entryRow(_ entry: InboxEntry) -> some View {
    VStack(alignment: .leading, spacing: 6) {
      Text(entry.prompt)
      Text(entry.kind.rawValue)
        .font(.caption)
        .foregroundStyle(.secondary)
      HStack {
        Button("Confirm") { answer(entry, .confirmed(confirmed: true)) }
        Button("Decline") { answer(entry, .confirmed(confirmed: false)) }
        Button("Cancel") { answer(entry, .cancelled) }
      }
    }
    .padding(.vertical, 4)
  }

  private func answer(_ entry: InboxEntry, _ value: UiAnswer) {
    Task { try? await inbox.answer(entry, with: value) }
  }
}

/// Hosts a `MissionControlInbox` for the shell's inbox sheet: it tracks every active
/// session the board currently surfaces (idempotent) so their outstanding prompts
/// aggregate here, and tears the subscriptions down on dismissal.
struct InboxContainer: View {
  let model: AppModel
  @State private var inbox: MissionControlInbox

  init(model: AppModel, backend: any Backend) {
    self.model = model
    _inbox = State(initialValue: MissionControlInbox(backend: backend))
  }

  var body: some View {
    MissionControlInboxView(inbox: inbox)
      .onAppear { trackActiveSessions() }
      .onDisappear { inbox.stop() }
  }

  private func trackActiveSessions() {
    for workstream in model.board.workstreams {
      for epic in workstream.epics {
        for issue in epic.issues {
          if let session = issue.activity?.sessionId {
            inbox.track(session)
          }
        }
      }
    }
  }
}
