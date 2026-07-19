import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SprinterMissionControl
import SwiftUI

/// Thin Mission Control inbox View (CE3.1): renders `MissionControlInbox.entries` — the
/// outstanding `extension_ui_request`s across sessions — and answers through the model.
/// The round-trip lives in the tested `MissionControlInbox`; this only lays it out and,
/// per the request kind, shows the matching answering widget (confirm buttons / text
/// field / editor / option picker). The kind → wire-answer mapping is the tested
/// `makeUiAnswer(forKind:reply:)` helper (`SprinterAppSupport`).
struct MissionControlInboxView: View {
  let inbox: MissionControlInbox

  @State private var actionError: String?

  var body: some View {
    Group {
      if inbox.entries.isEmpty {
        ContentUnavailableView("No agents waiting", systemImage: "tray")
      } else {
        List(inbox.entries) { entry in
          InboxEntryRow(entry: entry) { reply in
            answer(entry, reply)
          }
        }
      }
    }
    .frame(minWidth: 360, minHeight: 280)
    .shellActionErrorAlert($actionError)
  }

  private func answer(_ entry: InboxEntry, _ reply: UiRequestReply) {
    runShellAction(
      onError: { actionError = $0 },
      action: {
        let answer = try makeUiAnswer(forKind: entry.kind, reply: reply)
        try await inbox.answer(entry, with: answer)
      })
  }
}

/// One inbox row: the prompt, its kind, and the answering widget for that kind. Local
/// `@State` holds the in-progress text / selection so each row edits independently. The
/// row reports the user's action as a ``UiRequestReply``; the container maps it to the
/// correct wire answer.
private struct InboxEntryRow: View {
  let entry: InboxEntry
  let onReply: (UiRequestReply) -> Void

  var body: some View {
    VStack(alignment: .leading, spacing: 6) {
      Text(entry.prompt)
      Text(entry.kind.rawValue)
        .font(.caption)
        .foregroundStyle(.secondary)
      UiRequestAnswerControls(
        kind: entry.kind, options: entry.options, onReply: onReply)
    }
    .padding(.vertical, 4)
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
