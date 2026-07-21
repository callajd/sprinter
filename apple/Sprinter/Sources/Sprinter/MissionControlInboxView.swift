import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SprinterMissionControl
import SwiftUI

/// Thin Mission Control inbox View (CE3.1): renders `MissionControlInbox.entries` — the
/// outstanding `extension_ui_request`s across executions — and answers through the model.
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

/// Hosts a `MissionControlInbox` for the shell's inbox sheet: it LIVE-tracks the
/// active executions the board surfaces (CE3.1-F4) so their outstanding prompts
/// aggregate here — following executions that activate/deactivate while the sheet is
/// open, not a point-in-time snapshot — and tears the subscriptions down on dismissal.
/// The tracking/diff logic lives in the tested `MissionControlInbox`; this only wires
/// the board to it.
struct InboxContainer: View {
  let model: AppModel
  @State private var inbox: MissionControlInbox

  init(model: AppModel, backend: any Backend) {
    self.model = model
    _inbox = State(initialValue: MissionControlInbox(backend: backend))
  }

  var body: some View {
    MissionControlInboxView(inbox: inbox)
      .onAppear { inbox.trackActiveExecutions(of: model.board) }
      .onDisappear { inbox.stop() }
  }
}
