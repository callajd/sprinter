import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SprinterExecution
import SprinterInspector
import SwiftUI

/// Thin interactive-execution View (CE3.1): renders `ExecutionViewModel` — the projected
/// `transcript`, the inline outstanding prompts, and the lifecycle — and drives input /
/// interrupt / answer through the model. No projection logic here (it lives in the tested
/// view model); the owner (`InspectorView` / `PlannerView`) manages start/stop.
///
/// The transcript is rendered declaratively in the model's current order, with no
/// monotonic assumption (the CE2 carried out-of-order constraint).
struct ExecutionView: View {
  let model: ExecutionViewModel

  @State private var draft = ""
  @State private var actionError: String?
  /// The per-view memo for edit/write diff refinement: the LCS pass is O(removed×added),
  /// so it is computed once per distinct diff content and read from the cache on every
  /// re-render rather than re-run — held here so it survives the View's re-creation.
  @State private var diffCache = RefinedDiffCache()

  var body: some View {
    VStack(spacing: 0) {
      transcript
      Divider()
      composer
    }
    .shellActionErrorAlert($actionError)
  }

  private var transcript: some View {
    ScrollView {
      LazyVStack(alignment: .leading, spacing: 8) {
        ForEach(model.transcript.items) { item in
          TranscriptItemView(item: item, diffCache: diffCache)
        }
        ForEach(model.outstandingRequests) { request in
          promptRow(request)
        }
      }
      .padding()
      .frame(maxWidth: .infinity, alignment: .leading)
    }
  }

  private var composer: some View {
    HStack {
      TextField("Message", text: $draft)
        .textFieldStyle(.roundedBorder)
      Button("Send") { send() }
        .disabled(draft.isEmpty)
      Button("Interrupt") {
        runShellAction(onError: { actionError = $0 }, action: { try await model.interrupt() })
      }
    }
    .padding()
  }

  private func promptRow(_ request: OutstandingUiRequest) -> some View {
    VStack(alignment: .leading, spacing: 4) {
      Text(request.prompt).font(.headline)
      UiRequestAnswerControls(kind: request.kind, options: request.options) { reply in
        answer(request, reply)
      }
    }
    .padding(8)
    .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
  }

  private func answer(_ request: OutstandingUiRequest, _ reply: UiRequestReply) {
    runShellAction(
      onError: { actionError = $0 },
      action: {
        let answer = try makeUiAnswer(forKind: request.kind, reply: reply)
        try await model.answer(requestId: request.id, answer)
      })
  }

  private func send() {
    // Clear the composed text only AFTER the send succeeds: on a transient failure the
    // error alert fires (runShellAction's catch) while `draft` is left intact, so the user
    // can retry without retyping. INV-NOFORCE: runShellAction's do/catch owns the failure.
    let input = ExecutionInput(text: draft, images: nil, mode: .prompt)
    runShellAction(
      onError: { actionError = $0 },
      action: {
        try await model.send(input)
        draft = ""
      })
  }
}

/// Renders one `TranscriptItem` case — a thin, exhaustive switch over the projected item
/// (message, tool call, notice, status, retry, compaction).
private struct TranscriptItemView: View {
  let item: TranscriptItem
  /// The shared refinement memo the tool row reads its diff from, so a large Edit's LCS
  /// pass runs once per distinct content, not once per re-render.
  let diffCache: RefinedDiffCache

  var body: some View {
    switch item {
    case .message(let message):
      messageRow(message)
    case .toolCall(let call):
      toolRow(call)
    case .notice(let notice):
      Text(notice.message)
        .font(.callout)
        .foregroundStyle(.secondary)
    case .status(let status):
      Text("\(status.key): \(status.text)")
        .font(.caption)
        .foregroundStyle(.secondary)
    case .retry(let retry):
      Text("retry attempt \(retry.attempt)")
        .font(.caption)
        .foregroundStyle(.orange)
    case .compaction:
      Text("— context compacted —")
        .font(.caption)
        .foregroundStyle(.secondary)
    }
  }

  /// A tool call: its name, plus — for a recognized edit/write — the intraline-refined
  /// diff so unchanged lines read as context, not churn. The refinement is read through
  /// the shared ``RefinedDiffCache`` so the O(removed×added) LCS pass runs once per
  /// distinct diff content rather than on every re-render.
  @ViewBuilder
  private func toolRow(_ call: TranscriptToolCall) -> some View {
    VStack(alignment: .leading, spacing: 4) {
      Label(call.name.isEmpty ? "tool" : call.name, systemImage: "wrench.and.screwdriver")
        .font(.callout)
      if let refined = diffCache.refined(for: call) {
        TranscriptDiffView(diff: refined)
      }
    }
  }

  private func messageRow(_ message: TranscriptMessage) -> some View {
    VStack(alignment: .leading, spacing: 2) {
      Text(message.role == .user ? "You" : "Agent")
        .font(.caption)
        .foregroundStyle(.secondary)
      Text(message.text)
    }
  }
}
