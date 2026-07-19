import SprinterBackend
import SprinterContract
import SprinterSession
import SwiftUI

/// Thin interactive-session View (CE3.1): renders `SessionViewModel` — the projected
/// `transcript`, the inline outstanding prompts, and the lifecycle — and drives input /
/// interrupt / answer through the model. No projection logic here (it lives in the tested
/// view model); the owner (`InspectorView` / `PlannerView`) manages start/stop.
///
/// The transcript is rendered declaratively in the model's current order, with no
/// monotonic assumption (the CE2 carried out-of-order constraint).
struct SessionView: View {
  let model: SessionViewModel

  @State private var draft = ""

  var body: some View {
    VStack(spacing: 0) {
      transcript
      Divider()
      composer
    }
  }

  private var transcript: some View {
    ScrollView {
      LazyVStack(alignment: .leading, spacing: 8) {
        ForEach(model.transcript.items) { item in
          TranscriptItemView(item: item)
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
        Task { try? await model.interrupt() }
      }
    }
    .padding()
  }

  private func promptRow(_ request: OutstandingUiRequest) -> some View {
    VStack(alignment: .leading, spacing: 4) {
      Text(request.prompt).font(.headline)
      Button("Confirm") {
        Task { try? await model.answer(requestId: request.id, .confirmed(confirmed: true)) }
      }
    }
    .padding(8)
    .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
  }

  private func send() {
    let text = draft
    draft = ""
    let input = SessionInput(text: text, images: nil, mode: .prompt)
    Task { try? await model.send(input) }
  }
}

/// Renders one `TranscriptItem` case — a thin, exhaustive switch over the projected item
/// (message, tool call, notice, status, retry, compaction).
private struct TranscriptItemView: View {
  let item: TranscriptItem

  var body: some View {
    switch item {
    case .message(let message):
      messageRow(message)
    case .toolCall(let call):
      Label(call.name.isEmpty ? "tool" : call.name, systemImage: "wrench.and.screwdriver")
        .font(.callout)
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

  private func messageRow(_ message: TranscriptMessage) -> some View {
    VStack(alignment: .leading, spacing: 2) {
      Text(message.role == .user ? "You" : "Agent")
        .font(.caption)
        .foregroundStyle(.secondary)
      Text(message.text)
    }
  }
}
