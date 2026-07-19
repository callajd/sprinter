import SprinterAppSupport
import SprinterContract
import SwiftUI

/// The per-kind answering widget for a raised UI request, shared by the inbox rows and the
/// in-session prompt rows. It shows the control matching the request `kind` — confirm
/// buttons, a single-line text field, a multi-line editor, or an option picker — and
/// reports the user's action as a ``UiRequestReply``. The owner maps that reply to the
/// correct wire ``UiAnswer`` via the tested `makeUiAnswer(forKind:reply:)` helper; this
/// View holds no mapping logic (the platform edge, INV-COV exempt).
struct UiRequestAnswerControls: View {
  let kind: UiRequestKind
  let options: [String]?
  let onReply: (UiRequestReply) -> Void

  @State private var draft = ""

  var body: some View {
    switch kind {
    case .confirm:
      confirmControls
    case .input:
      inputControls
    case .editor:
      editorControls
    case .select:
      selectControls
    }
  }

  private var confirmControls: some View {
    HStack {
      Button("Confirm") { onReply(.decided(true)) }
      Button("Decline") { onReply(.decided(false)) }
      Button("Cancel") { onReply(.dismissed) }
    }
  }

  private var inputControls: some View {
    HStack {
      TextField("Answer", text: $draft)
        .textFieldStyle(.roundedBorder)
      submitButton
      cancelButton
    }
  }

  private var editorControls: some View {
    VStack(alignment: .leading, spacing: 6) {
      TextEditor(text: $draft)
        .frame(minHeight: 80)
        .border(.quaternary)
      HStack {
        submitButton
        cancelButton
      }
    }
  }

  private var selectControls: some View {
    VStack(alignment: .leading, spacing: 4) {
      ForEach(options ?? [], id: \.self) { option in
        Button(option) { onReply(.entered(option)) }
      }
      cancelButton
    }
  }

  private var submitButton: some View {
    Button("Submit") { onReply(.entered(draft)) }
      .disabled(draft.isEmpty)
  }

  private var cancelButton: some View {
    Button("Cancel") { onReply(.dismissed) }
  }
}
