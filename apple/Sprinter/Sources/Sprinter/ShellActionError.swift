import SwiftUI

/// A minimal, reusable error surface for the shell's mutating actions — `send` /
/// `interrupt` / `answer` / `materialize`. These previously used `try?`, so a failure was
/// silently dropped and the user saw nothing. Instead each View captures a thrown error
/// into an `@State String?` and attaches ``SwiftUI/View/shellActionErrorAlert(_:)``, which
/// presents the reason.
///
/// This is pure View-layer presentation (the executable target `Sprinter` is the coverage-exempt platform
/// edge); it changes no view-model logic. Where a view model already reflects failure in
/// its own state (e.g. `PlannerViewModel.outcome` → `.rejected`), the View surfaces that
/// state directly and only uses this affordance for the errors a call re-throws.
extension View {
  /// Presents an alert whenever `message` is non-`nil`, clearing it on dismissal.
  func shellActionErrorAlert(_ message: Binding<String?>) -> some View {
    alert(
      "Something went wrong",
      isPresented: Binding(
        get: { message.wrappedValue != nil },
        set: { presented in
          if !presented { message.wrappedValue = nil }
        }
      ),
      presenting: message.wrappedValue
    ) { _ in
      Button("OK", role: .cancel) { message.wrappedValue = nil }
    } message: { reason in
      Text(reason)
    }
  }
}

/// Runs a throwing shell action, reporting any thrown error through `onError` (typically a
/// View's `@State` setter) instead of dropping it. Runs on the main actor so the error
/// state mutation is a main-actor write, matching the `@MainActor` view models it drives.
@MainActor
func runShellAction(
  onError: @escaping (String) -> Void,
  action: @escaping () async throws -> Void
) {
  Task { @MainActor in
    do {
      try await action()
    } catch {
      onError(String(describing: error))
    }
  }
}
