import SprinterAppSupport
import SprinterBackend
import SprinterContract
import SwiftUI

/// The app's root shell (CE3.1): reflects the connection lifecycle and, once connected,
/// composes the feature Views into a running window — the board in the sidebar and the
/// selected session's inspector (transcript ↔ PR) in the detail, with the inbox and
/// planner reachable from the toolbar. Thin: it wires already-tested view models; it
/// holds no feature logic.
struct RootView: View {
  let model: AppModel

  var body: some View {
    Group {
      if let backend = model.backend {
        ConnectedShell(model: model, backend: backend)
      } else if case .failed(let reason) = model.connectionState {
        failureView(reason)
      } else {
        connectingView
      }
    }
  }

  private var connectingView: some View {
    ContentUnavailableView {
      Label("Connecting to the daemon", systemImage: "network")
    } description: {
      ProgressView()
    }
  }

  private func failureView(_ reason: String) -> some View {
    ContentUnavailableView(
      "Cannot reach the daemon",
      systemImage: "exclamationmark.triangle",
      description: Text(reason))
  }
}

/// The connected shell: the board + a selection-driven inspector detail, with the inbox
/// and planner presented as sheets. Selection state and sheet presentation are the only
/// UI state it owns; every feature surface is a thin View over an existing view model.
private struct ConnectedShell: View {
  let model: AppModel
  let backend: any Backend

  @State private var selectedSession: SessionId?
  @State private var showInbox = false
  @State private var showPlanner = false

  var body: some View {
    NavigationSplitView {
      MissionControlBoardView(board: model.board) { session in
        selectedSession = session
      }
      .navigationTitle("Mission Control")
    } detail: {
      detail
    }
    .toolbar { toolbarContent }
    .sheet(isPresented: $showInbox) {
      InboxContainer(model: model, backend: backend)
    }
    .sheet(isPresented: $showPlanner) {
      PlannerContainer(backend: backend)
    }
  }

  @ViewBuilder
  private var detail: some View {
    if let session = selectedSession {
      InspectorContainer(model: model, backend: backend, sessionId: session)
        .id(session.rawValue)
    } else {
      ContentUnavailableView(
        "Select an active issue",
        systemImage: "sidebar.squares.left")
    }
  }

  @ToolbarContentBuilder
  private var toolbarContent: some ToolbarContent {
    ToolbarItemGroup {
      Button {
        showInbox = true
      } label: {
        Label("Inbox", systemImage: "tray")
      }
      Button {
        showPlanner = true
      } label: {
        Label("New plan", systemImage: "plus")
      }
    }
  }
}
