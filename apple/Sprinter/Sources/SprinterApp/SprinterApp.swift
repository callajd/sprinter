import Foundation
import SprinterAppSupport
import SwiftUI

#if os(macOS)
  import AppKit
#endif

/// The running Sprinter app (CE3.1): the `@main` SwiftUI `App` that launches the client
/// against the live daemon. This target is the ONE platform edge (D10 / INV-PORT) — the
/// `#if os(...)` shell and any AppKit/UIKit glue live HERE ONLY; the feature libraries it
/// renders stay platform-neutral.
///
/// The daemon endpoint is resolved from the environment (`SPRINTER_SOCKET`, a local Unix
/// socket) and wired live: `DaemonConnection` → `BackendConnector` → `DaemonTransports`
/// (`UnixSocketTransport`) → `RpcBackend` → `WorkGraphResync`, all behind `AppModel`.
@main
@MainActor
struct SprinterApp: App {
  #if os(macOS)
    @NSApplicationDelegateAdaptor(MacLaunchDelegate.self) private var launchDelegate
  #endif

  @State private var model: AppModel

  init() {
    let endpoint = DaemonEndpointResolver.resolve(
      environment: ProcessInfo.processInfo.environment)
    _model = State(initialValue: AppModel(daemon: DaemonConnection(endpoint: endpoint)))
  }

  var body: some Scene {
    WindowGroup {
      RootView(model: model)
        .frame(minWidth: 900, minHeight: 600)
        .task {
          // Own the model's lifetime for the scene: start it, then hold the task alive
          // until SwiftUI tears the scene down (which CANCELS this task) and release the
          // load-bearing transport thread + fd via `stop()` (CE4.2 scene-lifecycle
          // teardown — the CE4 lifecycle watch-item), rather than only at process exit.
          model.start()
          while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(3600))
          }
          model.stop()
        }
    }
    #if os(macOS)
      .windowStyle(.titleBar)
      .windowToolbarStyle(.unified)
    #endif
  }
}

#if os(macOS)
  /// macOS launch glue (INV-PORT: AppKit stays in the executable, the one platform edge).
  /// A `swift run` launch starts as an accessory process, so promote it to a regular
  /// foreground app and bring its window forward — otherwise the window never takes focus.
  final class MacLaunchDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
      NSApp.setActivationPolicy(.regular)
      NSApp.activate(ignoringOtherApps: true)
    }
  }
#endif
