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
    // SINGLE-WINDOW by construction (CE4.2 cold-review FIX): the macOS `Window` scene
    // (macOS 13+; deployment target is macOS 14, see Package.swift) is a UNIQUE window —
    // unlike `WindowGroup` it neither allows Cmd-N / File▸New Window nor generates a
    // "New Window" menu item. Mission Control is a single dashboard, so exactly one scene
    // exists over the whole run and the scene lifecycle == process lifetime. That makes
    // the `stop()`-on-teardown wiring below correct: teardown fires ONCE at app exit, so
    // it can never cancel the shared `AppModel`/backend out from under a still-live second
    // window (the multi-window regression `WindowGroup` allowed).
    Window("Sprinter", id: "main") {
      RootView(model: model)
        .frame(minWidth: 900, minHeight: 600)
        .task {
          // Own the model's lifetime for the (single) scene: start it, then hold the task
          // alive until SwiftUI tears the scene down (which CANCELS this task) and release
          // the load-bearing transport thread + fd via `stop()` (CE4.2 scene-lifecycle
          // teardown — the CE4 lifecycle watch-item). Because the scene is unique this runs
          // exactly once, at process exit — never per-window.
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
