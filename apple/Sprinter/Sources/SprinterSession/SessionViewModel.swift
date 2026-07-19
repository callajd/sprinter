import Observation
import SprinterBackend
import SprinterContract

/// The one reusable **interactive-session view model** (BE3.1 / D9): the
/// `@Observable @MainActor` surface a session view renders, driving ANY session id.
///
/// It is built ON BE1's ``InteractiveSession`` — it does NOT reimplement the
/// session channel. Over the injected ``Backend`` port (INV-PORT) it exposes:
///
/// - ``transcript`` — the live feed projected into a view-facing ``Transcript``
///   (messages/reasoning, tool calls/results, notices/status, retries/compaction,
///   plus the durable `EntryAppended` record reconciled in, D17), via the pure
///   ``TranscriptProjection``. Reading it establishes observation transitively on
///   ``InteractiveSession/events``, so the view re-renders as events arrive.
/// - ``outstandingRequests`` and ``answer(requestId:_:)`` — the inline
///   `extension_ui_request` round-trip; an outstanding prompt surfaces inline and
///   clears once answered.
/// - ``send(_:)`` and ``interrupt()`` — input and abort, resolving success or the
///   mirrored ``ContractError/sessionNotFound(id:)``.
/// - ``lifecycle`` and ``start()``/``stop()`` — an idempotent, single-consumer feed
///   subscription (a fresh feed per (re)start) whose terminal state distinguishes a
///   clean end from a drop.
///
/// Every value it carries is a frozen ``SprinterContract`` DTO (INV-CONTRACT).
@Observable
@MainActor
public final class SessionViewModel {
  /// The session this view model drives.
  public let sessionId: SessionId

  private let session: InteractiveSession
  /// Incremental transcript memo (CE3.2): continues the projection fold from the last
  /// read instead of re-folding the whole feed each time SwiftUI reads ``transcript``,
  /// so repeated re-reads don't cost O(events²). Ignored by observation — it is pure
  /// cache; ``InteractiveSession/events`` is what drives the view.
  @ObservationIgnored private let transcriptMemo = TranscriptProjection.Memo()
  /// Whether ``start()`` has ever been called — distinguishes a never-started feed
  /// (`.idle`) from one that has ended (`.ended`). Observed like any stored property.
  private var hasStarted = false

  public init(backend: any Backend, sessionId: SessionId) {
    self.sessionId = sessionId
    self.session = InteractiveSession(backend: backend, sessionId: sessionId)
  }

  /// The live feed projected into the view-facing transcript. Reading
  /// ``InteractiveSession/events`` establishes observation transitively (so the view
  /// re-renders as events arrive); the incremental ``TranscriptProjection/Memo``
  /// continues the fold from the last read rather than re-folding the whole feed, so
  /// repeated SwiftUI re-reads stay O(events) overall, not O(events²).
  public var transcript: Transcript {
    transcriptMemo.project(session.events)
  }

  /// The `extension_ui_request` prompts raised but not yet answered, in arrival
  /// order — surfaced inline in the session view.
  public var outstandingRequests: [OutstandingUiRequest] {
    session.outstandingRequests
  }

  /// The terminal-distinguishable feed lifecycle: `.idle` before the first
  /// ``start()``, `.live` while subscribed, `.ended` after a clean end or an
  /// intentional ``stop()``, `.dropped` after an abnormal termination.
  public var lifecycle: SessionLifecycle {
    if session.isRunning { return .live }
    if session.terminationError != nil { return .dropped }
    return hasStarted ? .ended : .idle
  }

  /// The error that terminated the feed abnormally (a transport drop or failure
  /// `Exit`); `nil` after a clean end or an intentional ``stop()``.
  public var terminationError: (any Error)? {
    session.terminationError
  }

  /// Subscribes to the live session feed. **Idempotent while already running** (a
  /// second start is a no-op — the single-consumer feed is never cancel-and-respun);
  /// a fresh feed is subscribed on each (re)start after a ``stop()``.
  public func start() {
    hasStarted = true
    session.start()
  }

  /// Cancels the live feed subscription (a clean, intentional stop). Idempotent.
  public func stop() {
    session.stop()
  }

  /// Drives input into the session (a fresh prompt, a mid-turn steer, or a
  /// follow-up); resolves on success or throws the mirrored
  /// ``ContractError/sessionNotFound(id:)`` for an unknown session.
  public func send(_ input: SessionInput) async throws {
    try await session.send(input)
  }

  /// Interrupts the in-flight turn (D9 — every session is interruptible); resolves
  /// on success or throws the mirrored ``ContractError/sessionNotFound(id:)``.
  public func interrupt() async throws {
    try await session.interrupt()
  }

  /// Answers an outstanding `extension_ui_request` by id, completing the round-trip;
  /// the prompt clears from ``outstandingRequests`` once the daemon accepts the
  /// reply. A no-op for an unknown or already-answered request id.
  public func answer(requestId: String, _ answer: UiAnswer) async throws {
    try await session.answer(requestId: requestId, answer)
  }
}
