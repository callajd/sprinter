import Observation
import SprinterContract

/// An outstanding UI request awaiting an answer — projected from a
/// `UiRequestRaised` on the session feed and keyed by the request `id` the
/// answer must echo (the outstanding-request → answer correlation).
public struct OutstandingUiRequest: Identifiable, Equatable, Sendable {
  public let id: String
  public let kind: UiRequestKind
  public let prompt: String
  public let options: [String]?

  public init(id: String, kind: UiRequestKind, prompt: String, options: [String]?) {
    self.id = id
    self.kind = kind
    self.prompt = prompt
    self.options = options
  }
}

/// The one reusable interactive-session surface (D9): a view-facing `@Observable`
/// model over the ``Backend`` session channel.
///
/// It consumes the live ``SessionEvent`` feed for a session, records the
/// transcript of events, and tracks outstanding `UiRequestRaised` prompts by id
/// so a consumer can render each prompt and reply through ``answer(requestId:_:)``
/// — the `extension_ui_request` round-trip. It also drives input outward via
/// ``send(_:)`` and ``interrupt()``. Because it depends only on the ``Backend``
/// port, it is identical for a local or remote daemon (INV-PORT), and it carries
/// only the frozen `SprinterContract` DTOs (INV-CONTRACT).
@MainActor
@Observable
public final class InteractiveSession {
  /// The session feed in arrival order (the reactive transcript source).
  public private(set) var events: [SessionEvent] = []
  /// UI requests raised but not yet answered, in the order they surfaced.
  public private(set) var outstandingRequests: [OutstandingUiRequest] = []
  /// Whether the live feed is currently subscribed.
  public private(set) var isRunning = false

  private let backend: any Backend
  private let sessionId: SessionId
  private var feed: Task<Void, Never>?

  public init(backend: any Backend, sessionId: SessionId) {
    self.backend = backend
    self.sessionId = sessionId
  }

  /// Subscribes to the live session feed. Idempotent while already running.
  public func start() {
    guard feed == nil else { return }
    isRunning = true
    feed = Task { @MainActor [weak self] in
      guard let self else { return }
      do {
        for try await event in self.backend.sessionEvents(sessionId: self.sessionId) {
          self.ingest(event)
        }
      } catch {
        // The feed ended (a drop or a failure exit); reflected by `isRunning`.
      }
      self.isRunning = false
      self.feed = nil
    }
  }

  /// Cancels the live feed subscription.
  public func stop() {
    feed?.cancel()
    feed = nil
    isRunning = false
  }

  /// Drives input into the session (prompt / steer / follow-up).
  public func send(_ input: SessionInput) async throws {
    try await backend.sessionSend(sessionId: sessionId, input: input)
  }

  /// Interrupts the running session (D9).
  public func interrupt() async throws {
    try await backend.interrupt(sessionId: sessionId)
  }

  /// Answers an outstanding UI request by id and clears it once the daemon
  /// accepts the reply — the `extension_ui_request` round-trip.
  public func answer(requestId: String, _ answer: UiAnswer) async throws {
    let response = UiResponse(requestId: requestId, answer: answer)
    try await backend.answerUiRequest(sessionId: sessionId, response: response)
    outstandingRequests.removeAll { $0.id == requestId }
  }

  private func ingest(_ event: SessionEvent) {
    events.append(event)
    if case .uiRequestRaised(let id, let kind, let prompt, let options) = event {
      outstandingRequests.append(
        OutstandingUiRequest(id: id, kind: kind, prompt: prompt, options: options))
    }
  }
}
