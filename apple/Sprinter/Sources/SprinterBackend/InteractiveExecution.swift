import Observation
import SprinterContract

/// An outstanding UI request awaiting an answer — projected from a
/// `UiRequestRaised` on the execution feed and keyed by the request `id` the
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

/// The one reusable interactive-execution surface (D9): a view-facing `@Observable`
/// model over the ``Backend`` execution channel.
///
/// It consumes the live ``ExecutionEvent`` feed for an execution, records the
/// transcript of events, and tracks outstanding `UiRequestRaised` prompts by id
/// so a consumer can render each prompt and reply through ``answer(requestId:_:)``
/// — the `extension_ui_request` round-trip. It also drives input outward via
/// ``send(_:)`` and ``interrupt()``. Because it depends only on the ``Backend``
/// port, it is identical for a local or remote daemon (INV-PORT), and it carries
/// only the frozen `SprinterContract` DTOs (INV-CONTRACT).
@MainActor
@Observable
public final class InteractiveExecution {
  /// The execution feed in arrival order (the reactive transcript source).
  public private(set) var events: [ExecutionEvent] = []
  /// UI requests raised but not yet answered, in the order they surfaced.
  public private(set) var outstandingRequests: [OutstandingUiRequest] = []
  /// Whether the live feed is currently subscribed.
  public private(set) var isRunning = false
  /// The error that terminated the feed, when it ended abnormally — a transport
  /// drop or a failure `Exit`. `nil` after a clean end (the execution ended) or an
  /// intentional ``stop()``, so a view can distinguish "ended" from "dropped".
  public private(set) var terminationError: (any Error)?

  private let backend: any Backend
  private let executionId: ExecutionId
  private var feed: Task<Void, Never>?
  /// Bumped on each ``start()``; the feed task's terminal cleanup runs only if its
  /// generation is still current, so a rapid `stop()`+`start()` (whose prior,
  /// cancelled task finishes its cleanup AFTER the new task is installed) cannot
  /// clobber the new task's `feed`/`isRunning`.
  private var generation = 0

  public init(backend: any Backend, executionId: ExecutionId) {
    self.backend = backend
    self.executionId = executionId
  }

  /// Subscribes to the live execution feed. Idempotent while already running.
  public func start() {
    guard feed == nil else { return }
    isRunning = true
    terminationError = nil
    generation += 1
    let generation = generation
    feed = Task { @MainActor [weak self] in
      guard let self else { return }
      do {
        for try await event in self.backend.executionEvents(executionId: self.executionId) {
          self.ingest(event)
        }
      } catch {
        // A transport drop or a failure `Exit` — surface it (unless this is an
        // intentional `stop()` cancellation) so a consumer can tell "dropped" from
        // a clean "ended". A SUPERSEDED task (a newer start() bumped the
        // generation) records nothing — its outcome is stale.
        if !Task.isCancelled && self.generation == generation {
          self.terminationError = error
        }
      }
      // Only the CURRENT feed task tears down the running state; a superseded task
      // leaves the new task's `feed`/`isRunning` intact.
      guard self.generation == generation else { return }
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

  /// Drives input into the execution (prompt / steer / follow-up).
  public func send(_ input: ExecutionInput) async throws {
    try await backend.executionSend(executionId: executionId, input: input)
  }

  /// Interrupts the running execution (D9).
  public func interrupt() async throws {
    try await backend.interrupt(executionId: executionId)
  }

  /// Answers an outstanding UI request by id and clears it once the daemon
  /// accepts the reply — the `extension_ui_request` round-trip.
  public func answer(requestId: String, _ answer: UiAnswer) async throws {
    // Only answer an actually-outstanding request — never send for an unknown or
    // already-answered id (which would let a caller double-answer).
    guard outstandingRequests.contains(where: { $0.id == requestId }) else { return }
    let response = UiResponse(requestId: requestId, answer: answer)
    try await backend.answerUiRequest(executionId: executionId, response: response)
    outstandingRequests.removeAll { $0.id == requestId }
  }

  private func ingest(_ event: ExecutionEvent) {
    events.append(event)
    if case .uiRequestRaised(let id, let kind, let prompt, let options) = event {
      outstandingRequests.append(
        OutstandingUiRequest(id: id, kind: kind, prompt: prompt, options: options))
    }
  }
}
