import Observation
import SprinterBackend
import SprinterContract

/// The Mission Control **"agent waiting on you" inbox** (BE2.2): the cross-session
/// aggregation of outstanding `extension_ui_request`s, answered through BE1's
/// session channel.
///
/// It is `@Observable @MainActor`: the SwiftUI shell (convergence, not this epic)
/// observes ``entries`` and re-renders when a prompt arrives or is answered, and
/// every mutation happens on the main actor. The inbox owns no transport and no
/// localness — it drives each tracked session through BE1's ``InteractiveSession``
/// over the injected ``Backend`` port (INV-PORT), and every prompt/answer it
/// carries is a mirrored ``SprinterContract`` DTO (INV-CONTRACT); it never
/// reimplements the session channel or the `extension_ui_request` round-trip.
///
/// ``entries`` is a projection over each session's ``InteractiveSession/outstandingRequests``.
/// Because both the inbox and each session are `@Observable`, reading `entries`
/// establishes observation transitively, so a new `UiRequestRaised` surfacing on
/// any tracked feed appears in the inbox and an answered request leaves it — with
/// no incremental bookkeeping here.
@Observable
@MainActor
public final class MissionControlInbox {
  private let backend: any Backend

  /// The sessions whose outstanding prompts this inbox aggregates, keyed by id.
  /// Mutating the map (``track``/``untrack``/``stop``) and each session's
  /// `outstandingRequests` both drive ``entries`` reactively.
  private var sessions: [SessionId: InteractiveSession] = [:]

  public init(backend: any Backend) {
    self.backend = backend
  }

  /// The flat, cross-session inbox: every outstanding prompt, ordered by session id
  /// then arrival order within a session, so the rendering is deterministic.
  public var entries: [InboxEntry] {
    sessions
      .sorted { $0.key.rawValue < $1.key.rawValue }
      .flatMap { sessionId, session in
        session.outstandingRequests.map { InboxEntry(sessionId: sessionId, request: $0) }
      }
  }

  /// Whether any tracked session currently has an agent waiting on the user.
  public var hasWaitingAgents: Bool {
    sessions.values.contains { !$0.outstandingRequests.isEmpty }
  }

  /// Starts aggregating a session's outstanding prompts, subscribing its live feed.
  ///
  /// **Idempotent while already tracked: a second call is a no-op** — it does NOT
  /// cancel-and-respin. BE1's session feed is single-consumer, so re-tracking a
  /// session already being consumed would drop the live subscription; a session is
  /// therefore consumed once for the inbox's lifetime (``InteractiveSession/start()``
  /// is itself idempotent for the same reason).
  public func track(_ sessionId: SessionId) {
    guard sessions[sessionId] == nil else { return }
    let session = InteractiveSession(backend: backend, sessionId: sessionId)
    sessions[sessionId] = session
    session.start()
  }

  /// Stops tracking a session, tearing down its feed subscription and dropping its
  /// entries from the inbox. Idempotent — untracking an unknown session is a no-op.
  public func untrack(_ sessionId: SessionId) {
    sessions.removeValue(forKey: sessionId)?.stop()
  }

  /// Answers an inbox entry, driving `answerUiRequest` through the owning
  /// ``InteractiveSession`` with the neutral ``UiResponse`` keyed to its request id.
  /// The entry clears from ``entries`` once the daemon accepts the reply (BE1 clears
  /// the outstanding request on success) — the `extension_ui_request` round-trip. A
  /// no-op if the entry's session is no longer tracked.
  public func answer(_ entry: InboxEntry, with answer: UiAnswer) async throws {
    try await sessions[entry.sessionId]?.answer(requestId: entry.requestId, answer)
  }

  /// Stops every tracked session's feed and empties the inbox. Idempotent.
  public func stop() {
    for session in sessions.values {
      session.stop()
    }
    sessions.removeAll()
  }
}
