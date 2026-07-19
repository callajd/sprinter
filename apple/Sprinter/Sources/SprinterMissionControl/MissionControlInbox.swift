import Foundation
import Observation
import SprinterBackend
import SprinterContract

/// The Mission Control **"agent waiting on you" inbox** (BE2.2 / CE3.2): the
/// cross-session aggregation of outstanding `extension_ui_request`s, answered through
/// BE1's session channel.
///
/// It is `@Observable @MainActor`: the SwiftUI shell observes ``entries`` and
/// re-renders when a prompt arrives or is answered, and every mutation happens on the
/// main actor. The inbox owns no transport and no localness — it drives each tracked
/// session through BE1's ``InteractiveSession`` over the injected ``Backend`` port
/// (INV-PORT), and every prompt/answer it carries is a mirrored ``SprinterContract``
/// DTO (INV-CONTRACT); it never reimplements the session channel or the
/// `extension_ui_request` round-trip.
///
/// **Wait-time ordering + no-longer-outstanding (CE3.2).** The mirrored
/// `UiRequestRaised` carries no timestamp, so the inbox stamps each request's
/// CLIENT-SIDE arrival time (``arrivals``) the first time it observes it and orders
/// ``entries`` longest-waiting-first off those stamps. When a request stops being
/// outstanding — answered here or withdrawn by the daemon — its stamp is pruned and
/// it leaves ``entries``; ``isOutstanding(_:)`` is the signal a focused view reads to
/// dismiss a prompt that is no longer awaiting the user.
///
/// The projection is kept live by an Observation-driven ``reconcile()``: reading each
/// tracked session's ``InteractiveSession/outstandingRequests`` (and the tracked-set
/// itself) under `withObservationTracking` re-runs the fold whenever a prompt
/// surfaces, is answered, or the tracked set changes — no polling.
@Observable
@MainActor
public final class MissionControlInbox {
  @ObservationIgnored private let backend: any Backend
  /// The client-side clock stamping arrival times — injected so tests are
  /// deterministic; defaults to the wall clock.
  @ObservationIgnored private let now: () -> Date

  /// The sessions whose outstanding prompts this inbox aggregates, keyed by id.
  /// Mutating the map (``track``/``untrack``/``stop``) is observed by ``reconcile()``,
  /// so a newly-tracked session's prompts fold in and an untracked one's fold out.
  private var sessions: [SessionId: InteractiveSession] = [:]

  /// Client-side arrival stamps keyed by composite entry id — the wire carries no
  /// timestamp, so each request is stamped when first seen and PRUNED when it is no
  /// longer outstanding (so a re-raised request is stamped afresh, never inheriting a
  /// stale wait). Internal bookkeeping, not observed directly.
  @ObservationIgnored private var arrivals: [String: Date] = [:]

  /// Whether ``stop()`` has torn the inbox down. Once set, BOTH self-re-arming
  /// Observation loops (``reconcile()`` and ``trackActiveSessions(of:)``) short-circuit,
  /// so a board/feed mutation that fires an already-armed one-shot `onChange` in the
  /// window before the `@State` inbox is released does NOT re-arm the loop or re-track a
  /// session — which would re-`start()` a feed `stop()` just tore down on a dismissed
  /// sheet. It is one-way (a stopped inbox stays stopped), making ``stop()`` idempotent
  /// and its teardown final.
  @ObservationIgnored private var isStopped = false

  /// The flat, cross-session inbox, ordered **longest-waiting-first** by client-side
  /// arrival time (ties broken by the composite entry id, so the rendering is
  /// deterministic). Rebuilt by ``reconcile()`` whenever any tracked session's
  /// outstanding set or the tracked-set changes.
  public private(set) var entries: [InboxEntry] = []

  public init(backend: any Backend, now: @escaping () -> Date = { Date() }) {
    self.backend = backend
    self.now = now
    // Arm the Observation-driven projection: the first `reconcile()` establishes
    // tracking on the (empty) session set; each subsequent `track` chains in the new
    // session's outstanding feed.
    reconcile()
  }

  /// Whether any tracked session currently has an agent waiting on the user.
  public var hasWaitingAgents: Bool { !entries.isEmpty }

  /// Whether the entry with `entryId` is still outstanding — the
  /// **no-longer-outstanding** signal. It flips to `false` the moment the request is
  /// answered here or withdrawn by the daemon, so a view focused on a specific prompt
  /// can dismiss it rather than let the user answer a stale request.
  public func isOutstanding(_ entryId: String) -> Bool {
    entries.contains { $0.id == entryId }
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
    // Reflect the tracked-set change immediately (the async observation chain then
    // re-arms on the new session's feed for its future prompts).
    refreshEntries()
  }

  /// Stops tracking a session, tearing down its feed subscription and dropping its
  /// entries from the inbox. Idempotent — untracking an unknown session is a no-op.
  public func untrack(_ sessionId: SessionId) {
    guard let session = sessions.removeValue(forKey: sessionId) else { return }
    session.stop()
    refreshEntries()
  }

  /// Reconciles the tracked-session set to `activeSessions`: tracks each newly-active
  /// session and untracks ones no longer active. The pure set-diff behind live inbox
  /// tracking (CE3.1-F4) — call it whenever the active set changes so the inbox
  /// follows sessions that activate/deactivate while it is open, rather than a
  /// point-in-time snapshot.
  public func syncTrackedSessions(to activeSessions: Set<SessionId>) {
    for sessionId in activeSessions where sessions[sessionId] == nil {
      track(sessionId)
    }
    for sessionId in Array(sessions.keys) where !activeSessions.contains(sessionId) {
      untrack(sessionId)
    }
  }

  /// LIVE-tracks the sessions `board` surfaces as active (CE3.1-F4): it syncs the
  /// tracked set to the board's active sessions now AND re-syncs whenever the board
  /// changes while the inbox is open (a session activating adds it; one going inactive
  /// drops it) — not the one-shot `onAppear` snapshot CE3.1 shipped. Reading the board
  /// under `withObservationTracking` re-arms the sync on each board update.
  public func trackActiveSessions(of board: MissionControlBoard) {
    // Short-circuit once stopped: neither re-arm the board observation nor re-sync
    // tracking, so an armed one-shot `onChange` firing after `stop()` re-subscribes
    // nothing (see ``isStopped``).
    guard !isStopped else { return }
    let active = withObservationTracking {
      Self.activeSessionIds(in: board.workstreams)
    } onChange: { [weak self, weak board] in
      Task { @MainActor in
        guard let self, let board, !self.isStopped else { return }
        self.trackActiveSessions(of: board)
      }
    }
    syncTrackedSessions(to: active)
  }

  /// The set of sessions with a live agent across the board's tree — each
  /// ``BoardIssue``'s ``IssueActivity/sessionId``, when present. The pure extractor
  /// behind ``trackActiveSessions(of:)``.
  public static func activeSessionIds(in workstreams: [BoardWorkstream]) -> Set<SessionId> {
    var ids: Set<SessionId> = []
    for workstream in workstreams {
      for epic in workstream.epics {
        for issue in epic.issues {
          if let sessionId = issue.activity?.sessionId {
            ids.insert(sessionId)
          }
        }
      }
    }
    return ids
  }

  /// Answers an inbox entry, driving `answerUiRequest` through the owning
  /// ``InteractiveSession`` with the neutral ``UiResponse`` keyed to its request id.
  /// The entry clears from ``entries`` once the daemon accepts the reply (BE1 clears
  /// the outstanding request on success) — the `extension_ui_request` round-trip. A
  /// no-op if the entry's session is no longer tracked.
  public func answer(_ entry: InboxEntry, with answer: UiAnswer) async throws {
    try await sessions[entry.sessionId]?.answer(requestId: entry.requestId, answer)
  }

  /// Stops every tracked session's feed and empties the inbox. Idempotent: sets
  /// ``isStopped`` so both self-re-arming Observation loops short-circuit, then tears
  /// every session down — after it returns no `onChange` can re-arm the projection or
  /// re-subscribe a feed, so a board/feed mutation racing the inbox's release is inert.
  public func stop() {
    isStopped = true
    for session in sessions.values {
      session.stop()
    }
    sessions.removeAll()
    refreshEntries()
  }

  /// The current cross-session outstanding set, sorted by session id — the raw the
  /// fold consumes. Reading it inside ``reconcile()``'s tracked closure establishes
  /// observation on each session's `outstandingRequests` and on `sessions` itself.
  private func currentOutstanding() -> [(sessionId: SessionId, request: OutstandingUiRequest)] {
    sessions
      .sorted { $0.key.rawValue < $1.key.rawValue }
      .flatMap { sessionId, session in
        session.outstandingRequests.map { (sessionId: sessionId, request: $0) }
      }
  }

  /// The single Observation-driven fold: reading the outstanding set under
  /// `withObservationTracking` re-arms `reconcile` on the next feed change — a new
  /// prompt or an answer/withdrawal — with no polling. Armed once at init; each new
  /// session tracked chains in through the observed `sessions` mutation.
  private func reconcile() {
    // Short-circuit once stopped: do NOT re-arm the outstanding-feed observation, so an
    // armed one-shot `onChange` firing after `stop()` re-subscribes nothing and the
    // observation chain dies rather than re-establishing tracking (see ``isStopped``).
    guard !isStopped else { return }
    let raw = withObservationTracking {
      currentOutstanding()
    } onChange: { [weak self] in
      Task { @MainActor in self?.reconcile() }
    }
    applyOutstanding(raw)
  }

  /// Refolds ``entries`` from the current outstanding set WITHOUT re-arming
  /// observation — the synchronous update for a tracked-set change (``track`` /
  /// ``untrack`` / ``stop``), so `entries` reflects it immediately while the single
  /// observation chain keeps handling live feed changes.
  private func refreshEntries() {
    applyOutstanding(currentOutstanding())
  }

  /// Rebuilds ``entries`` and ``arrivals`` from the current outstanding set: each
  /// request keeps its prior arrival stamp or is stamped `now()` on first sight, and
  /// stamps for requests no longer present are dropped (the no-longer-outstanding
  /// prune). Entries are ordered longest-waiting-first, ties by composite id.
  private func applyOutstanding(_ raw: [(sessionId: SessionId, request: OutstandingUiRequest)]) {
    var refreshed: [String: Date] = [:]
    var built: [InboxEntry] = []
    built.reserveCapacity(raw.count)
    for pair in raw {
      let key = InboxEntry.compositeId(sessionId: pair.sessionId, requestId: pair.request.id)
      let arrival = arrivals[key] ?? now()
      refreshed[key] = arrival
      built.append(
        InboxEntry(sessionId: pair.sessionId, request: pair.request, waitingSince: arrival))
    }
    arrivals = refreshed
    entries = built.sorted { lhs, rhs in
      lhs.waitingSince == rhs.waitingSince ? lhs.id < rhs.id : lhs.waitingSince < rhs.waitingSince
    }
  }
}
