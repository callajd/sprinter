import Foundation
import SprinterBackend
import SprinterContract

/// One "agent waiting on you" item in the Mission Control inbox (BE2.2): a single
/// outstanding `extension_ui_request` surfaced across sessions.
///
/// It pairs the ``SessionId`` of the session that raised the prompt with the
/// underlying ``OutstandingUiRequest`` (BE1's projected `UiRequestRaised`, keyed by
/// the request `id` the answer must echo), so a view can render *which* agent is
/// waiting and *what* it is asking — the prompt, its ``UiRequestKind``, and any
/// `select`/`editor` options. The mirrored ``SprinterContract`` DTOs are carried
/// unchanged (INV-CONTRACT); nothing here redefines a wire type.
///
/// It also carries ``waitingSince`` — the CLIENT-SIDE arrival timestamp the inbox
/// stamps when the request first appears (CE3.2). The mirrored `UiRequestRaised`
/// carries no wire timestamp, so the inbox records first-seen time itself and orders
/// the inbox longest-waiting-first off it.
public struct InboxEntry: Identifiable, Equatable, Sendable {
  /// The session whose agent raised this request — the answer is routed back to it.
  public let sessionId: SessionId
  /// The outstanding request itself (id / kind / prompt / options), as BE1 projects
  /// it from the session feed.
  public let request: OutstandingUiRequest
  /// When the inbox first observed this request — the client-side arrival stamp used
  /// to order the inbox by wait time (the wire carries no timestamp). Stable across
  /// re-projections while the request stays outstanding; a request that resolves and
  /// is later re-raised is stamped afresh.
  public let waitingSince: Date

  /// A stable identity unique across sessions. Request ids are unique only within a
  /// session, so the inbox key composes the session id with the request id (joined
  /// by an ASCII unit separator that cannot occur in either token's rendered text).
  public var id: String { Self.compositeId(sessionId: sessionId, requestId: request.id) }

  /// The stable cross-session key for a `(session, request)` pair — the inbox's
  /// arrival-tracking key and each entry's identity.
  public static func compositeId(sessionId: SessionId, requestId: String) -> String {
    "\(sessionId.rawValue)\u{001F}\(requestId)"
  }

  /// The request id the answer must echo (BE1's outstanding-request → answer
  /// correlation).
  public var requestId: String { request.id }
  /// What the agent is asking for (`select` / `confirm` / `input` / `editor`).
  public var kind: UiRequestKind { request.kind }
  /// The prompt to render.
  public var prompt: String { request.prompt }
  /// The choices to offer, for a `select`/`editor` prompt; `nil` otherwise.
  public var options: [String]? { request.options }

  public init(sessionId: SessionId, request: OutstandingUiRequest, waitingSince: Date) {
    self.sessionId = sessionId
    self.request = request
    self.waitingSince = waitingSince
  }
}
