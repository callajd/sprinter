import SprinterContract

/// The user's action from the widget the shell shows for a raised UI request, before it
/// is mapped to the wire ``UiAnswer``. Keeping this separate from ``UiAnswer`` lets the
/// (coverage-exempt) SwiftUI View describe only *what the user did* — pick, type, edit,
/// confirm, or dismiss — while the tested ``makeUiAnswer(forKind:reply:)`` mapping decides
/// the correct wire variant per request kind.
public enum UiRequestReply: Equatable, Sendable {
  /// A confirm / decline decision from a `confirm` prompt's buttons.
  case decided(Bool)
  /// Free text the user supplied — an `input` field's text, an `editor`'s edited body, or
  /// a `select` prompt's chosen option.
  case entered(String)
  /// The user dismissed the request without answering it (valid for any kind).
  case dismissed
}

/// A raised request's kind and the collected ``UiRequestReply`` disagree — e.g. a
/// `confirm` decision was reported for an `input` prompt, or free text for a `confirm`.
/// It is surfaced (never force-unwrapped / silently coerced) so the shell can report the
/// programmer error rather than send a semantically wrong answer to the agent.
public struct UiAnswerMismatch: Error, Equatable {
  /// The request kind whose reply did not match.
  public let kind: UiRequestKind

  public init(kind: UiRequestKind) {
    self.kind = kind
  }
}

/// Map a raised request's ``UiRequestKind`` and the user's ``UiRequestReply`` to the wire
/// ``UiAnswer`` the daemon forwards to the agent.
///
/// This lives here, in a covered target, because the mapping is load-bearing: the daemon
/// does **not** re-derive the answer variant from the request kind — its
/// `encodeUiResponse` (`@sprinter/runner` `execution-handle.ts`) encodes whatever variant it
/// receives verbatim into Pi's `extension_ui_response`. So sending the correct variant per
/// kind is entirely the client's responsibility:
///
/// - `confirm` expects a `Confirmed` boolean (or `Cancelled`),
/// - `input` / `editor` / `select` each expect a `Value` — the entered text, the edited
///   body, or the chosen option respectively (or `Cancelled`).
///
/// The pre-fix Views always sent `Confirmed` / `Cancelled` regardless of kind, so an
/// `input` / `editor` / `select` prompt received a semantically wrong answer.
public func makeUiAnswer(forKind kind: UiRequestKind, reply: UiRequestReply) throws -> UiAnswer {
  switch (kind, reply) {
  case (_, .dismissed):
    return .cancelled
  case (.confirm, .decided(let decided)):
    return .confirmed(confirmed: decided)
  case (.input, .entered(let text)), (.editor, .entered(let text)), (.select, .entered(let text)):
    return .value(value: text)
  case (.confirm, .entered), (.input, .decided), (.editor, .decided), (.select, .decided):
    throw UiAnswerMismatch(kind: kind)
  }
}
