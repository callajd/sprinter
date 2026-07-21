import SprinterContract
import SprinterExecution

/// One line of a rendered file diff — an addition or a removal.
///
/// The inspector renders a tool call's edit as a sequence of added/removed lines;
/// this is the neutral, `Sendable`, view-agnostic unit the SwiftUI diff view
/// (convergence, not this epic) lays out.
public struct DiffLine: Equatable, Sendable {
  /// Whether the line was added by the edit or removed by it.
  public enum Kind: Equatable, Sendable {
    case added
    case removed
  }

  public let kind: Kind
  public let text: String

  public init(kind: Kind, text: String) {
    self.kind = kind
    self.text = text
  }
}

/// A file edit rendered from an edit/write tool call's ``JSONValue`` payload
/// (BE4.1): the file path the edit targets plus the ordered removed/added lines.
///
/// "Diffs" are not a first-class transcript item — they arrive as the `input` of an
/// edit/write tool call (``TranscriptToolCall/input``). This is the pure, testable
/// transform that recognizes the relevant tool call and renders its diff from the
/// opaque payload; the transcript projection itself stays untouched (the diff is
/// derived from the existing tool-call item, not a parallel renderer).
public struct TranscriptDiff: Equatable, Sendable {
  /// The path the edit targets, when the payload names one.
  public let filePath: String?
  /// The removed-then-added lines the edit applies.
  public let lines: [DiffLine]

  public init(filePath: String?, lines: [DiffLine]) {
    self.filePath = filePath
    self.lines = lines
  }
}

extension TranscriptToolCall {
  /// The file diff this tool call renders, or `nil` when the call is not a
  /// recognized edit/write (so the transcript renders it as a plain tool call).
  ///
  /// Recognizes the two edit shapes the agent emits:
  /// - an **`Edit`** — `old_string` → removed lines, `new_string` → added lines;
  /// - a **`Write`** — `content` → added lines (a whole-file write is all additions).
  ///
  /// Matching is case-insensitive on the tool `name`; the payload is read
  /// defensively (a missing/mistyped field yields no lines rather than a crash),
  /// so a malformed edit degrades to an empty diff instead of trapping (INV-NOFORCE).
  public var fileDiff: TranscriptDiff? {
    switch name.lowercased() {
    case "edit":
      let path = input.string(at: "file_path")
      let removed = input.string(at: "old_string").map(Self.lines(of: .removed)) ?? []
      let added = input.string(at: "new_string").map(Self.lines(of: .added)) ?? []
      return TranscriptDiff(filePath: path, lines: removed + added)
    case "write":
      let path = input.string(at: "file_path")
      let added = input.string(at: "content").map(Self.lines(of: .added)) ?? []
      return TranscriptDiff(filePath: path, lines: added)
    default:
      return nil
    }
  }

  /// Splits `text` into per-line ``DiffLine``s of the given kind, dropping a single
  /// trailing empty line so a payload with a terminal newline does not render a
  /// spurious blank line. An empty payload (a pure-insertion `Edit`'s empty
  /// `old_string`, or an empty `Write` `content`) yields no lines at all — not one
  /// blank line — so an empty side of the edit renders as nothing.
  private static func lines(of kind: DiffLine.Kind) -> (String) -> [DiffLine] {
    { text in
      guard !text.isEmpty else { return [] }
      var parts = text.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
      if parts.count > 1, parts.last == "" { parts.removeLast() }
      return parts.map { DiffLine(kind: kind, text: $0) }
    }
  }
}

extension JSONValue {
  /// The string at `key` when this value is an object whose `key` is a string;
  /// `nil` otherwise — the defensive accessor the diff transform reads payloads with.
  fileprivate func string(at key: String) -> String? {
    guard case .object(let fields) = self, case .string(let value)? = fields[key] else {
      return nil
    }
    return value
  }
}
