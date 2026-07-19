import SprinterContract
import Testing

@testable import SprinterAppSupport

/// Verifies the kind → answer mapping the shell relies on: every ``UiRequestKind`` maps
/// its user reply to the wire ``UiAnswer`` variant the daemon forwards verbatim. The
/// pre-fix Views always sent `Confirmed` / `Cancelled`, so `input` / `editor` / `select`
/// got the wrong variant — these tests pin the correct one per kind.
@Suite("UI request answering")
struct UiRequestAnsweringTests {
  @Test("confirm accepts a decision as Confirmed(true)")
  func confirmAccepts() throws {
    #expect(
      try makeUiAnswer(forKind: .confirm, reply: .decided(true)) == .confirmed(confirmed: true))
  }

  @Test("confirm declines as Confirmed(false)")
  func confirmDeclines() throws {
    #expect(
      try makeUiAnswer(forKind: .confirm, reply: .decided(false)) == .confirmed(confirmed: false))
  }

  @Test("input entered text maps to a Value")
  func inputMapsToValue() throws {
    #expect(try makeUiAnswer(forKind: .input, reply: .entered("a name")) == .value(value: "a name"))
  }

  @Test("editor edited body maps to a Value, preserving newlines")
  func editorMapsToValue() throws {
    let body = "line one\nline two"
    #expect(try makeUiAnswer(forKind: .editor, reply: .entered(body)) == .value(value: body))
  }

  @Test("select chosen option maps to a Value")
  func selectMapsToValue() throws {
    #expect(
      try makeUiAnswer(forKind: .select, reply: .entered("option-b")) == .value(value: "option-b"))
  }

  @Test("dismiss maps to Cancelled for every kind", arguments: UiRequestKind.allCases)
  func dismissMapsToCancelled(kind: UiRequestKind) throws {
    #expect(try makeUiAnswer(forKind: kind, reply: .dismissed) == .cancelled)
  }

  @Test("a text reply to a confirm prompt is a mismatch, not a wrong answer")
  func textToConfirmIsMismatch() {
    #expect(throws: UiAnswerMismatch(kind: .confirm)) {
      try makeUiAnswer(forKind: .confirm, reply: .entered("oops"))
    }
  }

  @Test(
    "a decision reply to a value-kind prompt is a mismatch",
    arguments: [UiRequestKind.input, .editor, .select])
  func decisionToValueKindIsMismatch(kind: UiRequestKind) {
    #expect(throws: UiAnswerMismatch(kind: kind)) {
      try makeUiAnswer(forKind: kind, reply: .decided(true))
    }
  }
}
