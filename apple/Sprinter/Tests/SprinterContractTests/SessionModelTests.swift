import Foundation
import Testing

@testable import SprinterContract

@Suite("Session model")
struct SessionModelTests {
  @Test("decodes every SessionEvent variant and round-trips it")
  func decodesSessionEvents() throws {
    let events = try Golden.decode([SessionEvent].self, from: "session-events")
    #expect(events.count == 21)
    for event in events {
      #expect(try Golden.roundTrip(event) == event)
    }
  }

  @Test("maps the turn-lifecycle SessionEvent variants")
  func mapsLifecycleEvents() throws {
    let events = try Golden.decode([SessionEvent].self, from: "session-events")
    #expect(events[0] == .turnStarted)
    #expect(
      events[1]
        == .turnCompleted(
          usage: Usage(
            inputTokens: 1200, outputTokens: 340, cacheReadTokens: 800, cacheWriteTokens: 64)
        ))
    #expect(events[2] == .turnCompleted(usage: nil))
    #expect(events[12] == .sessionIdle)
    #expect(events[14] == .contextCompacted)
  }

  @Test("maps message deltas including optional text/reasoning")
  func mapsMessageDeltas() throws {
    let events = try Golden.decode([SessionEvent].self, from: "session-events")
    #expect(events[4] == .messageDelta(messageId: "m1", text: "Hello", reasoning: "thinking"))
    #expect(events[5] == .messageDelta(messageId: "m1", text: "world", reasoning: nil))
    #expect(events[6] == .messageDelta(messageId: "m1", text: nil, reasoning: "more thought"))
    #expect(events[7] == .messageDelta(messageId: "m1", text: nil, reasoning: nil))
  }

  @Test("maps tool and signal SessionEvent variants")
  func mapsToolAndSignalEvents() throws {
    let events = try Golden.decode([SessionEvent].self, from: "session-events")
    #expect(
      events[9]
        == .toolStarted(
          id: "t1",
          name: "read_file",
          input: .object(["path": .string("/etc/hosts"), "limit": .number(20)])
        ))
    #expect(events[13] == .retryScheduled(attempt: 2, delayMs: 1500, error: "429 rate limited"))
    #expect(
      events[15]
        == .uiRequestRaised(
          id: "req-1", kind: .select, prompt: "Pick one", options: ["a", "b"]
        ))
    #expect(
      events[16]
        == .uiRequestRaised(
          id: "req-2", kind: .confirm, prompt: "Proceed?", options: nil
        ))
    #expect(events[17] == .notice(id: "notice-disk", level: .warn, message: "disk space low"))
    #expect(events[18] == .statusChanged(key: "phase", text: "planning"))
    // The reconciliation-key-less Notice (optional NoticeId absent) decodes to a nil id.
    #expect(
      events[20]
        == .notice(id: nil, level: .error, message: "retry failed after 5 attempt(s)"))
  }

  @Test("decodes an EntryAppended carrying a transcript entry")
  func mapsEntryAppended() throws {
    let events = try Golden.decode([SessionEvent].self, from: "session-events")
    #expect(
      events[19]
        == .entryAppended(
          entry: .assistantMessage(id: "a1", text: "done", reasoning: "because")
        ))
  }

  @Test("decodes the dual-modality sessionEvents-stream envelope")
  func decodesOffsetSessionEvents() throws {
    let items = try Golden.decode([OffsetSessionEvent].self, from: "offset-session-events")
    #expect(items.count == 6)
    // The ONE channel interleaves DURABLE transcript-grade events (offset-BEARING) and
    // EPHEMERAL live deltas (offset-LESS); unwrapping `.event` gives the bare event the
    // existing SessionEvent fold consumes, regardless of whether an offset was present.
    // Items 0/2/4 are ephemeral (offset-less); items 1/3/5 are durable (offset-bearing).
    #expect(items[0].event == .turnStarted)
    #expect(
      items[1].event
        == .entryAppended(
          entry: .assistantMessage(id: "a1", text: "on it", reasoning: "planning")))
    #expect(items[2].event == .messageDelta(messageId: "a1", text: "on ", reasoning: nil))
    #expect(items[3].event == .notice(id: "notice-disk", level: .warn, message: "disk low"))
    #expect(
      items[4].event
        == .uiRequestRaised(id: "u1", kind: .confirm, prompt: "proceed?", options: nil))
    #expect(
      items[5].event
        == .entryAppended(entry: .toolResult(id: "c1", output: .number(3), isError: false)))

    // A missing `offset` key decodes to `nil` (ephemeral); a present one to the durable
    // per-session coordinate. The durable offsets are monotonic — the value a client feeds
    // back as `sinceOffset` (ephemerals never advance it).
    #expect(items.map(\.offset) == [nil, 2, nil, 3, nil, 4])
    for item in items {
      #expect(try Golden.roundTrip(item) == item)
    }
  }

  @Test("decodes every TranscriptEntry variant")
  func decodesTranscriptEntries() throws {
    let entries = try Golden.decode([TranscriptEntry].self, from: "transcript-entries")
    #expect(entries.count == 6)
    #expect(entries[0] == .userMessage(id: "u1", text: "please fix the bug"))
    #expect(entries[1] == .assistantMessage(id: "a1", text: "on it", reasoning: "planning"))
    #expect(entries[2] == .assistantMessage(id: "a2", text: "no reasoning here", reasoning: nil))
    #expect(
      entries[5] == .noticeEntry(id: "notice-compile", level: .error, message: "compilation failed")
    )
    for entry in entries {
      #expect(try Golden.roundTrip(entry) == entry)
    }
  }

  @Test("decodes Usage with and without optional cache fields")
  func decodesUsages() throws {
    let usages = try Golden.decode([Usage].self, from: "usages")
    #expect(usages.count == 2)
    #expect(usages[0].cacheReadTokens == 800)
    #expect(usages[1].cacheReadTokens == nil)
    #expect(usages[1].cacheWriteTokens == nil)
    for usage in usages {
      #expect(try Golden.roundTrip(usage) == usage)
    }
  }

  @Test("decodes every SessionInput mode (images optional)")
  func decodesSessionInputs() throws {
    let inputs = try Golden.decode([SessionInput].self, from: "session-inputs")
    #expect(inputs.count == 3)
    #expect(inputs[0].mode == .prompt)
    #expect(inputs[0].images == ["img-ref-1"])
    #expect(inputs[1].mode == .steer)
    #expect(inputs[1].images == nil)
    #expect(inputs[2].mode == .followUp)
    for input in inputs {
      #expect(try Golden.roundTrip(input) == input)
    }
  }

  @Test("decodes every UiResponse / UiAnswer variant")
  func decodesUiResponses() throws {
    let responses = try Golden.decode([UiResponse].self, from: "ui-responses")
    #expect(responses.count == 3)
    #expect(responses[0].answer == .value(value: "option-a"))
    #expect(responses[1].answer == .confirmed(confirmed: true))
    #expect(responses[2].answer == .cancelled)
    for response in responses {
      #expect(try Golden.roundTrip(response) == response)
    }
  }
}
