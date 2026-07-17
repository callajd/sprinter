import Foundation
import Testing

@testable import SprinterCore

@Suite("Workstream")
struct WorkstreamTests {
  @Test("constructs a valid workstream")
  func constructsValid() throws {
    let workstream = try Workstream(id: "fdn", name: "Foundation", status: .active)
    #expect(workstream.id == "fdn")
    #expect(workstream.name == "Foundation")
    #expect(workstream.status == .active)
    #expect(workstream.isComplete == false)
  }

  @Test("isComplete is true only for a done workstream")
  func doneIsComplete() throws {
    let done = try Workstream(id: "a", name: "A", status: .done)
    #expect(done.isComplete)
  }

  @Test("rejects an empty identifier")
  func rejectsEmptyIdentifier() {
    #expect(throws: WorkGraphError.emptyIdentifier) {
      try Workstream(id: "", name: "x", status: .pending)
    }
  }

  @Test("rejects an empty name")
  func rejectsEmptyName() {
    #expect(throws: WorkGraphError.emptyName) {
      try Workstream(id: "a", name: "", status: .pending)
    }
  }

  @Test("preserves status across the lifecycle", arguments: WorkStatus.allCases)
  func preservesStatus(status: WorkStatus) throws {
    let workstream = try Workstream(id: "id", name: "name", status: status)
    #expect(workstream.status == status)
    #expect(workstream.isComplete == (status == .done))
  }

  @Test("round-trips through Codable")
  func codableRoundTrip() throws {
    let original = try Workstream(id: "fdn", name: "Foundation", status: .active)
    let data = try JSONEncoder().encode(original)
    let decoded = try JSONDecoder().decode(Workstream.self, from: data)
    #expect(decoded == original)
  }
}
