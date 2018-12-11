import XCTest

#if !os(macOS)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(redis_jobsTests.allTests),
    ]
}
#endif