import XCTest

import redis_jobsTests

var tests = [XCTestCaseEntry]()
tests += redis_jobsTests.allTests()
XCTMain(tests)