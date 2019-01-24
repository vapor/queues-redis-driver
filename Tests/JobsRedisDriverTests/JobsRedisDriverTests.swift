import XCTest
import class Foundation.Bundle
import JobsRedisDriver
import Redis
import NIO
import Jobs

final class JobsRedisDriverTests: XCTestCase {
    
    var eventLoop: EventLoop!
    var redisDatabase: RedisDatabase!
    var jobsDriver: JobsRedisDriver!
    
    override func setUp() {
        do {
            guard let url = URL(string: "redis://localhost:6379") else { return }
            redisDatabase = try RedisDatabase(url: url)
            eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
            jobsDriver = JobsRedisDriver(database: redisDatabase, eventLoop: eventLoop)
        } catch {
            XCTFail()
        }
    }
    
    func testWarningExists() throws {
        let job = EmailJob(to: "email@email.com")
        try jobsDriver.set(key: "key", job: job, maxRetryCount: 1).wait()
    }

    func testSettingValue() throws {
        let job = EmailJob(to: "email@email.com")
        try jobsDriver.set(key: "key", job: job, maxRetryCount: 1).wait()
        
        guard let savedJobString = try redisDatabase.newConnection(on: eventLoop).wait().get("key", as: String.self).wait() else {
            XCTFail()
            return
        }

        
    }
    
    func testGettingValue() throws {
        
    }
    
    static var allTests = [
        ("testWarningExists", testWarningExists),
        ("testSettingValue", testSettingValue),
        ("testGettingValue", testGettingValue)
    ]
}

struct EmailJob: Job {
    let to: String
    
    func dequeue(context: JobContext, worker: EventLoopGroup) -> EventLoopFuture<Void> {
        return worker.future()
    }
}

