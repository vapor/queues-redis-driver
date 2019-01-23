import XCTest
import class Foundation.Bundle
import JobsRedisDriver
import Redis
import NIO
import Jobs

final class JobsRedisDriverTests: XCTestCase {
    
    func testWarningExists() throws {
        guard let url = URL(string: "redis://localhost:6379") else { return }
        let database = try RedisDatabase(url: url)
        let el = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
        let redisInstance = JobsRedisDriver(database: database, eventLoop: el)
        
        let job = EmailJob()
        try redisInstance.set(key: "key", job: job, maxRetryCount: 1).wait()
    }

    static var allTests = [
        ("testWarningExists", testWarningExists),
    ]
}

struct EmailJob: Job {
    func dequeue(context: JobContext, worker: EventLoopGroup) -> EventLoopFuture<Void> {
        return worker.future()
    }
}

