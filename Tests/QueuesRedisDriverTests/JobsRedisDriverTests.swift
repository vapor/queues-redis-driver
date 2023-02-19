@testable import QueuesRedisDriver
import Queues
import XCTVapor
import Redis
import Vapor

final class JobsRedisDriverTests: XCTestCase {
    func testApplication() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        let email = Email()
        app.queues.add(email)

        try app.queues.use(.redis(url: "redis://\(hostname):6379"))

        app.get("send-email") { req in
            req.queue.dispatch(Email.self, .init(to: "tanner@vapor.codes"))
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "send-email") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        XCTAssertEqual(email.sent, [])
        try app.queues.queue.worker.run().wait()
        XCTAssertEqual(email.sent, [.init(to: "tanner@vapor.codes")])
    }
    
    func testFailedJobLoss() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        app.queues.add(FailingJob())
        try app.queues.use(.redis(url: "redis://\(hostname):6379"))
        let jobId = JobIdentifier()
        app.get("test") { req in
            req.queue.dispatch(FailingJob.self, ["foo": "bar"], id: jobId)
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "test") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        do {
            try app.queues.queue.worker.run().wait()
        } catch is FailingJob.Failure {
            // pass
        } catch {
            XCTFail("unepxected error: \(error)")
        }
        
        // ensure this failed job is still in storage
        let redis = (app.queues.queue as! RedisClient)
        let job = try redis.get(RedisKey("job:\(jobId.string)"), asJSON: JobData.self).wait()!
        XCTAssertEqual(job.jobName, "FailingJob")
    }
    
    func testDateEncoding() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        app.queues.add(DelayedJob())

        try app.queues.use(.redis(url: "redis://\(hostname):6379"))
        let jobId = JobIdentifier()
        app.get("delay-job") { req in
            req.queue.dispatch(DelayedJob.self, .init(name: "vapor"),
                               delayUntil: Date(timeIntervalSince1970: 1609477200),
                               id: jobId) // Jan 1, 2021
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "delay-job") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        // Verify the delayUntil date is encoded as the correct epoch time
        let redis = (app.queues.queue as! RedisClient)
        let job = try redis.get(RedisKey("job:\(jobId.string)")).wait()
        let dict = try JSONSerialization.jsonObject(with: job.data!, options: .allowFragments) as! [String: Any]
        
        XCTAssertEqual(dict["jobName"] as! String, "DelayedJob")
        XCTAssertEqual(dict["delayUntil"] as! Int, 1609477200)
    }
    
    func testDelayedJobIsRemovedFromProcessingQueue() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        app.queues.add(DelayedJob())

        try app.queues.use(.redis(url: "redis://\(hostname):6379"))
        let jobId = JobIdentifier()
        app.get("delay-job") { req in
            req.queue.dispatch(DelayedJob.self, .init(name: "vapor"),
                               delayUntil: Date().addingTimeInterval(3600),
                               id: jobId)
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "delay-job") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        // Verify that a delayed job isn't still in processing after it's been put back in the queue
        try app.queues.queue.worker.run().wait()
        let redis = (app.queues.queue as! RedisClient)
        let value = try redis.lrange(from: RedisKey("vapor_queues[default]-processing"),
                                     indices: 0...10,
                                     as: String.self).wait()
        let originalQueue = try redis.lrange(from: RedisKey("vapor_queues[default]"),
                                             indices: 0...10,
                                             as: String.self).wait()
        XCTAssertEqual(value.count, 0)
        XCTAssertTrue(originalQueue.contains(jobId.string))
    }
}

var hostname: String {
    ProcessInfo.processInfo.environment["REDIS_HOSTNAME"] ?? "localhost"
}

final class Email: Job {
    struct Message: Codable, Equatable {
        let to: String
    }
    
    var sent: [Message]
    
    init() {
        self.sent = []
    }
    
    func dequeue(_ context: QueueContext, _ message: Message) -> EventLoopFuture<Void> {
        self.sent.append(message)
        context.logger.info("sending email \(message)")
        return context.eventLoop.makeSucceededFuture(())
    }
}

final class DelayedJob: Job {
    struct Message: Codable, Equatable {
        let name: String
    }
    
    init() {}
    
    func dequeue(_ context: QueueContext, _ message: Message) -> EventLoopFuture<Void> {
        context.logger.info("Hello \(message.name)")
        return context.eventLoop.makeSucceededFuture(())
    }
}

struct FailingJob: Job {
    struct Failure: Error { }
    
    init() { }
    
    func dequeue(_ context: QueueContext, _ message: [String: String]) -> EventLoopFuture<Void> {
        return context.eventLoop.makeFailedFuture(Failure())
    }
    
    func error(_ context: QueueContext, _ error: Error, _ payload: [String : String]) -> EventLoopFuture<Void> {
        return context.eventLoop.makeFailedFuture(Failure())
    }
}
