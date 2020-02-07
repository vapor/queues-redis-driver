import Jobs
import JobsRedisDriver
import XCTVapor

final class JobsRedisDriverTests: XCTestCase {
    func testApplication() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        let email = Email()
        app.jobs.add(email)

        try app.jobs.use(.redis(url: "redis://\(hostname):6379"))

        app.get("send-email") { req in
            req.jobs.dispatch(Email.self, .init(to: "tanner@vapor.codes"))
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "send-email") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        XCTAssertEqual(email.sent, [])
        try app.jobs.queue.worker.run().wait()
        XCTAssertEqual(email.sent, [.init(to: "tanner@vapor.codes")])
    }
    
    func testFailedJobLoss() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        app.jobs.add(FailingJob())
        try app.jobs.use(.redis(url: "redis://\(hostname):6379"))

        app.get("test") { req in
            req.jobs.dispatch(FailingJob.self, ["foo": "bar"])
                .map { HTTPStatus.ok }
        }

        try app.testable().test(.GET, "test") { res in
            XCTAssertEqual(res.status, .ok)
        }
        
        do {
            try app.jobs.queue.worker.run().wait()
        } catch is FailingJob.Failure {
            // pass
        } catch {
            XCTFail("unepxected error: \(error)")
        }
        
        // ensure this failed job is still in storage
        let redis = (app.jobs.queue as! RedisClient)
        let keys = try redis.send(command: "KEYS", with: ["*".convertedToRESPValue()]).wait()
        let id = keys.array![0].string!
        let job = try redis.get(id, asJSON: JobData.self).wait()!
        XCTAssertEqual(job.jobName, "FailingJob")
        _ = try redis.delete(id).wait()
    }

    func testApplicationStart() throws {
        var env = Environment(name: "testing", arguments: ["vapor", "jobs"])
        let app = Application(env)
        defer { app.shutdown() }

        let promise = Promise(on: app.eventLoopGroup.next())
        app.jobs.add(promise)
        try app.jobs.use(.redis(url: "redis://\(hostname):6379"))

        try app.start()

        try app.jobs.queue(.default)
            .dispatch(Promise.self, "test")
            .wait()

        try XCTAssertEqual(promise.future.wait(), "test")
    }
}

var hostname: String {
    ProcessInfo.processInfo.environment["REDIS_HOSTNAME"] ?? "localhost"
}

final class Promise: Job {
    private let promise: EventLoopPromise<String>

    var future: EventLoopFuture<String> {
        self.promise.futureResult
    }

    init(on eventLoop: EventLoop) {
        self.promise = eventLoop.makePromise()
    }

    func dequeue(_ context: JobContext, _ message: String) -> EventLoopFuture<Void> {
        self.promise.succeed(message)
        context.logger.info("promise succeeded \(message)")
        return context.eventLoop.makeSucceededFuture(())
    }
}

final class Email: Job {
    struct Message: Codable, Equatable {
        let to: String
    }
    
    var sent: [Message]
    
    init() {
        self.sent = []
    }
    
    func dequeue(_ context: JobContext, _ message: Message) -> EventLoopFuture<Void> {
        self.sent.append(message)
        context.logger.info("sending email \(message)")
        return context.eventLoop.makeSucceededFuture(())
    }
}

struct FailingJob: Job {
    struct Failure: Error { }
    
    init() { }
    
    func dequeue(_ context: JobContext, _ message: [String: String]) -> EventLoopFuture<Void> {
        return context.eventLoop.makeFailedFuture(Failure())
    }
    
    func error(_ context: JobContext, _ error: Error, _ payload: [String : String]) -> EventLoopFuture<Void> {
        return context.eventLoop.makeFailedFuture(Failure())
    }
}
