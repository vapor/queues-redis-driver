import Jobs
import JobsRedisDriver
import XCTVapor

final class JobsRedisDriverTests: XCTestCase {
    func testApplication() throws {
        let app = Application(.testing)
        defer { app.shutdown() }

        app.use(Jobs.self)
        let email = Email()
        app.jobs.add(email)
        try app.jobs.use(.redis(url: "redis://127.0.0.1:6379"))

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
