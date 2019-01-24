import XCTest
import class Foundation.Bundle
import JobsRedisDriver
import Redis
import NIO
@testable import Jobs

final class JobsRedisDriverTests: XCTestCase {
    
    var eventLoop: EventLoop!
    var redisDatabase: RedisDatabase!
    var jobsDriver: JobsRedisDriver!
    var jobsConfig: JobsConfig!
    var redisConn: RedisClient!
    
    override func setUp() {
        do {
            guard let url = URL(string: "redis://localhost:6379") else { return }
            redisDatabase = try RedisDatabase(url: url)
            eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
            jobsDriver = JobsRedisDriver(database: redisDatabase, eventLoop: eventLoop)
            redisConn = try redisDatabase.newConnection(on: eventLoop).wait()
            
            jobsConfig = JobsConfig()
            jobsConfig.add(EmailJob.self)
        } catch {
            XCTFail()
        }
    }
    
    override func tearDown() {
        redisConn.close()
    }
    
    func testWarningExists() throws {
        let job = EmailJob(to: "email@email.com")
        try jobsDriver.set(key: "key", job: job, maxRetryCount: 1).wait()
    }

    func testSettingValue() throws {
        let job = EmailJob(to: "email@email.com")
        try jobsDriver.set(key: "key", job: job, maxRetryCount: 1).wait()
        
        guard let savedJobString = try redisConn.rPop("key").wait().data else {
            XCTFail()
            return
        }
        
        let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: savedJobString)
        guard let jobData = try jobsConfig.decode(from: decoder.decoder) else {
            XCTFail()
            return
        }

        let receivedJob = (jobData.data as! EmailJob)
        XCTAssertEqual(jobData.maxRetryCount, 1)
        XCTAssertEqual(jobData.key, "key")
        XCTAssertEqual(receivedJob.to, "email@email.com")
        
        //Assert that it was not added to the processing list
        XCTAssertNil(try redisConn.rPop("key-processing").wait().array)
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

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
