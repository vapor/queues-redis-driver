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
        try! redisConn.delete("key").wait()
        try! redisConn.delete("key-processing").wait()
        redisConn.close()
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
        XCTAssertNil(try redisConn.lrange(list: "key-processing", range: 0...0).wait().data)
    }
    
    func testGettingValue() throws {
        let firstJob = EmailJob(to: "email@email.com")
        let secondJob = EmailJob(to: "email2@email.com")
        
        try jobsDriver.set(key: "key", job: firstJob, maxRetryCount: 1).wait()
        try jobsDriver.set(key: "key", job: secondJob, maxRetryCount: 1).wait()

        guard let fetchedJobData = try jobsDriver.get(key: "key", jobsConfig: jobsConfig).wait() else {
            XCTFail()
            return
        }

        let fetchedJob = fetchedJobData.data as! EmailJob
        XCTAssertEqual(fetchedJob.to, "email@email.com")
        
        //Assert that the base list still has data in it and the processing list has nothing
        XCTAssertNotNil(try redisConn.lrange(list: "key", range: 0...0).wait().array)
        XCTAssertEqual(try redisConn.lrange(list: "key-processing", range: 0...0).wait().array!.count,0 )
    }
    
    static var allTests = [
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
