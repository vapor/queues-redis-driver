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
            jobsConfig.add(EmailJob())
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
        let job = Email(to: "email@email.com")
        let jobData = try JSONEncoder().encode(job)
        let jobStorage = JobStorage(key: "key", data: jobData, maxRetryCount: 1, id: UUID().uuidString, jobName: EmailJob.jobName)
        
        try jobsDriver.set(key: "key", jobStorage: jobStorage).wait()
        
        XCTAssertNotNil(try redisConn.rawGet(jobStorage.id).wait().string)
        
        guard let jobId = try redisConn.rPop("key").wait().string else {
            XCTFail()
            return
        }
        
        guard let retrievedJobData = try redisConn.rawGet(jobId).wait().data else {
            XCTFail()
            return
        }
        
        let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: retrievedJobData)
        let retrievedJobStorage = try JobStorage(from: decoder.decoder)
        let retrievedJob = try JSONDecoder().decode(Email.self, from: retrievedJobStorage.data)
        
        XCTAssertEqual(retrievedJobStorage.maxRetryCount, 1)
        XCTAssertEqual(retrievedJobStorage.key, "key")
        XCTAssertEqual(retrievedJob.to, "email@email.com")
        
        //Assert that it was not added to the processing list
        XCTAssertNil(try redisConn.lrange(list: "key-processing", range: 0...0).wait().data)
    }
    
    func testGettingValue() throws {
        let firstJob = Email(to: "email@email.com")
        let secondJob = Email(to: "email2@email.com")
        
        let firstJobData = try JSONEncoder().encode(firstJob)
        let secondJobData = try JSONEncoder().encode(secondJob)
        
        let firstJobStorage = JobStorage(key: "key", data: firstJobData, maxRetryCount: 1, id: UUID().uuidString, jobName: EmailJob.jobName)
        let secondJobStorage = JobStorage(key: "key", data: secondJobData, maxRetryCount: 1, id: UUID().uuidString, jobName: EmailJob.jobName)
        
        try jobsDriver.set(key: "key", jobStorage: firstJobStorage).wait()
        try jobsDriver.set(key: "key", jobStorage: secondJobStorage).wait()

        guard let fetchedJobData = try jobsDriver.get(key: "key").wait() else {
            XCTFail()
            return
        }

        let fetchedJob = try JSONDecoder().decode(Email.self, from: fetchedJobData.data)
        XCTAssertEqual(fetchedJob.to, "email@email.com")
        
        //Assert that the base list still has data in it and the processing list has 1
        XCTAssertNotNil(try redisConn.lrange(list: "key", range: 0...0).wait().array)
        XCTAssertEqual(try redisConn.lrange(list: "key-processing", range: 0...0).wait().array!.count, 1)
        
        try jobsDriver.completed(key: "key", jobStorage: fetchedJobData).wait()
        XCTAssertEqual(try redisConn.lrange(list: "key-processing", range: 0...0).wait().array!.count, 0)
    }
    
    static var allTests = [
        ("testSettingValue", testSettingValue),
        ("testGettingValue", testGettingValue)
    ]
}

struct Email: Codable, JobData {
    let to: String
}

struct EmailJob: Job {
    func dequeue(_ context: JobContext, _ data: Email) -> EventLoopFuture<Void> {
        return context.eventLoop.future()
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
