import Jobs
import RedisKit
import NIO
import Foundation
import Vapor

/// A wrapper that conforms to `JobsDriver`
public struct JobsRedisDriver: JobsDriver {
    /// The `RedisClient` to run commands on
    public let client: RedisClient
    
    /// The event loop group
    public let eventLoopGroup: EventLoopGroup
    
    /// The logger
    let logger: Logger
    
    /// Creates a new `RedisJobs` instance
    ///
    /// - Parameters:
    ///   - client: The `RedisClient` to use
    public init(client: RedisClient, eventLoopGroup: EventLoopGroup) {
        self.client = client
        self.eventLoopGroup = eventLoopGroup
        self.logger = Logger(label: "codes.vapor.jobs-redis-driver")
    }
    
    /// See `JobsDriver.get`
    public func get(key: String, eventLoop: JobsEventLoopPreference) -> EventLoopFuture<JobStorage?> {
        let processing = processingKey(key: key)
        let el = eventLoop.delegate(for: self.eventLoopGroup)
        
        return self.client.rpoplpush(from: key, to: processing).flatMap { redisData -> EventLoopFuture<String?> in
            guard !redisData.isNull else {
                return el.makeSucceededFuture(nil)
            }

            guard let id = redisData.string else {
                self.logger.error("Could not convert RedisData to string: \(redisData)")
                return el.makeFailedFuture(Abort(.internalServerError))
            }
            
            return self.client.get(id)
        }.flatMapThrowing { redisData -> JobStorage? in
            guard let redisData = redisData else {
                return nil
            }

            guard let data = redisData.data(using: .utf8) else {
                self.logger.error("Could not convert redis data to string: \(redisData)")
                throw Abort(.internalServerError)
            }
            
            let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: data)
            return try JobStorage(from: decoder.decoder)
        }
    }
    
    /// See `JobsDriver.set`
    public func set(key: String, job: JobStorage, eventLoop: JobsEventLoopPreference) -> EventLoopFuture<Void> {
        do {
            let data = try JSONEncoder().encode(job).convertedToRESPValue()
            
            return client.set(job.id, to: data).flatMap { data in
                return self.client.lpush([job.id.convertedToRESPValue()], into: key).transform(to: ())
            }
        } catch {
            return eventLoop.delegate(for: self.eventLoopGroup).makeFailedFuture(error)
        }
    }
    
    /// See `JobsDriver.completed`
    public func completed(key: String, job: JobStorage, eventLoop: JobsEventLoopPreference) -> EventLoopFuture<Void> {
        let processing = self.processingKey(key: key)
        let jobData = job.id.convertedToRESPValue()
        
        return client.lrem(jobData, from: processing, count: 0).flatMap { _ in
            return self.client.delete([job.id]).transform(to: ())
        }
    }
    
    /// See `JobsDriver.requeue`
    public func requeue(key: String, job: JobStorage, eventLoop: JobsEventLoopPreference) -> EventLoopFuture<Void> {
        let processing = self.processingKey(key: key)
        let jobData = job.id.convertedToRESPValue()
        
        // Remove the job from the processing list
        return client.lrem(jobData, from: processing, count: 0).flatMap { _ in
            
            // Add the job back to the queue list
            return self.client.lpush([jobData], into: key).transform(to: ())
        }
    }
    
    /// See `JobsDriver.processingKey`
    public func processingKey(key: String) -> String {
        return key + "-processing"
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
