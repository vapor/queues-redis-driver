import Jobs
import RedisKit
import NIO
import Foundation
import Vapor

/// A wrapper that conforms to `JobsPersistenceLayer`
public struct JobsRedisDriver: JobsDriver {
    /// The `RedisClient` to run commands on
    public let client: RedisClient

    let logger: Logger
    
    /// The `EventLoop` to run jobs on
    public var eventLoop: EventLoop {
        return self.client.eventLoop
    }
    
    /// Creates a new `RedisJobs` instance
    ///
    /// - Parameters:
    ///   - client: The `RedisClient` to use
    public init(client: RedisClient) {
        self.client = client
        self.logger = Logger(label: "codes.vapor.jobs-redis-driver")
    }

    /// See `JobsPersistenceLayer.get`
    public func get(key: String) -> EventLoopFuture<JobStorage?> {
        let processing = processingKey(key: key)
        return self.client.rpoplpush(from: key, to: processing).flatMap { redisData -> EventLoopFuture<String?> in
            guard !redisData.isNull else {
                return self.eventLoop.makeSucceededFuture(nil)
            }

            guard let id = redisData.string else {
                self.logger.error("Could not convert RedisData to string: \(redisData)")
                return self.eventLoop.makeFailedFuture(Abort(.internalServerError))
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
    
    /// See `JobsPersistenceLayer.set`
    public func set(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        do {
            let data = try JSONEncoder().encode(jobStorage).convertedToRESPValue()
            
            return client.set(jobStorage.id, to: data).flatMap { data in
                return self.client.lpush([jobStorage.id.convertedToRESPValue()], into: key).transform(to: ())
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    /// See `JobsPersistenceLayer.completed`
    public func completed(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        let processing = self.processingKey(key: key)
        let jobData = jobStorage.id.convertedToRESPValue()
        
        return client.lrem(jobData, from: processing, count: 0).flatMap { _ in
            return self.client.delete([jobStorage.id]).transform(to: ())
        }
    }
    
    /// See `JobsPersistenceLayer.processingKey`
    public func processingKey(key: String) -> String {
        return key + "-processing"
    }
    
    /// See `JobsPersistenceLayer.requeue`
    public func requeue(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        let processing = self.processingKey(key: key)
        let jobData = jobStorage.id.convertedToRESPValue()
        
        // Remove the job from the processing list
        return client.lrem(jobData, from: processing, count: 0).flatMap { _ in
            
            // Add the job back to the queue list
            return self.client.lpush([jobData], into: key).transform(to: ())
        }
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
