import Jobs
import RedisKit
import NIO
import Foundation
import Vapor

/// A wrapper that conforms to `JobsPersistenceLayer`
public struct JobsRedisDriver {
    
    /// The `RedisClient` to run commands on
    let client: RedisClient
    
    /// The `EventLoop` to run jobs on
    public let eventLoop: EventLoop
    
    /// Creates a new `RedisJobs` instance
    ///
    /// - Parameters:
    ///   - database: The `RedisDatabase` to run commands on
    ///   - eventLoop: The `EventLoop` to run jobs on
    public init(client: RedisClient, eventLoop: EventLoop) {
        self.client = client
        self.eventLoop = eventLoop
    }
}

extension JobsRedisDriver: JobsPersistenceLayer {
    
    /// See `JobsPersistenceLayer.get`
    public func get(key: String) -> EventLoopFuture<JobStorage?> {
        let processing = processingKey(key: key)
        
        return client.rpoplpush(from: key, to: processing).flatMap { redisData -> EventLoopFuture<String?> in
            guard let id = redisData.string else {
                return self.eventLoop.makeFailedFuture(Abort(.internalServerError))
            }
            
            return self.client.get(id)
        }.flatMapThrowing { redisData in
            guard let data = redisData?.data(using: .utf8) else {
                print("Could not convert redis data to Data")
                return nil
            }
            
            let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: data)
            return try JobStorage(from: decoder.decoder)
        }.flatMapError { _ in
            return self.eventLoop.makeSucceededFuture(nil)
        }
    }
    
    /// See `JobsPersistenceLayer.set`
    public func set(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        guard let data = try? JSONEncoder().encode(jobStorage).convertedToRESPValue() else {
            return self.eventLoop.makeFailedFuture(JobsRedisDriverError.couldNotConvertData)
        }
        
        return client.set(jobStorage.id, to: data).flatMap { data in
            return self.client.lpush([jobStorage.id.convertedToRESPValue()], into: key).transform(to: ())
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
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
