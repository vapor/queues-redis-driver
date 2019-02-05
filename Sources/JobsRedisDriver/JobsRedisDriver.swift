import Jobs
import Redis
import NIO
import Foundation
import Vapor

/// A wrapper that conforms to `JobsPersistenceLayer`
public struct JobsRedisDriver {
    
    /// The `RedisDatabase` to run commands on
    let database: RedisDatabase
    
    /// The `EventLoop` to run jobs on
    public let eventLoop: EventLoop
    
    /// Creates a new `RedisJobs` instance
    ///
    /// - Parameters:
    ///   - database: The `RedisDatabase` to run commands on
    ///   - eventLoop: The `EventLoop` to run jobs on
    public init(database: RedisDatabase, eventLoop: EventLoop) {
        self.database = database
        self.eventLoop = eventLoop
    }
}

extension JobsRedisDriver: JobsPersistenceLayer {
    
    /// See `JobsPersistenceLayer.get`
    public func get(key: String) -> EventLoopFuture<JobStorage?> {
        let processing = processingKey(key: key)
        
        return database.newConnection(on: eventLoop).flatMap { conn in
            return conn.rpoplpush(source: key, destination: processing).and(result: conn)
        }.map { redisData, conn in
            conn.close()
            guard let data = redisData.data else { return nil }
            let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: data)
            return try JobStorage(from: decoder.decoder)
        }
    }
    
    /// See `JobsPersistenceLayer.set`
    public func set(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        return database.newConnection(on: eventLoop).flatMap(to: RedisClient.self) { conn in
            let data = try JSONEncoder().encode(jobStorage).convertToRedisData()
            return conn.lpush([data], into: key).transform(to: conn)
        }.map { conn in
            return conn.close()
        }
    }
    
    /// See `JobsPersistenceLayer.completed`
    public func completed(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        return database.newConnection(on: eventLoop).flatMap { conn in
            let processing = try self.processingKey(key: key).convertToRedisData()
            let count = try 1.convertToRedisData()
            
            guard let value = try jobStorage.stringValue()?.convertToRedisData() else { throw Abort(.internalServerError, reason: "Cannot get string value") }
            return conn.command("LREM", [processing, count, value]).transform(to: ())
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
