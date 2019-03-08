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
        }.flatMap(to: (RedisData, RedisClient).self) { redisData, conn in
            guard let id = redisData.string else {
                conn.close()
                throw Abort(.internalServerError)
            }
            
            return conn.rawGet(id).and(result: conn)
        }.map { redisData, conn in
            conn.close()
            
            guard let data = redisData.data else {
                print("Could not convert redis data to Data")
                return nil
            }
            
            let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: data)
            return try JobStorage(from: decoder.decoder)
        }.catchMap { error in
            return nil
        }
    }
    
    /// See `JobsPersistenceLayer.set`
    public func set(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        return database.newConnection(on: eventLoop).flatMap(to: (RedisData, RedisClient).self) { conn in
            let data = try JSONEncoder().encode(jobStorage).convertToRedisData()
            return conn.lpush([try jobStorage.id.convertToRedisData()], into: key).transform(to: (data, conn))
        }.flatMap { data, conn in
            return conn.set(jobStorage.id, to: data).transform(to: conn)
        }.map { conn in
            return conn.close()
        }
    }
    
    /// See `JobsPersistenceLayer.completed`
    public func completed(key: String, jobStorage: JobStorage) -> EventLoopFuture<Void> {
        return database.newConnection(on: eventLoop).flatMap(to: RedisClient.self) { conn in
            let processing = try self.processingKey(key: key).convertToRedisData()
            let count = try 0.convertToRedisData()

            return conn.command("LREM", [processing, count, try jobStorage.id.convertToRedisData()]).transform(to: conn)
        }.flatMap(to: RedisClient.self) { conn in
            return conn.delete(jobStorage.id).transform(to: conn)
        }.map { conn in
            conn.close()
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
