@_exported import Jobs
import Redis
import NIO
import Foundation

/// A wrapper that conforms to `JobsPersistenceLayer`
public struct RedisJobs {
    
    /// The `RedisDatabase` to run commands on
    let database: RedisDatabase
    
    /// The `EventLoop` to run jobs on
    var _eventLoop: EventLoop
    
    /// Creates a new `RedisJobs` instance
    ///
    /// - Parameters:
    ///   - database: The `RedisDatabase` to run commands on
    ///   - eventLoop: The `EventLoop` to run jobs on
    public init(database: RedisDatabase, eventLoop: EventLoop) {
        self.database = database
        self._eventLoop = eventLoop
    }
}

extension RedisJobs: JobsPersistenceLayer {
    
    /// The `EventLoop` to run jobs on
    public var eventLoop: EventLoop {
        get {
            return self._eventLoop
        }
        set(newValue) {
            self._eventLoop = newValue
        }
    }
    
    /// Stores the job in Redis with the specified data
    ///
    /// - Parameters:
    ///   - key: The key to store the data
    ///   - job: The `Job` to store
    ///   - maxRetryCount: The number of retries to
    /// - Returns: A future `Void` value used to signify completion
    public func set<J: Job>(key: String, job: J, maxRetryCount: Int) -> EventLoopFuture<Void> {
        return database.newConnection(on: eventLoop).flatMap(to: RedisClient.self) { conn in
            let jobData = JobData(key: key, data: job, maxRetryCount: maxRetryCount)
            let data = try JSONEncoder().encode(jobData).convertToRedisData()
            return conn.lpush([data], into: key).transform(to: conn)
        }.map { conn in
            return conn.close()
        }
    }
    
    /// Returns the job data using rpoplpush
    ///
    /// - Parameters:
    ///   - key: The key to retrieve the data from
    ///   - jobsConfig: The `JobsConfig` object registered via services
    /// - Returns: The returned `JobData` object, if it exists
    public func get(key: String, jobsConfig: JobsConfig) -> EventLoopFuture<JobData?> {
        let processingKey = key + "-processing"
        
        return database.newConnection(on: eventLoop).flatMap { conn in
            return conn.rpoplpush(source: key, destination: processingKey).transform(to: conn)
        }.flatMap { conn in
            return conn.command("LPOP", [try processingKey.convertToRedisData()]).and(result: conn)
        }.map { redisData, conn in
            conn.close()
            guard let data = redisData.data else { return nil }
            let decoder = try JSONDecoder().decode(DecoderUnwrapper.self, from: data)
            return try jobsConfig.decode(from: decoder.decoder)
        }
    }
    
    /// Left blank because Redis does not need a cleanup with this implementation.
    /// See `JobsPersistenceLayer`.`completed` for a full description
    ///
    /// - Parameters:
    ///   - key: The key that was used to complete the job
    ///   - jobString: The string representation of the job
    /// - Returns: A future `Void` value used to signify completion
    public func completed(key: String, jobString: String) -> EventLoopFuture<Void> {
        return eventLoop.future()
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
