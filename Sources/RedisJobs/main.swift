@_exported import Jobs
import Redis
import Foundation
import NIO

extension RedisDatabase: JobsPersistenceLayer {
    
    /// Stores the job in Redis with the specified data
    ///
    /// - Parameters:
    ///   - key: The key to store the data
    ///   - job: The `Job` to store
    ///   - maxRetryCount: The number of retries to
    ///   - worker: An `EventLoopGroup` that can be used to generate future values
    /// - Returns: A future `Void` value used to signify completion
    public func set<J: Job>(key: String, job: J, maxRetryCount: Int, worker: EventLoopGroup) -> EventLoopFuture<Void> {
        return self.newConnection(on: worker).flatMap(to: RedisClient.self) { conn in
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
    ///   - worker: An `EventLoopGroup` that can be used to generate future values
    /// - Returns: A future `Void` value used to signify completion
    public func get(key: String, worker: EventLoopGroup) -> EventLoopFuture<JobData?> {
        let processingKey = key + "-processing"
        
        return self.newConnection(on: worker).flatMap { conn in
            return conn.rpoplpush(source: key, destination: processingKey).transform(to: conn)
        }.flatMap { conn in
            return conn.command("LPOP", [try processingKey.convertToRedisData()]).and(result: conn)
        }.map { redisData, conn in
            conn.close()
            guard let data = redisData.data else { return nil }
            return try? JSONDecoder().decode(JobData.self, from: data)
        }
    }
    
    /// Left blank because Redis does not need a cleanup with this implementation.
    /// See `JobsPersistenceLayer`.`completed` for a full description
    ///
    /// - Parameters:
    ///   - key: The key that was used to complete the job
    ///   - jobString: The string representation of the job
    ///   - worker: An `EventLoopGroup` that can be used to generate future values
    /// - Returns: A future `Void` value used to signify completion
    public func completed(key: String, jobString: String, worker: EventLoopGroup) -> EventLoopFuture<Void> {
        return worker.future()
    }
}
