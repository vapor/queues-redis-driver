import Queues
import Logging
import Redis
import RediStack
import NIOCore
import NIOPosix
import NIOSSL
import Foundation
import Vapor

struct InvalidRedisURL: Error {
    let url: String
}

extension Application.Queues.Provider {

    /// Sets the driver to `Redis`
    /// - Parameter string: The `Redis` connection URL string
    /// - Throws: An error describing an invalid URL string
    /// - Returns: The new provider
    public static func redis(url string: String) throws -> Self {
        guard let url = URL(string: string) else {
            throw InvalidRedisURL(url: string)
        }
        return try .redis(url: url)
    }

    /// Sets the driver to `Redis`
    /// - Parameter url: The `Redis` connection URL
    /// - Throws: An error describing an invalid URL
    /// - Returns: The new provider
    public static func redis(url: URL) throws -> Self {
        guard let configuration = try? RedisConfiguration(url: url) else {
            throw InvalidRedisURL(url: url.absoluteString)
        }
        return .redis(configuration)
    }

    /// Sets the driver to `Redis`
    /// - Parameter configuration: The `RedisConfiguration` to enable the provider
    /// - Returns: The new provider
    public static func redis(_ configuration: RedisConfiguration) -> Self {
        .init {
            $0.queues.use(custom: RedisQueuesDriver(configuration: configuration, on: $0.eventLoopGroup))
        }
    }
}

/// A `QueuesDriver` for Redis
public struct RedisQueuesDriver {
    let pool: RedisConnectionPool

    /// Creates the RedisQueuesDriver
    /// - Parameters:
    ///   - configuration: The `RedisConfiguration` to boot the driver
    ///   - eventLoopGroup: The `EventLoopGroup` to run the driver with
    public init(configuration config: RedisConfiguration, on eventLoopGroup: EventLoopGroup) {
        let logger = Logger(label: "codes.vapor.redis")
        let eventLoop = eventLoopGroup.any()
        let redisTLSClient: ClientBootstrap? = {
            guard let tlsConfig = config.tlsConfiguration else { return nil }

            return ClientBootstrap(group: eventLoop)
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SocketOptionName(SO_REUSEADDR)), value: SocketOptionValue(1))
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(NIOSSLClientHandler(
                            context: NIOSSLContext(configuration: tlsConfig),
                            serverHostname: config.tlsHostname
                        ))
                    }
                    .flatMap { channel.pipeline.addBaseRedisHandlers() }
                }
        }()
        self.pool = RedisConnectionPool(
            configuration: .init(
                initialServerConnectionAddresses: config.serverAddresses,
                maximumConnectionCount: config.pool.maximumConnectionCount,
                connectionFactoryConfiguration: .init(
                    connectionInitialDatabase: config.database,
                    connectionPassword: config.password,
                    connectionDefaultLogger: logger,
                    tcpClient: redisTLSClient
                ),
                minimumConnectionCount: config.pool.minimumConnectionCount,
                connectionBackoffFactor: config.pool.connectionBackoffFactor,
                initialConnectionBackoffDelay: config.pool.initialConnectionBackoffDelay,
                connectionRetryTimeout: config.pool.connectionRetryTimeout,
                poolDefaultLogger: logger
            ),
            boundEventLoop: eventLoop
        )
    }

    /// Shuts down the driver
    public func shutdown() {
        self.pool.close()
    }
}

extension RedisQueuesDriver: QueuesDriver {

    /// Makes the `Queue`
    /// - Parameter context: Context to be passed to the creation of the `Queue`
    /// - Returns: The created `Queue`
    public func makeQueue(with context: QueueContext) -> Queue {
        _QueuesRedisQueue(
            client: self.pool,
            context: context
        )
    }
}

struct _QueuesRedisQueue<Client: RedisClient> {
    let client: Client
    let context: QueueContext
}

extension _QueuesRedisQueue: RedisClient {
    var isConnected: Bool {
        return true
    }

    var eventLoop: EventLoop {
        self.context.eventLoop
    }

    func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.client.send(command: command, with: arguments)
    }

    func logging(to logger: Logger) -> RedisClient {
        return self.client.logging(to: logger)
    }

    func subscribe(
        to channels: [RedisChannelName],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.client.subscribe(to: channels, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    func psubscribe(
        to patterns: [String],
        messageReceiver receiver: @escaping RedisSubscriptionMessageReceiver,
        onSubscribe subscribeHandler: RedisSubscriptionChangeHandler?,
        onUnsubscribe unsubscribeHandler: RedisSubscriptionChangeHandler?
    ) -> EventLoopFuture<Void> {
        return self.client.psubscribe(to: patterns, messageReceiver: receiver, onSubscribe: subscribeHandler, onUnsubscribe: unsubscribeHandler)
    }

    func unsubscribe(from channels: [RedisChannelName]) -> EventLoopFuture<Void> {
        return self.client.unsubscribe(from: channels)
    }

    func punsubscribe(from patterns: [String]) -> EventLoopFuture<Void> {
        return self.client.punsubscribe(from: patterns)
    }
}

enum _QueuesRedisError: Error {
    case missingJob
    case invalidIdentifier(RESPValue)
}

extension JobIdentifier {
    var key: String {
        "job:\(self.string)"
    }
}

extension _QueuesRedisQueue: Queue {
    func get(_ id: JobIdentifier) -> EventLoopFuture<JobData> {
        self.client.get(RedisKey(id.key), asJSON: JobData.self)
            .unwrap(or: _QueuesRedisError.missingJob)
    }

    func set(_ id: JobIdentifier, to storage: JobData) -> EventLoopFuture<Void> {
        self.client.set(RedisKey(id.key), toJSON: storage)
    }

    func clear(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        // Remove from sorted set (processing queue) using ZREM
        self.client.send(
            command: "ZREM",
            with: [
                .init(from: self.processingKey),
                .init(from: id.string)
            ]
        ).flatMap { _ in
            // Delete job data
            self.client.delete(RedisKey(id.key))
        }.map { _ in }
    }

    func push(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        // Push job ID back to main queue
        self.client.lpush(RedisKey(id.string), into: RedisKey(self.key))
            .flatMap { _ in
                // Remove from sorted set (processing queue) using ZREM
                self.client.send(
                    command: "ZREM",
                    with: [
                        .init(from: self.processingKey),
                        .init(from: id.string)
                    ]
                )
            }
            .map { _ in }
    }

    func pop() -> EventLoopFuture<JobIdentifier?> {
        // Atomically pop from main queue and add to sorted set with timestamp using Lua script
        // This ensures atomicity: either both operations succeed or both fail
        let timestamp = Date().timeIntervalSince1970
        let timestampString = String(timestamp)

        // Lua script: atomically RPOP from main queue and ZADD to sorted set with timestamp
        // In Redis Lua, RPOP returns false (nil) if list is empty
        let luaScript = """
            local jobId = redis.call('RPOP', KEYS[1])
            if not jobId then
                return nil
            end
            redis.call('ZADD', KEYS[2], ARGV[1], jobId)
            return jobId
        """

        return self.client.send(
            command: "EVAL",
            with: [
                .init(from: luaScript),
                .init(from: "2"),  // Number of keys
                .init(from: self.key),  // KEYS[1] - main queue
                .init(from: self.processingKey),  // KEYS[2] - processing sorted set
                .init(from: timestampString)  // ARGV[1] - timestamp
            ]
        ).flatMapThrowing { redisData in
            guard !redisData.isNull else {
                return nil
            }
            guard let idString = redisData.string else {
                throw _QueuesRedisError.invalidIdentifier(redisData)
            }
            return JobIdentifier(string: idString)
        }
    }

    var processingKey: String {
        self.key + "-processing"
    }

    func recoverStaleJobs() -> EventLoopFuture<Int> {
        let staleTimeout = self.context.configuration.staleJobTimeout
        let staleTimeoutSeconds = Double(staleTimeout.nanoseconds) / 1_000_000_000.0
        let cutoffTime = Date().addingTimeInterval(-staleTimeoutSeconds)
        let cutoffTimestamp = cutoffTime.timeIntervalSince1970

        var logger = self.context.logger
        logger[metadataKey: "queue"] = "\(self.queueName.string)"
        logger.info("Recovering stale jobs", metadata: [
            "stale-timeout": "\(Int(staleTimeoutSeconds))s",
            "cutoff-timestamp": "\(cutoffTimestamp)"
        ])

        // Query stale jobs from sorted set using ZRANGEBYSCORE
        // ZRANGEBYSCORE key min max WITHSCORES
        // Returns jobs with scores (timestamps) <= cutoffTimestamp
        logger.trace("Querying stale jobs from Redis", metadata: [
            "processing-key": "\(self.processingKey)",
            "cutoff-timestamp": "\(cutoffTimestamp)"
        ])

        return self.client.send(
            command: "ZRANGEBYSCORE",
            with: [
                .init(from: self.processingKey),
                .init(from: "0"),  // min: 0 (all jobs)
                .init(from: String(cutoffTimestamp)),  // max: cutoff timestamp
                .init(from: "WITHSCORES")
            ]
        ).flatMap { redisData -> EventLoopFuture<Int> in
            guard let array = redisData.array else {
                return self.context.eventLoop.makeSucceededFuture(0)
            }

            // Array contains: [jobId1, timestamp1, jobId2, timestamp2, ...]
            // We need to extract job IDs (even indices)
            var jobIds: [String] = []
            for (index, value) in array.enumerated() {
                if index % 2 == 0, let jobId = value.string {
                    jobIds.append(jobId)
                }
            }

            guard !jobIds.isEmpty else {
                logger.trace("No stale jobs found")
                return self.context.eventLoop.makeSucceededFuture(0)
            }

            logger.warning("Found stale jobs", metadata: ["count": "\(jobIds.count)"])

            // Requeue each stale job
            var futures: [EventLoopFuture<Int>] = []  // Return 1 if recovered, 0 if removed

            for jobIdString in jobIds {
                let jobId = JobIdentifier(string: jobIdString)
                logger[metadataKey: "job-id"] = "\(jobIdString)"

                // Check if job data exists first (before atomic recovery)
                let future = self.get(jobId).flatMap { jobData -> EventLoopFuture<Int> in
                    // Job data exists - atomically move from processing queue to main queue
                    logger.info("Recovering stale job", metadata: ["job-name": "\(jobData.jobName)"])

                    // Atomic recovery: ZREM from sorted set + LPUSH to main queue using Lua script
                    let luaScript = """
                        -- Remove from processing sorted set
                        local removed = redis.call('ZREM', KEYS[1], ARGV[1])
                        if removed > 0 then
                            -- If removed, push to main queue
                            redis.call('LPUSH', KEYS[2], ARGV[1])
                            return 1
                        end
                        return 0
                    """

                    return self.client.send(
                        command: "EVAL",
                        with: [
                            .init(from: luaScript),
                            .init(from: "2"),  // Number of keys
                            .init(from: self.processingKey),  // KEYS[1] - processing sorted set
                            .init(from: self.key),  // KEYS[2] - main queue
                            .init(from: jobIdString)  // ARGV[1] - job ID
                        ]
                    ).flatMapThrowing { result -> Int in
                        // Result is 1 if successfully moved, 0 if not found in processing queue
                        // RESPValue may return integer or string representation
                        let moved: Int
                        if let intValue = result.int {
                            moved = intValue
                        } else if let stringValue = result.string, let parsedInt = Int(stringValue) {
                            moved = parsedInt
                        } else {
                            logger.warning("Unexpected response format during recovery", metadata: ["result": "\(result)"])
                            return 0
                        }

                        guard moved > 0 else {
                            logger.warning("Stale job not found in processing queue during recovery")
                            return 0
                        }
                        return 1  // Successfully recovered
                    }
                }.flatMapError { error -> EventLoopFuture<Int> in
                    // Job data missing - just remove from processing queue
                    logger.error("Job data missing for stale job, removing from processing queue", metadata: ["error": "\(String(reflecting: error))"])

                    return self.client.send(
                        command: "ZREM",
                        with: [
                            .init(from: self.processingKey),
                            .init(from: jobIdString)
                        ]
                    ).map { _ in
                        0  // Removed (not recovered)
                    }
                }

                futures.append(future)
            }

            // Wait for all recoveries to complete
            return EventLoopFuture.whenAllSucceed(futures, on: self.context.eventLoop).map { results in
                let recovered = results.reduce(0, +)
                let removed = jobIds.count - recovered
                logger.info("Recovery complete", metadata: ["recovered": "\(recovered)", "removed": "\(removed)"])
                return recovered
            }
        }
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}

extension RedisClient {
    func get<D>(_ key: RedisKey, asJSON type: D.Type) -> EventLoopFuture<D?> where D: Decodable {
        return get(key, as: Data.self).flatMapThrowing { data in
            return try data.flatMap { data in
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .secondsSince1970
                return try decoder.decode(D.self, from: data)
            }
        }
    }

    func set<E>(_ key: RedisKey, toJSON entity: E) -> EventLoopFuture<Void> where E: Encodable {
        do {
            let encoder = JSONEncoder()
            encoder.dateEncodingStrategy = .secondsSince1970
            return try set(key, to: encoder.encode(entity))
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
