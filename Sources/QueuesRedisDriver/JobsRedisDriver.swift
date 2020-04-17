import Queues
import RedisKit
import NIO
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
        guard let configuration = RedisConfiguration(url: url) else {
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
    let pool: EventLoopGroupConnectionPool<RedisConnectionSource>
    
    /// Creates the RedisQueuesDriver
    /// - Parameters:
    ///   - configuration: The `RedisConfiguration` to boot the driver
    ///   - eventLoopGroup: The `EventLoopGroup` to run the driver with
    public init(configuration: RedisConfiguration, on eventLoopGroup: EventLoopGroup) {
        let logger = Logger(label: "codes.vapor.redis")
        self.pool = .init(
            source: .init(configuration: configuration, logger: logger),
            maxConnectionsPerEventLoop: 1,
            logger: logger,
            on: eventLoopGroup
        )
    }
    
    /// Shuts down the driver
    public func shutdown() {
        self.pool.shutdown()
    }
}

extension RedisQueuesDriver: QueuesDriver {
    
    /// Makes the `Queue`
    /// - Parameter context: Context to be passed to the creation of the `Queue`
    /// - Returns: The created `Queue`
    public func makeQueue(with context: QueueContext) -> Queue {
        _QueuesRedisQueue(
            client: pool.pool(for: context.eventLoop).client(),
            context: context
        )
    }
}

struct _QueuesRedisQueue {
    let client: RedisClient
    let context: QueueContext
}

extension _QueuesRedisQueue: RedisClient {    
    var isConnected: Bool {
        return true
    }
    
    func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.client.send(command: command, with: arguments)
    }
    
    func setLogging(to logger: Logger) {
        self.client.setLogging(to: logger)
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
        self.lrem(RedisKey(id.string), from: RedisKey(self.processingKey)).flatMap { _ in
            self.client.delete(RedisKey(id.key))
        }.map { _ in }
    }
    
    func push(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        self.client.lpush(RedisKey(id.string), into: RedisKey(self.key))
            .map { _ in }
    }
    
    func pop() -> EventLoopFuture<JobIdentifier?> {
        self.client.rpoplpush(from: RedisKey(self.key), to: RedisKey(self.processingKey)).flatMapThrowing { redisData in
            guard !redisData.isNull else {
                return nil
            }
            guard let id = redisData.string else {
                throw _QueuesRedisError.invalidIdentifier(redisData)
            }
            return .init(string: id)
        }
    }
    
    var processingKey: String {
        self.key + "-processing"
    }
}

struct DecoderUnwrapper: Decodable {
    let decoder: Decoder
    init(from decoder: Decoder) { self.decoder = decoder }
}
