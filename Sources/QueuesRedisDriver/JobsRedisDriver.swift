import Queues
import Redis
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
        self.pool = RedisConnectionPool(
            configuration: .init(
                initialServerConnectionAddresses: config.serverAddresses,
                maximumConnectionCount: config.pool.maximumConnectionCount,
                connectionFactoryConfiguration: .init(
                    connectionInitialDatabase: config.database,
                    connectionPassword: config.password,
                    connectionDefaultLogger: logger,
                    tcpClient: nil
                ),
                minimumConnectionCount: config.pool.minimumConnectionCount,
                connectionBackoffFactor: config.pool.connectionBackoffFactor,
                initialConnectionBackoffDelay: config.pool.initialConnectionBackoffDelay,
                connectionRetryTimeout: config.pool.connectionRetryTimeout,
                poolDefaultLogger: logger
            ),
            boundEventLoop: eventLoopGroup.next()
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
        self.lrem(RedisKey(id.string), from: RedisKey(self.processingKey)).flatMap { _ in
            self.client.delete(RedisKey(id.key))
        }.map { _ in }
    }
    
    func push(_ id: JobIdentifier) -> EventLoopFuture<Void> {
        self.client.lpush(RedisKey(id.string), into: RedisKey(self.key))
            .flatMap { _ in self.lrem(RedisKey(id.string), from: RedisKey(self.processingKey)).transform(to: ()) }
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
