//
//  MIT License
//
//  Copyright (c) 2026 Thomas Durand
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import NIOCore
import NIOPosix
@preconcurrency import RediStack
import SocketIO

/// Configuration for a Redis-backed Socket.IO cluster coordinator.
public struct RedisClusterCoordinatorConfiguration: Sendable {
    /// The Redis connection configuration used by the coordinator.
    public let connection: RedisConnection.Configuration
    /// The prefix applied to Redis Pub/Sub channels and membership keys.
    public let channelPrefix: String

    /// Creates a Redis coordinator configuration.
    ///
    /// - Parameters:
    ///   - connection: The Redis connection configuration used by the coordinator.
    ///   - channelPrefix: The prefix applied to Redis Pub/Sub channels and membership keys.
    public init(
        connection: RedisConnection.Configuration,
        channelPrefix: String = "socket.io"
    ) {
        self.connection = connection
        self.channelPrefix = channelPrefix
    }

    /// Creates a Redis coordinator configuration from a Redis URL.
    ///
    /// - Parameters:
    ///   - url: The Redis URL.
    ///   - channelPrefix: The prefix applied to Redis Pub/Sub channels and membership keys.
    public init(
        url: String,
        channelPrefix: String = "socket.io"
    ) throws {
        self.init(
            connection: try .init(url: url),
            channelPrefix: channelPrefix
        )
    }

    /// Creates a Redis coordinator configuration from a hostname and port.
    ///
    /// - Parameters:
    ///   - hostname: The Redis hostname.
    ///   - port: The Redis port.
    ///   - password: The optional Redis password.
    ///   - initialDatabase: The optional Redis database index.
    ///   - channelPrefix: The prefix applied to Redis Pub/Sub channels and membership keys.
    public init(
        hostname: String,
        port: Int = RedisConnection.Configuration.defaultPort,
        password: String? = nil,
        initialDatabase: Int? = nil,
        channelPrefix: String = "socket.io"
    ) throws {
        self.init(
            connection: try .init(
                hostname: hostname,
                port: port,
                password: password,
                initialDatabase: initialDatabase
            ),
            channelPrefix: channelPrefix
        )
    }
}

/// A Redis-backed implementation of ``SocketIOClusterCoordinator``.
public actor RedisClusterCoordinator: SocketIOClusterCoordinator {
    public let nodeID: String

    private let configuration: RedisClusterCoordinatorConfiguration
    private var group: MultiThreadedEventLoopGroup?
    private var commandConnection: RedisConnection?
    private var subscriptionConnection: RedisConnection?
    private var started = false

    /// Creates a coordinator for the given node identifier.
    ///
    /// - Parameters:
    ///   - nodeID: The unique node identifier for this process.
    ///   - configuration: The Redis coordinator configuration.
    public init(
        nodeID: String,
        configuration: RedisClusterCoordinatorConfiguration
    ) {
        self.nodeID = nodeID
        self.configuration = configuration
    }

    public func start(
        onCommand: @escaping @Sendable (Data) async -> Void,
        onResponse: @escaping @Sendable (Data) async -> Void
    ) async throws {
        guard !started else { return }

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let commandConnection = try await RedisConnection.make(
            configuration: configuration.connection,
            boundEventLoop: group.next()
        ).get()
        let subscriptionConnection = try await RedisConnection.make(
            configuration: configuration.connection,
            boundEventLoop: group.next()
        ).get()

        do {
            _ = try await commandConnection.sadd(nodeID, to: membersKey).get()
            try await subscriptionConnection.subscribe(
                to: [commandChannel],
                messageReceiver: { _, message in
                    guard let value = message.string, let data = value.data(using: .utf8) else { return }
                    Task {
                        await onCommand(data)
                    }
                }
            ).get()
            try await subscriptionConnection.subscribe(
                to: [responseChannel(for: nodeID)],
                messageReceiver: { _, message in
                    guard let value = message.string, let data = value.data(using: .utf8) else { return }
                    Task {
                        await onResponse(data)
                    }
                }
            ).get()
        } catch {
            _ = commandConnection.close()
            _ = subscriptionConnection.close()
            try? await shutdown(group: group)
            throw error
        }

        self.group = group
        self.commandConnection = commandConnection
        self.subscriptionConnection = subscriptionConnection
        self.started = true
    }

    public func stop() async {
        guard started else { return }

        if let commandConnection {
            _ = try? await commandConnection.srem(nodeID, from: membersKey).get()
        }
        if let subscriptionConnection {
            _ = try? await subscriptionConnection.unsubscribe(from: [commandChannel, responseChannel(for: nodeID)]).get()
        }
        if let subscriptionConnection {
            _ = try? await subscriptionConnection.close().get()
        }
        if let commandConnection {
            _ = try? await commandConnection.close().get()
        }
        if let group {
            try? await shutdown(group: group)
        }

        self.group = nil
        self.commandConnection = nil
        self.subscriptionConnection = nil
        self.started = false
    }

    public func otherNodeIDs() async throws -> Set<String> {
        guard let commandConnection else { return [] }
        let members = try await commandConnection.smembers(of: membersKey, as: String.self).get()
        return Set(members.compactMap { $0 }.filter { $0 != nodeID })
    }

    public func publishCommand(_ data: Data) async throws {
        guard let commandConnection else { return }
        let message = String(decoding: data, as: UTF8.self)
        _ = try await commandConnection.publish(message, to: commandChannel).get()
    }

    public func publishResponse(_ data: Data, to nodeID: String) async throws {
        guard let commandConnection else { return }
        let message = String(decoding: data, as: UTF8.self)
        _ = try await commandConnection.publish(message, to: responseChannel(for: nodeID)).get()
    }

    private var commandChannel: RedisChannelName {
        RedisChannelName("\(configuration.channelPrefix)#commands")
    }

    private var membersKey: RedisKey {
        RedisKey("\(configuration.channelPrefix)#nodes")
    }

    private func responseChannel(for nodeID: String) -> RedisChannelName {
        RedisChannelName("\(configuration.channelPrefix)#response#\(nodeID)")
    }

    private func shutdown(group: EventLoopGroup) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            group.shutdownGracefully { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }
}

extension RedisClusterCoordinator: SocketIOConnectionStateRecoveryStoreFactory {
    nonisolated public func makeConnectionStateRecoveryStore() -> any SocketIOConnectionStateRecoveryStore {
        RedisConnectionStateRecoveryStore(configuration: configuration)
    }
}
