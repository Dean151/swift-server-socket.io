#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import NIOCore
import NIOPosix
@preconcurrency import RediStack
import SocketIO

private struct RedisRecoveryEnvelope: Codable {
    let session: ConnectionStateRecoverySession
}

/// A Redis-backed implementation of ``SocketIOConnectionStateRecoveryStore``.
public actor RedisConnectionStateRecoveryStore: SocketIOConnectionStateRecoveryStore {
    private let configuration: RedisClusterCoordinatorConfiguration
    private var group: MultiThreadedEventLoopGroup?
    private var connection: RedisConnection?

    /// Creates a Redis-backed recovery store.
    public init(configuration: RedisClusterCoordinatorConfiguration) {
        self.configuration = configuration
    }

    deinit {
        let group = self.group
        let connection = self.connection
        if let connection {
            _ = connection.close()
        }
        if let group {
            try? group.syncShutdownGracefully()
        }
    }

    public func saveSession(
        _ session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws {
        let connection = try await redisConnection()
        let encoded = try encode(RedisRecoveryEnvelope(session: session))
        _ = try await connection.sadd(session.privateSessionID, to: namespaceKey(session.namespace)).get()
        try await connection.setex(
            sessionKey(session.privateSessionID),
            to: encoded,
            expirationInSeconds: expirationSeconds(from: maxDisconnectionDuration)
        ).get()
    }

    public func session(forPrivateSessionID privateSessionID: String) async throws -> ConnectionStateRecoverySession? {
        let connection = try await redisConnection()
        guard let encoded = try await connection.get(sessionKey(privateSessionID)).get().string else {
            return nil
        }
        return try decode(RedisRecoveryEnvelope.self, from: encoded).session
    }

    public func removeSession(forPrivateSessionID privateSessionID: String) async throws {
        let connection = try await redisConnection()
        if let session = try await session(forPrivateSessionID: privateSessionID) {
            _ = try await connection.srem(privateSessionID, from: namespaceKey(session.namespace)).get()
        }
        _ = try await connection.delete(
            sessionKey(privateSessionID),
            packetsKey(privateSessionID)
        ).get()
    }

    public func disconnectedSessions(in namespace: String) async throws -> [ConnectionStateRecoverySession] {
        let connection = try await redisConnection()
        let privateSessionIDs = try await connection.smembers(of: namespaceKey(namespace), as: String.self).get().compactMap { $0 }
        var sessions: [ConnectionStateRecoverySession] = []
        var stalePrivateSessionIDs: [String] = []

        for privateSessionID in privateSessionIDs {
            if let session = try await session(forPrivateSessionID: privateSessionID) {
                sessions.append(session)
            } else {
                stalePrivateSessionIDs.append(privateSessionID)
            }
        }

        if !stalePrivateSessionIDs.isEmpty {
            for privateSessionID in stalePrivateSessionIDs {
                _ = try? await connection.srem(privateSessionID, from: namespaceKey(namespace)).get()
            }
        }

        return sessions.sorted { $0.socketID < $1.socketID }
    }

    public func appendPacket(
        _ packet: ConnectionStateRecoveryPacket,
        updating session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws {
        let connection = try await redisConnection()
        let encodedSession = try encode(RedisRecoveryEnvelope(session: session))
        let encodedPacket = try encode(packet)
        let expirationSeconds = expirationSeconds(from: maxDisconnectionDuration)

        _ = try await connection.sadd(session.privateSessionID, to: namespaceKey(session.namespace)).get()
        try await connection.setex(
            sessionKey(session.privateSessionID),
            to: encodedSession,
            expirationInSeconds: expirationSeconds
        ).get()
        _ = try await connection.rpush(encodedPacket, into: packetsKey(session.privateSessionID)).get()
        _ = try await connection.expire(
            packetsKey(session.privateSessionID),
            after: .seconds(Int64(expirationSeconds))
        ).get()
    }

    public func queuedPackets(
        forPrivateSessionID privateSessionID: String,
        afterOffset offset: String?
    ) async throws -> [ConnectionStateRecoveryPacket] {
        let connection = try await redisConnection()
        let values = try await connection.lrange(from: packetsKey(privateSessionID), firstIndex: 0, lastIndex: -1).get()
        let packets = try values.compactMap { value -> ConnectionStateRecoveryPacket? in
            guard let encoded = value.string else { return nil }
            return try decode(ConnectionStateRecoveryPacket.self, from: encoded)
        }
        guard let offset, let numericOffset = Int(offset) else {
            return packets
        }
        return packets.filter { (Int($0.offset) ?? .min) > numericOffset }
    }

    private func redisConnection() async throws -> RedisConnection {
        if let connection {
            return connection
        }

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        do {
            let connection = try await RedisConnection.make(
                configuration: configuration.connection,
                boundEventLoop: group.next()
            ).get()
            self.group = group
            self.connection = connection
            return connection
        } catch {
            try? await shutdown(group: group)
            throw error
        }
    }

    private func encode<T: Encodable>(_ value: T) throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(value)
        guard let string = String(data: data, encoding: .utf8) else {
            throw SocketIOCodingError.invalidJSON
        }
        return string
    }

    private func decode<T: Decodable>(_ type: T.Type, from string: String) throws -> T {
        guard let data = string.data(using: .utf8) else {
            throw SocketIOCodingError.invalidJSON
        }
        return try JSONDecoder().decode(T.self, from: data)
    }

    private func expirationSeconds(from duration: Duration) -> Int {
        max(
            1,
            Int(duration.components.seconds)
                + Int(duration.components.attoseconds / 1_000_000_000_000_000_000)
        )
    }

    private func namespaceKey(_ namespace: String) -> RedisKey {
        RedisKey("\(configuration.channelPrefix)#recovery#namespace#\(namespace)")
    }

    private func sessionKey(_ privateSessionID: String) -> RedisKey {
        RedisKey("\(configuration.channelPrefix)#recovery#session#\(privateSessionID)")
    }

    private func packetsKey(_ privateSessionID: String) -> RedisKey {
        RedisKey("\(configuration.channelPrefix)#recovery#packets#\(privateSessionID)")
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
