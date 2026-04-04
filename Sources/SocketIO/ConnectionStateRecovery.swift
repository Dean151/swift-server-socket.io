#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Settings that control Socket.IO connection state recovery.
public struct ConnectionStateRecovery: Sendable, Equatable {
    /// How long disconnected session state and queued packets remain recoverable.
    public let maxDisconnectionDuration: Duration
    /// Whether namespace middlewares are skipped when a session is recovered successfully.
    public let skipMiddlewares: Bool

    /// Creates connection state recovery settings.
    ///
    /// - Parameters:
    ///   - maxDisconnectionDuration: How long disconnected session state remains recoverable.
    ///   - skipMiddlewares: Whether namespace middlewares are skipped on successful recovery.
    public init(
        maxDisconnectionDuration: Duration = .seconds(120),
        skipMiddlewares: Bool = true
    ) {
        self.maxDisconnectionDuration = maxDisconnectionDuration
        self.skipMiddlewares = skipMiddlewares
    }
}

/// A stored, recoverable namespace session snapshot.
public struct ConnectionStateRecoverySession: Codable, Sendable, Equatable {
    /// The private recovery identifier shared with the client.
    public let privateSessionID: String
    /// The namespace the recoverable socket belongs to.
    public let namespace: String
    /// The public Socket.IO socket identifier.
    public let socketID: String
    /// The rooms that should be restored when the session reconnects.
    public let rooms: [String]
    /// The server-side socket data that should be restored on reconnect.
    public let data: [String: SocketIOValue]
    /// The next replay packet offset to allocate.
    public let nextPacketOffset: Int

    /// Creates a recoverable session snapshot.
    public init(
        privateSessionID: String,
        namespace: String,
        socketID: String,
        rooms: [String],
        data: [String: SocketIOValue],
        nextPacketOffset: Int
    ) {
        self.privateSessionID = privateSessionID
        self.namespace = namespace
        self.socketID = socketID
        self.rooms = rooms
        self.data = data
        self.nextPacketOffset = nextPacketOffset
    }
}

/// A queued event packet that should be replayed during recovery.
public struct ConnectionStateRecoveryPacket: Codable, Sendable, Equatable {
    /// The namespace the packet belongs to.
    public let namespace: String
    /// The packet payload items, including the event name and recovery offset.
    public let items: [SocketIOValue]
    /// The replay offset assigned to the packet.
    public let offset: String

    /// Creates a queued replay packet.
    public init(namespace: String, items: [SocketIOValue], offset: String) {
        self.namespace = namespace
        self.items = items
        self.offset = offset
    }
}

/// Persists recoverable Socket.IO sessions and queued replay packets.
public protocol SocketIOConnectionStateRecoveryStore: Sendable {
    /// Saves or updates a recoverable session snapshot.
    func saveSession(
        _ session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws

    /// Returns the recoverable session for the given private identifier, if still available.
    func session(forPrivateSessionID privateSessionID: String) async throws -> ConnectionStateRecoverySession?

    /// Removes the recoverable session and any queued packets for the given private identifier.
    func removeSession(forPrivateSessionID privateSessionID: String) async throws

    /// Returns every disconnected recoverable session currently tracked for a namespace.
    func disconnectedSessions(in namespace: String) async throws -> [ConnectionStateRecoverySession]

    /// Appends a queued replay packet while updating the backing session snapshot.
    func appendPacket(
        _ packet: ConnectionStateRecoveryPacket,
        updating session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws

    /// Returns queued replay packets that occurred after the client's last processed offset.
    func queuedPackets(
        forPrivateSessionID privateSessionID: String,
        afterOffset offset: String?
    ) async throws -> [ConnectionStateRecoveryPacket]
}

/// Lets a cluster coordinator supply a shared recovery store for all nodes.
public protocol SocketIOConnectionStateRecoveryStoreFactory: Sendable {
    /// Returns the recovery store that should back connection state recovery.
    func makeConnectionStateRecoveryStore() -> any SocketIOConnectionStateRecoveryStore
}

/// The default in-memory recovery store.
public actor InMemoryConnectionStateRecoveryStore: SocketIOConnectionStateRecoveryStore {
    private struct SessionEnvelope: Sendable {
        var session: ConnectionStateRecoverySession
        var expiresAt: Date
    }

    private var sessions: [String: SessionEnvelope] = [:]
    private var packets: [String: [ConnectionStateRecoveryPacket]] = [:]
    private var namespaceIndex: [String: Set<String>] = [:]

    /// Creates an empty in-memory recovery store.
    public init() {}

    public func saveSession(
        _ session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws {
        pruneExpiredSessions()
        sessions[session.privateSessionID] = .init(
            session: session,
            expiresAt: expiryDate(after: maxDisconnectionDuration)
        )
        namespaceIndex[session.namespace, default: []].insert(session.privateSessionID)
        packets[session.privateSessionID, default: []] = packets[session.privateSessionID] ?? []
    }

    public func session(forPrivateSessionID privateSessionID: String) async throws -> ConnectionStateRecoverySession? {
        pruneExpiredSessions()
        return sessions[privateSessionID]?.session
    }

    public func removeSession(forPrivateSessionID privateSessionID: String) async throws {
        removeSessionSynchronously(forPrivateSessionID: privateSessionID)
    }

    public func disconnectedSessions(in namespace: String) async throws -> [ConnectionStateRecoverySession] {
        pruneExpiredSessions()
        return (namespaceIndex[namespace] ?? [])
            .compactMap { sessions[$0]?.session }
            .sorted { $0.socketID < $1.socketID }
    }

    public func appendPacket(
        _ packet: ConnectionStateRecoveryPacket,
        updating session: ConnectionStateRecoverySession,
        maxDisconnectionDuration: Duration
    ) async throws {
        pruneExpiredSessions()
        guard sessions[session.privateSessionID] != nil else { return }
        sessions[session.privateSessionID] = .init(
            session: session,
            expiresAt: expiryDate(after: maxDisconnectionDuration)
        )
        namespaceIndex[session.namespace, default: []].insert(session.privateSessionID)
        packets[session.privateSessionID, default: []].append(packet)
    }

    public func queuedPackets(
        forPrivateSessionID privateSessionID: String,
        afterOffset offset: String?
    ) async throws -> [ConnectionStateRecoveryPacket] {
        pruneExpiredSessions()
        let queued = packets[privateSessionID] ?? []
        guard let offset, let numericOffset = Int(offset) else { return queued }
        return queued.filter { (Int($0.offset) ?? .min) > numericOffset }
    }

    private func pruneExpiredSessions() {
        let now = Date()
        for (privateSessionID, envelope) in sessions where envelope.expiresAt <= now {
            removeSessionSynchronously(forPrivateSessionID: privateSessionID)
        }
    }

    private func removeSessionSynchronously(forPrivateSessionID privateSessionID: String) {
        if let namespace = sessions.removeValue(forKey: privateSessionID)?.session.namespace {
            namespaceIndex[namespace]?.remove(privateSessionID)
            if namespaceIndex[namespace]?.isEmpty == true {
                namespaceIndex.removeValue(forKey: namespace)
            }
        }
        packets.removeValue(forKey: privateSessionID)
    }

    private func expiryDate(after duration: Duration) -> Date {
        let interval =
            Double(duration.components.seconds) +
            Double(duration.components.attoseconds) / 1_000_000_000_000_000_000
        return Date().addingTimeInterval(max(0, interval))
    }
}
