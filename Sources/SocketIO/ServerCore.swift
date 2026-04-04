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

import EngineIO
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Hummingbird
import Mutex

private enum NamespaceKind: Sendable {
    case staticNamespace
    case dynamicChild(parentID: Int, childLifetimePolicy: DynamicChildLifetimePolicy)
}

private struct RegisteredNamespace: Sendable {
    let adapter: any SocketIOAdapter
    let kind: NamespaceKind
    var connectionHandlers: [Namespace.ConnectionHandler] = []
    var middlewares: [NamespaceMiddleware] = []
    var serverEventHandlers: [String: [ServerSideEventHandler]] = [:]
}

private struct RegisteredParentNamespace: @unchecked Sendable {
    let matcher: (String) -> Bool
    let options: ParentNamespaceOptions
    var connectionHandlers: [Namespace.ConnectionHandler] = []
    var middlewares: [NamespaceMiddleware] = []
    var serverEventHandlers: [String: [ServerSideEventHandler]] = [:]
}

private final class NamespaceRegistry: Sendable {
    private struct State {
        var namespaces: [String: RegisteredNamespace]
        var dynamicParents: [Int: RegisteredParentNamespace] = [:]
        var dynamicParentOrder: [Int] = []
        var nextDynamicParentID = 0
    }

    private let state: Mutex<State>
    private let configuration: ServerConfiguration

    init(configuration: ServerConfiguration) {
        self.configuration = configuration
        self.state = Mutex(.init(
            namespaces: [
                "/": .init(
                    adapter: configuration.adapterFactory("/"),
                    kind: .staticNamespace
                )
            ]
        ))
    }

    func registerNamespace(_ namespace: String) {
        let namespace = normalizeNamespace(namespace)
        state.withLock {
            guard $0.namespaces[namespace] == nil else { return }
            $0.namespaces[namespace] = .init(
                adapter: configuration.adapterFactory(namespace),
                kind: .staticNamespace
            )
        }
    }

    func addConnectionHandler(for namespace: String, handler: @escaping Namespace.ConnectionHandler) {
        let namespace = normalizeNamespace(namespace)
        state.withLock {
            if $0.namespaces[namespace] == nil {
                $0.namespaces[namespace] = .init(
                    adapter: configuration.adapterFactory(namespace),
                    kind: .staticNamespace
                )
            }
            var registeredNamespace = $0.namespaces[namespace]!
            registeredNamespace.connectionHandlers.append(handler)
            $0.namespaces[namespace] = registeredNamespace
        }
    }

    func addNamespaceMiddleware(for namespace: String, middleware: @escaping NamespaceMiddleware) {
        let namespace = normalizeNamespace(namespace)
        state.withLock {
            if $0.namespaces[namespace] == nil {
                $0.namespaces[namespace] = .init(
                    adapter: configuration.adapterFactory(namespace),
                    kind: .staticNamespace
                )
            }
            var registeredNamespace = $0.namespaces[namespace]!
            registeredNamespace.middlewares.append(middleware)
            $0.namespaces[namespace] = registeredNamespace
        }
    }

    func addServerEventHandler(
        for namespace: String,
        event: String,
        handler: @escaping ServerSideEventHandler
    ) {
        let namespace = normalizeNamespace(namespace)
        state.withLock {
            if $0.namespaces[namespace] == nil {
                $0.namespaces[namespace] = .init(
                    adapter: configuration.adapterFactory(namespace),
                    kind: .staticNamespace
                )
            }
            var registeredNamespace = $0.namespaces[namespace]!
            registeredNamespace.serverEventHandlers[event, default: []].append(handler)
            $0.namespaces[namespace] = registeredNamespace
        }
    }

    func registeredNamespace(for namespace: String) -> RegisteredNamespace? {
        let namespace = normalizeNamespace(namespace)
        return state.withLock { $0.namespaces[namespace] }
    }

    func registerDynamicParentNamespace(
        matcher: @escaping (String) -> Bool,
        options: ParentNamespaceOptions
    ) -> Int {
        state.withLock {
            let id = $0.nextDynamicParentID
            $0.nextDynamicParentID += 1
            $0.dynamicParents[id] = .init(matcher: matcher, options: options)
            $0.dynamicParentOrder.append(id)
            return id
        }
    }

    func addConnectionHandler(forParentNamespace id: Int, handler: @escaping Namespace.ConnectionHandler) {
        state.withLock {
            guard var parent = $0.dynamicParents[id] else { return }
            parent.connectionHandlers.append(handler)
            $0.dynamicParents[id] = parent
        }
    }

    func addNamespaceMiddleware(forParentNamespace id: Int, middleware: @escaping NamespaceMiddleware) {
        state.withLock {
            guard var parent = $0.dynamicParents[id] else { return }
            parent.middlewares.append(middleware)
            $0.dynamicParents[id] = parent
        }
    }

    func addServerEventHandler(
        forParentNamespace id: Int,
        event: String,
        handler: @escaping ServerSideEventHandler
    ) {
        state.withLock {
            guard var parent = $0.dynamicParents[id] else { return }
            parent.serverEventHandlers[event, default: []].append(handler)
            $0.dynamicParents[id] = parent
        }
    }

    func resolveNamespaceForConnection(_ namespace: String) -> RegisteredNamespace? {
        let namespace = normalizeNamespace(namespace)
        return state.withLock {
            if let registered = $0.namespaces[namespace] {
                return registered
            }

            for id in $0.dynamicParentOrder {
                guard let parent = $0.dynamicParents[id], parent.matcher(namespace) else {
                    continue
                }

                let child = RegisteredNamespace(
                    adapter: configuration.adapterFactory(namespace),
                    kind: .dynamicChild(
                        parentID: id,
                        childLifetimePolicy: parent.options.childLifetimePolicy
                    ),
                    connectionHandlers: parent.connectionHandlers,
                    middlewares: parent.middlewares,
                    serverEventHandlers: parent.serverEventHandlers
                )
                $0.namespaces[namespace] = child
                return child
            }

            return nil
        }
    }

    func namespaceKind(for namespace: String) -> NamespaceKind? {
        let namespace = normalizeNamespace(namespace)
        return state.withLock { $0.namespaces[namespace]?.kind }
    }

    func unregisterDynamicNamespace(_ namespace: String) {
        let namespace = normalizeNamespace(namespace)
        state.withLock {
            guard case .dynamicChild = $0.namespaces[namespace]?.kind else { return }
            $0.namespaces.removeValue(forKey: namespace)
        }
    }
}

private struct ClusterSocketIOValue: Codable, Sendable {
    private enum Kind: String, Codable {
        case string
        case number
        case bool
        case object
        case array
        case null
        case binary
    }

    private let kind: Kind
    private let string: String?
    private let number: Double?
    private let bool: Bool?
    private let object: [String: ClusterSocketIOValue]?
    private let array: [ClusterSocketIOValue]?
    private let binary: Data?

    init(_ value: SocketIOValue) {
        switch value {
        case .string(let value):
            self.kind = .string
            self.string = value
            self.number = nil
            self.bool = nil
            self.object = nil
            self.array = nil
            self.binary = nil
        case .number(let value):
            self.kind = .number
            self.string = nil
            self.number = value
            self.bool = nil
            self.object = nil
            self.array = nil
            self.binary = nil
        case .bool(let value):
            self.kind = .bool
            self.string = nil
            self.number = nil
            self.bool = value
            self.object = nil
            self.array = nil
            self.binary = nil
        case .object(let value):
            self.kind = .object
            self.string = nil
            self.number = nil
            self.bool = nil
            self.object = value.mapValues(Self.init)
            self.array = nil
            self.binary = nil
        case .array(let value):
            self.kind = .array
            self.string = nil
            self.number = nil
            self.bool = nil
            self.object = nil
            self.array = value.map(Self.init)
            self.binary = nil
        case .null:
            self.kind = .null
            self.string = nil
            self.number = nil
            self.bool = nil
            self.object = nil
            self.array = nil
            self.binary = nil
        case .binary(let value):
            self.kind = .binary
            self.string = nil
            self.number = nil
            self.bool = nil
            self.object = nil
            self.array = nil
            self.binary = Data(value.readableBytesView)
        }
    }

    var value: SocketIOValue {
        switch kind {
        case .string:
            .string(string ?? "")
        case .number:
            .number(number ?? 0)
        case .bool:
            .bool(bool ?? false)
        case .object:
            .object(object?.mapValues(\.value) ?? [:])
        case .array:
            .array(array?.map(\.value) ?? [])
        case .null:
            .null
        case .binary:
            .binary(.init(bytes: binary ?? .init()))
        }
    }
}

private struct ClusterSocketAckResponse: Codable, Sendable {
    let socketID: String
    let items: [ClusterSocketIOValue]
}

private struct RemoteSocketSnapshot: Codable, Sendable {
    let nodeID: String
    let id: String
    let namespace: String
    let handshake: RemoteSocketHandshake
    let rooms: [String]
    let data: [String: SocketIOValue]
}

private struct ClusterEnvelope: Codable, Sendable {
    let originNodeID: String
    let targetNodeID: String?
    let message: Message

    enum Message: Codable, Sendable {
        case broadcast(namespace: String, targets: BroadcastTargetsPayload, volatility: EmitVolatility, event: String, items: [ClusterSocketIOValue])
        case broadcastAckRequest(requestID: UUID, namespace: String, targets: BroadcastTargetsPayload, volatility: EmitVolatility, event: String, items: [ClusterSocketIOValue], timeoutMilliseconds: Int)
        case broadcastAckResponse(requestID: UUID, responses: [ClusterSocketAckResponse], receivedCount: Int, expectedCount: Int, timedOut: Bool)
        case fetchSocketsRequest(requestID: UUID, namespace: String, targets: BroadcastTargetsPayload)
        case fetchSocketsResponse(requestID: UUID, sockets: [RemoteSocketSnapshot])
        case joinRooms(namespace: String, targets: BroadcastTargetsPayload, rooms: [String])
        case leaveRooms(namespace: String, targets: BroadcastTargetsPayload, rooms: [String])
        case disconnectSockets(namespace: String, targets: BroadcastTargetsPayload, close: Bool)
        case remoteSocketEmit(namespace: String, socketID: String, event: String, items: [ClusterSocketIOValue])
        case remoteSocketJoin(namespace: String, socketID: String, rooms: [String])
        case remoteSocketLeave(namespace: String, socketID: String, rooms: [String])
        case remoteSocketDisconnect(namespace: String, socketID: String, close: Bool)
        case serverSideEmit(namespace: String, event: String, items: [ClusterSocketIOValue])
        case serverSideEmitAckRequest(requestID: UUID, namespace: String, event: String, items: [ClusterSocketIOValue], timeoutMilliseconds: Int)
        case serverSideEmitAckResponse(requestID: UUID, items: [ClusterSocketIOValue])
    }
}

private struct BroadcastTargetsPayload: Codable, Sendable {
    let includedRooms: [String]
    let excludedRooms: [String]
    let excludedSocketIDs: [String]
    let isLocalOnly: Bool

    init(_ targets: BroadcastTargets) {
        self.includedRooms = targets.includedRooms.sorted()
        self.excludedRooms = targets.excludedRooms.sorted()
        self.excludedSocketIDs = targets.excludedSocketIDs.sorted()
        self.isLocalOnly = targets.isLocalOnly
    }

    var targets: BroadcastTargets {
        .init(
            includedRooms: Set(includedRooms),
            excludedRooms: Set(excludedRooms),
            excludedSocketIDs: Set(excludedSocketIDs),
            isLocalOnly: isLocalOnly
        )
    }
}

private extension BroadcastTargets {
    var localOnly: BroadcastTargets {
        var copy = self
        copy.isLocalOnly = true
        return copy
    }
}

private struct LocalBroadcastAckOutcome: Sendable {
    let responses: [ClusterSocketAckResponse]
    let receivedCount: Int
    let expectedCount: Int
    let timedOut: Bool
}

private actor ClusterResponseAggregator<Response: Sendable> {
    private let expectedNodeIDs: Set<String>
    private var responses: [String: Response] = [:]
    private var continuation: CheckedContinuation<[(String, Response)], Never>?
    private var finished = false

    init(expectedNodeIDs: Set<String>) {
        self.expectedNodeIDs = expectedNodeIDs
    }

    func receive(from nodeID: String, response: Response) {
        guard !finished, expectedNodeIDs.contains(nodeID), responses[nodeID] == nil else { return }
        responses[nodeID] = response
        guard responses.count == expectedNodeIDs.count else { return }
        finished = true
        continuation?.resume(returning: orderedResponses())
        continuation = nil
    }

    func wait(timeout: Duration) async -> [(String, Response)] {
        guard !expectedNodeIDs.isEmpty else { return [] }
        if finished {
            return orderedResponses()
        }

        let timeoutTask = Task {
            do {
                try await Task.sleep(for: timeout)
            } catch {
                return
            }
            finishIfNeeded()
        }

        let result = await withCheckedContinuation { (continuation: CheckedContinuation<[(String, Response)], Never>) in
            if finished {
                continuation.resume(returning: orderedResponses())
            } else {
                self.continuation = continuation
            }
        }
        timeoutTask.cancel()
        return result
    }

    private func finishIfNeeded() {
        guard !finished else { return }
        finished = true
        continuation?.resume(returning: orderedResponses())
        continuation = nil
    }

    private func orderedResponses() -> [(String, Response)] {
        responses.keys.sorted().compactMap { nodeID in
            guard let response = responses[nodeID] else { return nil }
            return (nodeID, response)
        }
    }
}

actor ServerCore {
    private struct RecoveryReconnectContext {
        let privateSessionID: String
        let offset: String?
    }

    private struct ActiveConnectionRecoveryState: Sendable {
        let privateSessionID: String
        var nextPacketOffset: Int
    }

    private struct PendingBinaryState {
        let pendingPacket: PendingSocketIOPacket
        var attachments: [ByteBuffer] = []
    }

    private struct ConnectedNamespaceState {
        let socket: Socket
        let socketState: SocketState
        var recovery: ActiveConnectionRecoveryState?
        var nextAckID: Int = 0
        var pendingAcks: [Int: @Sendable ([SocketIOValue]) async -> Void] = [:]
        var ignoredAckIDs: Set<Int> = []
    }

    private struct ConnectionState {
        let connection: EngineIOConnection
        var connectTimeoutTask: Task<Void, Never>?
        var connectedNamespaces: [String: ConnectedNamespaceState] = [:]
        var pendingBinaryState: PendingBinaryState?
    }

    private struct NamespaceSocketKey: Hashable {
        let namespace: String
        let socketID: String
    }

    private struct BroadcastAckToken: Hashable {
        let connectionID: String
        let namespace: String
        let ackID: Int
    }

    private struct ConnectedTarget {
        let socketID: String
        let connectionID: String
        let connection: EngineIOConnection
    }

    private struct ClusterState {
        let coordinator: any SocketIOClusterCoordinator
        let requestTimeout: Duration
    }

    private let configuration: ServerConfiguration
    private let namespaceRegistry: NamespaceRegistry
    private let jsonEncoder = JSONEncoder()
    private let jsonDecoder = JSONDecoder()
    private let recoveryStore: (any SocketIOConnectionStateRecoveryStore)?
    private var clusterState: ClusterState?
    private var clusterStarted = false
    private var clusterStartupError: (any Error)?
    private var pendingFetchSocketsResponses: [UUID: ClusterResponseAggregator<[RemoteSocketSnapshot]>] = [:]
    private var pendingBroadcastAckResponses: [UUID: ClusterResponseAggregator<ClusterEnvelope.Message>] = [:]
    private var pendingServerAckResponses: [UUID: ClusterResponseAggregator<[ClusterSocketIOValue]>] = [:]
    private var connections: [String: ConnectionState] = [:]
    private var socketIndex: [NamespaceSocketKey: String] = [:]
    private var pendingTransportDisconnectReasons: [String: SocketDisconnectReason] = [:]

    init(configuration: ServerConfiguration) {
        self.configuration = configuration
        self.namespaceRegistry = NamespaceRegistry(configuration: configuration)
        if configuration.connectionStateRecovery != nil {
            if let explicitRecoveryStore = configuration.cluster.recoveryStore {
                self.recoveryStore = explicitRecoveryStore
            } else if let factory = configuration.clusterCoordinator as? any SocketIOConnectionStateRecoveryStoreFactory {
                self.recoveryStore = factory.makeConnectionStateRecoveryStore()
            } else {
                self.recoveryStore = InMemoryConnectionStateRecoveryStore()
            }
        } else {
            self.recoveryStore = nil
        }
        if let coordinator = configuration.clusterCoordinator {
            self.clusterState = .init(
                coordinator: coordinator,
                requestTimeout: configuration.clusterRequestTimeout
            )
        }
    }

    nonisolated func registerNamespace(_ namespace: String) {
        namespaceRegistry.registerNamespace(namespace)
    }

    nonisolated func addConnectionHandler(for namespace: String, handler: @escaping Namespace.ConnectionHandler) {
        namespaceRegistry.addConnectionHandler(for: namespace, handler: handler)
    }

    nonisolated func addNamespaceMiddleware(for namespace: String, middleware: @escaping NamespaceMiddleware) {
        namespaceRegistry.addNamespaceMiddleware(for: namespace, middleware: middleware)
    }

    nonisolated func addServerEventHandler(
        for namespace: String,
        event: String,
        handler: @escaping ServerSideEventHandler
    ) {
        namespaceRegistry.addServerEventHandler(for: namespace, event: event, handler: handler)
    }

    nonisolated func registerDynamicParentNamespace(
        options: ParentNamespaceOptions,
        matcher: @escaping (String) -> Bool
    ) -> Int {
        namespaceRegistry.registerDynamicParentNamespace(matcher: matcher, options: options)
    }

    nonisolated func addConnectionHandler(forParentNamespace id: Int, handler: @escaping Namespace.ConnectionHandler) {
        namespaceRegistry.addConnectionHandler(forParentNamespace: id, handler: handler)
    }

    nonisolated func addNamespaceMiddleware(forParentNamespace id: Int, middleware: @escaping NamespaceMiddleware) {
        namespaceRegistry.addNamespaceMiddleware(forParentNamespace: id, middleware: middleware)
    }

    nonisolated func addServerEventHandler(
        forParentNamespace id: Int,
        event: String,
        handler: @escaping ServerSideEventHandler
    ) {
        namespaceRegistry.addServerEventHandler(forParentNamespace: id, event: event, handler: handler)
    }

    func prepareForServerShutdown() {
        for connectionID in connections.keys {
            pendingTransportDisconnectReasons[connectionID] = .serverShuttingDown
        }
    }

    func transportConnected(_ connection: EngineIOConnection) {
        Task { [weak self] in
            await self?.startClusterIfNeeded()
        }
        var state = ConnectionState(connection: connection)
        state.connectTimeoutTask = Task { [weak self] in
            do {
                try await Task.sleep(for: self?.configuration.connectTimeout ?? .seconds(45))
            } catch {
                return
            }
            await self?.handleConnectTimeout(connectionID: connection.sid)
        }
        connections[connection.sid] = state
    }

    func transportClosed(_ connection: EngineIOConnection, reason: EngineIOCloseReason) async {
        guard let state = connections.removeValue(forKey: connection.sid) else { return }
        state.connectTimeoutTask?.cancel()
        let disconnectReason = pendingTransportDisconnectReasons.removeValue(forKey: connection.sid)
            ?? socketDisconnectReason(from: reason)
        for (namespace, namespaceState) in state.connectedNamespaces {
            await tearDownNamespace(
                namespace,
                namespaceState: namespaceState,
                reason: disconnectReason,
                notifyPeerOn: nil
            )
        }
    }

    func transportReceived(_ connection: EngineIOConnection, data: EngineIOData) async {
        guard var state = connections[connection.sid] else { return }

        do {
            if var pendingBinaryState = state.pendingBinaryState {
                guard case .binary(let buffer) = data else {
                    state.pendingBinaryState = nil
                    connections[connection.sid] = state
                    pendingTransportDisconnectReasons[connection.sid] = .parseError
                    await connection.close()
                    return
                }
                pendingBinaryState.attachments.append(buffer)
                if pendingBinaryState.attachments.count == pendingBinaryState.pendingPacket.expectedAttachments {
                    let packet = try pendingBinaryState.pendingPacket.complete(with: pendingBinaryState.attachments)
                    state.pendingBinaryState = nil
                    connections[connection.sid] = state
                    try await handle(packet, on: connection)
                } else {
                    state.pendingBinaryState = pendingBinaryState
                    connections[connection.sid] = state
                }
                return
            }

            guard case .text(let text) = data else {
                pendingTransportDisconnectReasons[connection.sid] = .parseError
                await connection.close()
                return
            }
            switch try SocketIOPacket.decode(from: text) {
            case .packet(let packet):
                connections[connection.sid] = state
                try await handle(packet, on: connection)
            case .pending(let pendingPacket):
                state.pendingBinaryState = PendingBinaryState(pendingPacket: pendingPacket)
                connections[connection.sid] = state
            }
        } catch {
            pendingTransportDisconnectReasons[connection.sid] = .parseError
            await connection.close()
        }
    }

    private func handleConnectTimeout(connectionID: String) async {
        guard let state = connections[connectionID], state.connectedNamespaces.isEmpty else { return }
        pendingTransportDisconnectReasons[connectionID] = .forcedServerClose
        await state.connection.close()
    }

    private func handle(_ packet: SocketIOPacket, on connection: EngineIOConnection) async throws {
        switch packet {
        case .connect(let namespace, let auth):
            try await handleConnect(namespace: namespace, auth: auth, on: connection)
        case .disconnect(let namespace):
            try await handleDisconnect(
                namespace: namespace,
                on: connection,
                reason: .clientNamespaceDisconnect,
                notifyPeer: false,
                closeTransport: false
            )
        case .event(let namespace, let items, let ackID):
            try await handleEvent(namespace: namespace, items: items, ackID: ackID, on: connection)
        case .ack(let namespace, let items, let ackID):
            try await handleAck(namespace: namespace, items: items, ackID: ackID, on: connection)
        case .connectError:
            throw SocketIOPacketDecodingError.invalidPacket("Client cannot send CONNECT_ERROR packets")
        }
    }

    private func extractRecoveryContext(from auth: SocketIOValue?) -> (RecoveryReconnectContext?, SocketIOValue?) {
        guard case .object(let object)? = auth else {
            return (nil, auth)
        }

        var sanitized = object
        let privateSessionID: String?
        if case .string(let value)? = sanitized.removeValue(forKey: "pid") {
            privateSessionID = value
        } else {
            privateSessionID = nil
        }

        let offset: String?
        if case .string(let value)? = sanitized.removeValue(forKey: "offset") {
            offset = value
        } else {
            offset = nil
        }

        let sanitizedAuth: SocketIOValue? = sanitized.isEmpty ? nil : .object(sanitized)
        guard let privateSessionID else {
            return (nil, sanitizedAuth)
        }
        return (.init(privateSessionID: privateSessionID, offset: offset), sanitizedAuth)
    }

    private func handleConnect(namespace: String, auth: SocketIOValue?, on connection: EngineIOConnection) async throws {
        let namespace = normalizeNamespace(namespace)
        guard let registeredNamespace = namespaceRegistry.resolveNamespaceForConnection(namespace) else {
            guard let state = connections[connection.sid] else { return }
            connections[connection.sid] = state
            _ = try await send(.connectError(namespace: namespace, data: .object(["message": .string("Invalid namespace")])), on: connection)
            return
        }
        guard var state = connections[connection.sid] else { return }
        guard state.connectedNamespaces[namespace] == nil else {
            throw SocketIOPacketDecodingError.invalidPacket("Namespace is already connected")
        }

        let (recoveryContext, sanitizedAuth) = extractRecoveryContext(from: auth)
        let recoveredSession = await restoreSession(
            from: recoveryContext,
            namespace: namespace
        )

        let authorization = try await configuration.authorizeNamespaceConnection(.init(
            engineSessionID: connection.sid,
            namespace: namespace,
            auth: sanitizedAuth,
            request: connection.request
        ))
        switch authorization {
        case .allow:
            let socketID = recoveredSession?.socketID ?? configuration.socketIDGenerator(connection.sid, namespace)
            let privateSessionID = recoveredSession?.privateSessionID ?? (configuration.connectionStateRecovery != nil ? UUID().uuidString : nil)
            let handshake = SocketHandshake(engineSessionID: connection.sid, request: connection.request, auth: sanitizedAuth)
            let socketState = SocketState(
                id: socketID,
                namespace: namespace,
                handshake: handshake,
                recovered: recoveredSession != nil,
                data: recoveredSession?.data ?? [:],
                emitOperation: { [weak self] volatility, event, items, ack in
                    await self?.emitEvent(
                        on: connection.sid,
                        socketID: socketID,
                        privateSessionID: privateSessionID,
                        namespace: namespace,
                        volatility: volatility,
                        event: event,
                        items: items,
                        ack: ack
                    )
                },
                disconnectOperation: { [weak self] close in
                    try? await self?.handleDisconnect(
                        namespace: namespace,
                        on: connection,
                        reason: .serverNamespaceDisconnect,
                        notifyPeer: true,
                        closeTransport: close
                    )
                }
            )
            let restoredRooms = Set((recoveredSession?.rooms ?? []).filter { $0 != socketID })
            let socket = Socket(
                id: socketID,
                namespace: namespace,
                handshake: handshake,
                state: socketState,
                joinOperation: { [weak self] room in
                    await self?.join(room: room, socketID: socketID, namespace: namespace)
                },
                leaveOperation: { [weak self] room in
                    await self?.leave(room: room, socketID: socketID, namespace: namespace)
                },
                leaveAllOperation: { [weak self] in
                    await self?.leaveAllRooms(for: socketID, namespace: namespace)
                },
                roomsOperation: { [weak self] in
                    await self?.rooms(for: socketID, namespace: namespace) ?? []
                },
                broadcastEmitOperation: { [weak self] targets, volatility, event, items in
                    await self?.broadcast(
                        in: namespace,
                        targets: targets,
                        volatility: volatility,
                        event: event,
                        items: items
                    )
                },
                broadcastEmitWithAckOperation: { [weak self] targets, volatility, event, items, timeout in
                    try await self?.broadcastExpectingAcks(
                        in: namespace,
                        targets: targets,
                        volatility: volatility,
                        event: event,
                        items: items,
                        timeout: timeout
                    ) ?? []
                },
                broadcastFetchSocketsOperation: { [weak self] targets in
                    await self?.fetchSockets(in: namespace, targets: targets) ?? []
                },
                broadcastSocketsJoinOperation: { [weak self] targets, rooms in
                    await self?.join(rooms: rooms, in: namespace, targets: targets)
                },
                broadcastSocketsLeaveOperation: { [weak self] targets, rooms in
                    await self?.leave(rooms: rooms, in: namespace, targets: targets)
                },
                broadcastDisconnectSocketsOperation: { [weak self] targets, close in
                    await self?.disconnectSockets(in: namespace, targets: targets, close: close)
                }
            )

            var connectedNamespaceState = ConnectedNamespaceState(
                socket: socket,
                socketState: socketState,
                recovery: privateSessionID.map {
                    .init(
                        privateSessionID: $0,
                        nextPacketOffset: recoveredSession?.nextPacketOffset ?? 0
                    )
                }
            )

            if recoveredSession != nil, configuration.connectionStateRecovery?.skipMiddlewares == false {
                state.connectedNamespaces[namespace] = connectedNamespaceState
                connections[connection.sid] = state
                socketIndex[.init(namespace: namespace, socketID: socketID)] = connection.sid
                await registeredNamespace.adapter.add(socketID: socketID, to: socketID)
                for room in restoredRooms {
                    await registeredNamespace.adapter.add(socketID: socketID, to: room)
                }
                do {
                    try await runNamespaceMiddlewares(registeredNamespace.middlewares, socket: socket)
                } catch {
                    socketIndex.removeValue(forKey: .init(namespace: namespace, socketID: socketID))
                    await registeredNamespace.adapter.remove(socketID: socketID)
                    state.connectedNamespaces.removeValue(forKey: namespace)
                    connections[connection.sid] = state
                    _ = try await send(.connectError(namespace: namespace, data: socketIOPayload(from: error)), on: connection)
                    return
                }
                guard let refreshedState = connections[connection.sid]?.connectedNamespaces[namespace] else { return }
                connectedNamespaceState = refreshedState
            } else if configuration.connectionStateRecovery?.skipMiddlewares != true || recoveredSession == nil {
                do {
                    try await runNamespaceMiddlewares(registeredNamespace.middlewares, socket: socket)
                } catch {
                    connections[connection.sid] = state
                    _ = try await send(.connectError(namespace: namespace, data: socketIOPayload(from: error)), on: connection)
                    return
                }
            }

            state.connectedNamespaces[namespace] = connectedNamespaceState
            state.connectTimeoutTask?.cancel()
            state.connectTimeoutTask = nil
            connections[connection.sid] = state
            socketIndex[.init(namespace: namespace, socketID: socketID)] = connection.sid
            await registeredNamespace.adapter.add(socketID: socketID, to: socketID)
            for room in restoredRooms {
                await registeredNamespace.adapter.add(socketID: socketID, to: room)
            }

            let recoveryPayload = makeConnectionPayload(socketID: socketID, privateSessionID: privateSessionID)
            _ = try await send(.connect(namespace: namespace, auth: recoveryPayload), on: connection)
            if let recoveredSession, let recoveryContext {
                let replayPackets = (try? await recoveryStore?.queuedPackets(
                    forPrivateSessionID: recoveredSession.privateSessionID,
                    afterOffset: recoveryContext.offset
                )) ?? []
                for replayPacket in replayPackets where replayPacket.namespace == namespace {
                    _ = try? await send(.event(namespace: namespace, items: replayPacket.items, ackID: nil), on: connection)
                }
                try? await recoveryStore?.removeSession(forPrivateSessionID: recoveredSession.privateSessionID)
            }
            for handler in registeredNamespace.connectionHandlers {
                await handler(socket)
            }
        case .deny(let data):
            connections[connection.sid] = state
            _ = try await send(.connectError(namespace: namespace, data: data), on: connection)
        }
    }

    private func makeConnectionPayload(socketID: String, privateSessionID: String?) -> SocketIOValue? {
        var payload: [String: SocketIOValue] = ["sid": .string(socketID)]
        if let privateSessionID {
            payload["pid"] = .string(privateSessionID)
        }
        return .object(payload)
    }

    private func restoreSession(
        from recoveryContext: RecoveryReconnectContext?,
        namespace: String
    ) async -> ConnectionStateRecoverySession? {
        guard let recoveryContext else { return nil }
        guard let recoveryStore else { return nil }
        guard let session = try? await recoveryStore.session(forPrivateSessionID: recoveryContext.privateSessionID) else {
            return nil
        }
        guard session.namespace == namespace else { return nil }
        return session
    }

    private func handleDisconnect(
        namespace: String,
        on connection: EngineIOConnection,
        reason: SocketDisconnectReason,
        notifyPeer: Bool,
        closeTransport: Bool = false
    ) async throws {
        let namespace = normalizeNamespace(namespace)
        guard var state = connections[connection.sid] else { return }
        guard let namespaceState = state.connectedNamespaces.removeValue(forKey: namespace) else {
            throw SocketIOPacketDecodingError.invalidPacket("Unknown namespace")
        }
        connections[connection.sid] = state
        if closeTransport {
            pendingTransportDisconnectReasons[connection.sid] = .forcedClose
        }
        await tearDownNamespace(
            namespace,
            namespaceState: namespaceState,
            reason: reason,
            notifyPeerOn: notifyPeer ? connection : nil
        )
        if closeTransport {
            await connection.close()
        }
    }

    private func handleEvent(
        namespace: String,
        items: [SocketIOValue],
        ackID: Int?,
        on connection: EngineIOConnection
    ) async throws {
        guard case .string = items.first else {
            throw SocketIOPacketDecodingError.invalidPacket("EVENT name must be a string")
        }
        let namespace = normalizeNamespace(namespace)
        guard let state = connections[connection.sid], let namespaceState = state.connectedNamespaces[namespace] else {
            throw SocketIOPacketDecodingError.invalidPacket("Namespace is not connected")
        }
        let ack = ackID.map { ackID in
            SocketAck { [weak self] items in
                await self?.acknowledgeEvent(on: connection.sid, namespace: namespace, ackID: ackID, items: items)
            }
        }
        do {
            try await namespaceState.socketState.handleIncomingEvent(arguments: items, ack: ack)
        } catch {
            await namespaceState.socketState.emitError(error)
        }
    }

    private func handleAck(namespace: String, items: [SocketIOValue], ackID: Int, on connection: EngineIOConnection) async throws {
        let namespace = normalizeNamespace(namespace)
        guard var state = connections[connection.sid], var namespaceState = state.connectedNamespaces[namespace] else {
            throw SocketIOPacketDecodingError.invalidPacket("Namespace is not connected")
        }
        if let ack = namespaceState.pendingAcks.removeValue(forKey: ackID) {
            state.connectedNamespaces[namespace] = namespaceState
            connections[connection.sid] = state
            await ack(items)
            return
        }
        if namespaceState.ignoredAckIDs.remove(ackID) != nil {
            state.connectedNamespaces[namespace] = namespaceState
            connections[connection.sid] = state
            return
        }
        throw SocketIOPacketDecodingError.invalidPacket("Unknown acknowledgement id")
    }

    private func emitEvent(
        on connectionID: String,
        socketID: String,
        privateSessionID: String?,
        namespace: String,
        volatility: EmitVolatility,
        event: String,
        items: [SocketIOValue],
        ack: (@Sendable ([SocketIOValue]) async -> Void)?
    ) async {
        guard var state = connections[connectionID], var namespaceState = state.connectedNamespaces[namespace] else {
            guard let privateSessionID else { return }
            await queueRecoverablePacket(
                forPrivateSessionID: privateSessionID,
                namespace: namespace,
                event: event,
                items: items
            )
            return
        }

        var arguments = [.string(event)] + items
        if ack == nil, volatility == .reliable, var recovery = namespaceState.recovery {
            let offset = String(recovery.nextPacketOffset)
            recovery.nextPacketOffset += 1
            namespaceState.recovery = recovery
            arguments.append(.string(offset))
        }

        let ackID: Int?
        if let ack {
            ackID = namespaceState.nextAckID
            namespaceState.nextAckID += 1
            namespaceState.pendingAcks[ackID!] = ack
        } else {
            ackID = nil
        }
        state.connectedNamespaces[namespace] = namespaceState
        connections[connectionID] = state
        let accepted = (try? await send(
            .event(namespace: namespace, items: arguments, ackID: ackID),
            on: state.connection,
            volatility: volatility
        )) ?? false
        guard !accepted, let ackID else { return }
        guard var currentState = connections[connectionID], var currentNamespaceState = currentState.connectedNamespaces[namespace] else { return }
        currentNamespaceState.pendingAcks.removeValue(forKey: ackID)
        currentState.connectedNamespaces[namespace] = currentNamespaceState
        connections[connectionID] = currentState
    }

    private func queueRecoverablePacket(
        forPrivateSessionID privateSessionID: String,
        namespace: String,
        event: String,
        items: [SocketIOValue]
    ) async {
        guard let recovery = configuration.connectionStateRecovery else { return }
        guard let recoveryStore else { return }
        guard let session = try? await recoveryStore.session(forPrivateSessionID: privateSessionID) else {
            return
        }
        guard session.namespace == namespace else { return }

        let offset = String(session.nextPacketOffset)
        let packet = ConnectionStateRecoveryPacket(
            namespace: namespace,
            items: [.string(event)] + items + [.string(offset)],
            offset: offset
        )
        let updatedSession = ConnectionStateRecoverySession(
            privateSessionID: session.privateSessionID,
            namespace: session.namespace,
            socketID: session.socketID,
            rooms: session.rooms,
            data: session.data,
            nextPacketOffset: session.nextPacketOffset + 1
        )
        try? await recoveryStore.appendPacket(
            packet,
            updating: updatedSession,
            maxDisconnectionDuration: recovery.maxDisconnectionDuration
        )
    }

    private func acknowledgeEvent(on connectionID: String, namespace: String, ackID: Int, items: [SocketIOValue]) async {
        guard let state = connections[connectionID] else { return }
        _ = try? await send(.ack(namespace: namespace, items: items, ackID: ackID), on: state.connection)
    }

    func join(room: String, socketID: String, namespace: String) async {
        let namespace = normalizeNamespace(namespace)
        guard
            socketIndex[.init(namespace: namespace, socketID: socketID)] != nil,
            let registeredNamespace = namespaceRegistry.registeredNamespace(for: namespace)
        else { return }
        await registeredNamespace.adapter.add(socketID: socketID, to: room)
    }

    func leave(room: String, socketID: String, namespace: String) async {
        let namespace = normalizeNamespace(namespace)
        guard
            socketIndex[.init(namespace: namespace, socketID: socketID)] != nil,
            let registeredNamespace = namespaceRegistry.registeredNamespace(for: namespace)
        else { return }
        guard room != socketID else { return }
        await registeredNamespace.adapter.remove(socketID: socketID, from: room)
    }

    func leaveAllRooms(for socketID: String, namespace: String) async {
        let namespace = normalizeNamespace(namespace)
        guard
            socketIndex[.init(namespace: namespace, socketID: socketID)] != nil,
            let adapter = namespaceRegistry.registeredNamespace(for: namespace)?.adapter
        else { return }
        await adapter.remove(socketID: socketID)
        await adapter.add(socketID: socketID, to: socketID)
    }

    func rooms(for socketID: String, namespace: String) async -> Set<String> {
        let namespace = normalizeNamespace(namespace)
        guard
            socketIndex[.init(namespace: namespace, socketID: socketID)] != nil,
            let registeredNamespace = namespaceRegistry.registeredNamespace(for: namespace)
        else { return [] }
        return await registeredNamespace.adapter.rooms(for: socketID)
    }

    func join(rooms: [String], in namespace: String, targets: BroadcastTargets) async {
        let namespace = normalizeNamespace(namespace)
        await joinLocally(rooms: rooms, in: namespace, targets: targets)
        guard !targets.isLocalOnly else { return }
        try? await publishClusterCommand(.init(
            originNodeID: currentNodeID,
            targetNodeID: nil,
            message: .joinRooms(namespace: namespace, targets: .init(targets), rooms: rooms)
        ))
    }

    func join(room: String, in namespace: String, targets: BroadcastTargets) async {
        await join(rooms: [room], in: namespace, targets: targets)
    }

    func leave(rooms: [String], in namespace: String, targets: BroadcastTargets) async {
        let namespace = normalizeNamespace(namespace)
        await leaveLocally(rooms: rooms, in: namespace, targets: targets)
        guard !targets.isLocalOnly else { return }
        try? await publishClusterCommand(.init(
            originNodeID: currentNodeID,
            targetNodeID: nil,
            message: .leaveRooms(namespace: namespace, targets: .init(targets), rooms: rooms)
        ))
    }

    func leave(room: String, in namespace: String, targets: BroadcastTargets) async {
        await leave(rooms: [room], in: namespace, targets: targets)
    }

    func fetchSockets(in namespace: String, targets: BroadcastTargets) async -> [RemoteSocket] {
        let namespace = normalizeNamespace(namespace)
        let localSockets = await localSocketSnapshots(in: namespace, targets: targets).map(makeRemoteSocket(from:))
        await startClusterIfNeeded()
        guard !targets.isLocalOnly, let clusterState else { return localSockets }

        let otherNodeIDs = (try? await clusterState.coordinator.otherNodeIDs()) ?? []
        guard !otherNodeIDs.isEmpty else { return localSockets }

        let requestID = UUID()
        let aggregator = ClusterResponseAggregator<[RemoteSocketSnapshot]>(expectedNodeIDs: otherNodeIDs)
        pendingFetchSocketsResponses[requestID] = aggregator
        defer { pendingFetchSocketsResponses.removeValue(forKey: requestID) }

        do {
            try await publishClusterCommand(.init(
                originNodeID: clusterState.coordinator.nodeID,
                targetNodeID: nil,
                message: .fetchSocketsRequest(
                    requestID: requestID,
                    namespace: namespace,
                    targets: .init(targets)
                )
            ))
        } catch {
            return localSockets
        }

        let remoteSnapshots = await aggregator.wait(timeout: clusterState.requestTimeout)
            .flatMap(\.1)
            .map(makeRemoteSocket(from:))
        return localSockets + remoteSnapshots
    }

    func disconnectSockets(in namespace: String, targets: BroadcastTargets, close: Bool) async {
        let namespace = normalizeNamespace(namespace)
        await disconnectSocketsLocally(in: namespace, targets: targets, close: close)
        guard !targets.isLocalOnly else { return }
        try? await publishClusterCommand(.init(
            originNodeID: currentNodeID,
            targetNodeID: nil,
            message: .disconnectSockets(namespace: namespace, targets: .init(targets), close: close)
        ))
    }

    func serverSideEmit(in namespace: String, event: String, items: [SocketIOValue]) async {
        let namespace = normalizeNamespace(namespace)
        await startClusterIfNeeded()
        guard let clusterState else { return }
        let otherNodeIDs = (try? await clusterState.coordinator.otherNodeIDs()) ?? []
        guard !otherNodeIDs.isEmpty else { return }
        try? await publishClusterCommand(.init(
            originNodeID: clusterState.coordinator.nodeID,
            targetNodeID: nil,
            message: .serverSideEmit(
                namespace: namespace,
                event: event,
                items: items.map(ClusterSocketIOValue.init)
            )
        ))
    }

    func serverSideEmitExpectingAcks(
        in namespace: String,
        event: String,
        items: [SocketIOValue],
        timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        let namespace = normalizeNamespace(namespace)
        await startClusterIfNeeded()
        guard let clusterState else { return [] }
        let otherNodeIDs = try await clusterState.coordinator.otherNodeIDs()
        guard !otherNodeIDs.isEmpty else { return [] }

        let requestID = UUID()
        let aggregator = ClusterResponseAggregator<[ClusterSocketIOValue]>(expectedNodeIDs: otherNodeIDs)
        pendingServerAckResponses[requestID] = aggregator
        defer { pendingServerAckResponses.removeValue(forKey: requestID) }

        try await publishClusterCommand(.init(
            originNodeID: clusterState.coordinator.nodeID,
            targetNodeID: nil,
            message: .serverSideEmitAckRequest(
                requestID: requestID,
                namespace: namespace,
                event: event,
                items: items.map(ClusterSocketIOValue.init),
                timeoutMilliseconds: timeoutMilliseconds(timeout)
            )
        ))

        let responses = await aggregator.wait(timeout: timeout)
        let payloads = responses.map { $0.1.map(\.value) }
        guard responses.count == otherNodeIDs.count else {
            throw ServerSideAckTimeoutError(
                responses: payloads,
                receivedCount: responses.count,
                expectedCount: otherNodeIDs.count,
                missingCount: otherNodeIDs.count - responses.count
            )
        }
        return payloads
    }

    func emit(event: String, items: [SocketIOValue], to socketID: String, in namespace: String) async {
        let namespace = normalizeNamespace(namespace)
        if let connectionID = socketIndex[.init(namespace: namespace, socketID: socketID)] {
            let privateSessionID = connections[connectionID]?.connectedNamespaces[namespace]?.recovery?.privateSessionID
            await emitEvent(
                on: connectionID,
                socketID: socketID,
                privateSessionID: privateSessionID,
                namespace: namespace,
                volatility: .reliable,
                event: event,
                items: items,
                ack: nil
            )
            return
        }

        guard let recoveryStore else { return }
        let disconnectedSessions = (try? await recoveryStore.disconnectedSessions(in: namespace)) ?? []
        guard let disconnectedSession = disconnectedSessions.first(where: { $0.socketID == socketID }) else { return }
        await queueRecoverablePacket(
            forPrivateSessionID: disconnectedSession.privateSessionID,
            namespace: namespace,
            event: event,
            items: items
        )
    }

    func join(rooms: [String], to socketID: String, in namespace: String) async {
        for room in rooms {
            await join(room: room, socketID: socketID, namespace: namespace)
        }
    }

    func leave(rooms: [String], from socketID: String, in namespace: String) async {
        for room in rooms {
            await leave(room: room, socketID: socketID, namespace: namespace)
        }
    }

    func disconnect(socketID: String, in namespace: String, close: Bool) async {
        let namespace = normalizeNamespace(namespace)
        guard let connectionID = socketIndex[.init(namespace: namespace, socketID: socketID)] else { return }
        guard let state = connections[connectionID] else { return }
        try? await handleDisconnect(
            namespace: namespace,
            on: state.connection,
            reason: .serverNamespaceDisconnect,
            notifyPeer: true,
            closeTransport: close
        )
    }

    func broadcast(
        in namespace: String,
        targets: BroadcastTargets,
        volatility: EmitVolatility,
        event: String,
        items: [SocketIOValue]
    ) async {
        let namespace = normalizeNamespace(namespace)
        await broadcastLocally(in: namespace, targets: targets, volatility: volatility, event: event, items: items)
        if volatility == .reliable, !targets.isLocalOnly {
            await queueRecoverableBroadcastPackets(in: namespace, targets: targets, event: event, items: items)
        }
        guard !targets.isLocalOnly else { return }
        try? await publishClusterCommand(.init(
            originNodeID: currentNodeID,
            targetNodeID: nil,
            message: .broadcast(
                namespace: namespace,
                targets: .init(targets),
                volatility: volatility,
                event: event,
                items: items.map(ClusterSocketIOValue.init)
            )
        ))
    }

    func broadcastExpectingAcks(
        in namespace: String,
        targets: BroadcastTargets,
        volatility: EmitVolatility,
        event: String,
        items: [SocketIOValue],
        timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        let namespace = normalizeNamespace(namespace)
        await startClusterIfNeeded()
        let localTask = Task { [weak self] in
            await self?.broadcastLocallyExpectingAcks(
                in: namespace,
                targets: targets,
                volatility: volatility,
                event: event,
                items: items,
                timeout: timeout
            ) ?? .init(responses: [], receivedCount: 0, expectedCount: 0, timedOut: false)
        }

        var remotePayloads: [(String, ClusterEnvelope.Message)] = []
        var expectedRemoteNodes: Set<String> = []
        if !targets.isLocalOnly, let clusterState {
            expectedRemoteNodes = (try? await clusterState.coordinator.otherNodeIDs()) ?? []
            if !expectedRemoteNodes.isEmpty {
                let requestID = UUID()
                let aggregator = ClusterResponseAggregator<ClusterEnvelope.Message>(expectedNodeIDs: expectedRemoteNodes)
                pendingBroadcastAckResponses[requestID] = aggregator
                defer { pendingBroadcastAckResponses.removeValue(forKey: requestID) }

                try? await publishClusterCommand(.init(
                    originNodeID: clusterState.coordinator.nodeID,
                    targetNodeID: nil,
                    message: .broadcastAckRequest(
                        requestID: requestID,
                        namespace: namespace,
                        targets: .init(targets),
                        volatility: volatility,
                        event: event,
                        items: items.map(ClusterSocketIOValue.init),
                        timeoutMilliseconds: timeoutMilliseconds(timeout)
                    )
                ))

                remotePayloads = await aggregator.wait(timeout: timeout)
            }
        }

        let localOutcome = await localTask.value
        var allResponses = localOutcome.responses
        var receivedCount = localOutcome.receivedCount
        var expectedCount = localOutcome.expectedCount
        var timedOut = localOutcome.timedOut || remotePayloads.count < expectedRemoteNodes.count

        for (_, payload) in remotePayloads {
            guard case .broadcastAckResponse(_, let responses, let remoteReceived, let remoteExpected, let remoteTimedOut) = payload else {
                continue
            }
            allResponses.append(contentsOf: responses)
            receivedCount += remoteReceived
            expectedCount += remoteExpected
            timedOut = timedOut || remoteTimedOut
        }

        let orderedResponses = allResponses
            .sorted { $0.socketID < $1.socketID }
            .map { $0.items.map(\.value) }
        if timedOut {
            let adjustedExpectedCount = max(expectedCount, orderedResponses.count + max(0, expectedRemoteNodes.count - remotePayloads.count))
            throw BroadcastAckTimeoutError(
                responses: orderedResponses,
                receivedCount: receivedCount,
                expectedCount: adjustedExpectedCount,
                missingCount: max(0, adjustedExpectedCount - receivedCount)
            )
        }
        return orderedResponses
    }

    private func send(
        _ packet: SocketIOPacket,
        on connection: EngineIOConnection,
        volatility: EmitVolatility = .reliable
    ) async throws -> Bool {
        let encoded = try packet.encode()
        switch volatility {
        case .reliable:
            await connection.send(.text(encoded.text))
            for attachment in encoded.attachments {
                await connection.send(.binary(attachment))
            }
            return true
        case .volatile:
            guard await connection.isWritable else { return false }
            guard await connection.sendVolatile(.text(encoded.text)) else { return false }
            for attachment in encoded.attachments {
                guard await connection.sendVolatile(.binary(attachment)) else { return false }
            }
            return true
        }
    }

    private var currentNodeID: String {
        clusterState?.coordinator.nodeID ?? "__local__"
    }

    private func recordClusterStartupError(_ error: any Error) {
        clusterStartupError = error
    }

    private func startClusterIfNeeded() async {
        guard !clusterStarted, let clusterState else { return }
        clusterStarted = true
        do {
            try await clusterState.coordinator.start(
                onCommand: { [weak self] data in
                    await self?.receiveClusterCommand(data)
                },
                onResponse: { [weak self] data in
                    await self?.receiveClusterResponse(data)
                }
            )
        } catch {
            clusterStartupError = error
        }
    }

    func stopCluster() async {
        if let clusterState {
            await clusterState.coordinator.stop()
        }
    }

    private func publishClusterCommand(_ envelope: ClusterEnvelope) async throws {
        await startClusterIfNeeded()
        guard let clusterState else { return }
        let data = try jsonEncoder.encode(envelope)
        try await clusterState.coordinator.publishCommand(data)
    }

    private func publishClusterResponse(_ envelope: ClusterEnvelope, to nodeID: String) async throws {
        await startClusterIfNeeded()
        guard let clusterState else { return }
        let data = try jsonEncoder.encode(envelope)
        try await clusterState.coordinator.publishResponse(data, to: nodeID)
    }

    private func receiveClusterCommand(_ data: Data) async {
        guard let clusterState else { return }
        let envelope: ClusterEnvelope
        do {
            envelope = try jsonDecoder.decode(ClusterEnvelope.self, from: data)
        } catch {
            return
        }
        if envelope.originNodeID == clusterState.coordinator.nodeID {
            return
        }
        if let targetNodeID = envelope.targetNodeID, targetNodeID != clusterState.coordinator.nodeID {
            return
        }

        switch envelope.message {
        case .broadcast(let namespace, let targets, let volatility, let event, let items):
            await broadcastLocally(
                in: namespace,
                targets: targets.targets,
                volatility: volatility,
                event: event,
                items: items.map(\.value)
            )
        case .broadcastAckRequest(let requestID, let namespace, let targets, let volatility, let event, let items, let timeoutMilliseconds):
            let timeout = Duration.milliseconds(timeoutMilliseconds)
            let outcome = await broadcastLocallyExpectingAcks(
                in: namespace,
                targets: targets.targets.localOnly,
                volatility: volatility,
                event: event,
                items: items.map(\.value),
                timeout: timeout
            )
            try? await publishClusterResponse(.init(
                originNodeID: clusterState.coordinator.nodeID,
                targetNodeID: envelope.originNodeID,
                message: .broadcastAckResponse(
                    requestID: requestID,
                    responses: outcome.responses,
                    receivedCount: outcome.receivedCount,
                    expectedCount: outcome.expectedCount,
                    timedOut: outcome.timedOut
                )
            ), to: envelope.originNodeID)
        case .fetchSocketsRequest(let requestID, let namespace, let targets):
            let snapshots = await localSocketSnapshots(in: namespace, targets: targets.targets.localOnly)
            try? await publishClusterResponse(.init(
                originNodeID: clusterState.coordinator.nodeID,
                targetNodeID: envelope.originNodeID,
                message: .fetchSocketsResponse(requestID: requestID, sockets: snapshots)
            ), to: envelope.originNodeID)
        case .joinRooms(let namespace, let targets, let rooms):
            await joinLocally(rooms: rooms, in: namespace, targets: targets.targets.localOnly)
        case .leaveRooms(let namespace, let targets, let rooms):
            await leaveLocally(rooms: rooms, in: namespace, targets: targets.targets.localOnly)
        case .disconnectSockets(let namespace, let targets, let close):
            await disconnectSocketsLocally(in: namespace, targets: targets.targets.localOnly, close: close)
        case .remoteSocketEmit(let namespace, let socketID, let event, let items):
            await emit(event: event, items: items.map(\.value), to: socketID, in: namespace)
        case .remoteSocketJoin(let namespace, let socketID, let rooms):
            await join(rooms: rooms, to: socketID, in: namespace)
        case .remoteSocketLeave(let namespace, let socketID, let rooms):
            await leave(rooms: rooms, from: socketID, in: namespace)
        case .remoteSocketDisconnect(let namespace, let socketID, let close):
            await disconnect(socketID: socketID, in: namespace, close: close)
        case .serverSideEmit(let namespace, let event, let items):
            await dispatchServerSideEvent(
                namespace: namespace,
                event: event,
                items: items.map(\.value),
                ackOperation: nil
            )
        case .serverSideEmitAckRequest(let requestID, let namespace, let event, let items, _):
            let responseItems = await dispatchServerSideEventExpectingAck(
                namespace: namespace,
                event: event,
                items: items.map(\.value)
            )
            try? await publishClusterResponse(.init(
                originNodeID: clusterState.coordinator.nodeID,
                targetNodeID: envelope.originNodeID,
                message: .serverSideEmitAckResponse(
                    requestID: requestID,
                    items: responseItems.map(ClusterSocketIOValue.init)
                )
            ), to: envelope.originNodeID)
        case .broadcastAckResponse, .fetchSocketsResponse, .serverSideEmitAckResponse:
            break
        }
    }

    private func receiveClusterResponse(_ data: Data) async {
        let envelope: ClusterEnvelope
        do {
            envelope = try jsonDecoder.decode(ClusterEnvelope.self, from: data)
        } catch {
            return
        }

        switch envelope.message {
        case .broadcastAckResponse(let requestID, _, _, _, _):
            await pendingBroadcastAckResponses[requestID]?.receive(from: envelope.originNodeID, response: envelope.message)
        case .fetchSocketsResponse(let requestID, let sockets):
            await pendingFetchSocketsResponses[requestID]?.receive(from: envelope.originNodeID, response: sockets)
        case .serverSideEmitAckResponse(let requestID, let items):
            await pendingServerAckResponses[requestID]?.receive(from: envelope.originNodeID, response: items)
        case .broadcast,
             .broadcastAckRequest,
             .fetchSocketsRequest,
             .joinRooms,
             .leaveRooms,
             .disconnectSockets,
             .remoteSocketEmit,
             .remoteSocketJoin,
             .remoteSocketLeave,
             .remoteSocketDisconnect,
             .serverSideEmit,
             .serverSideEmitAckRequest:
            break
        }
    }

    private func joinLocally(rooms: [String], in namespace: String, targets: BroadcastTargets) async {
        let namespace = normalizeNamespace(namespace)
        guard let adapter = namespaceRegistry.registeredNamespace(for: namespace)?.adapter else { return }
        for target in await resolveConnectedTargets(in: namespace, targets: targets) {
            for room in rooms {
                await adapter.add(socketID: target.socketID, to: room)
            }
        }
    }

    private func leaveLocally(rooms: [String], in namespace: String, targets: BroadcastTargets) async {
        let namespace = normalizeNamespace(namespace)
        guard let adapter = namespaceRegistry.registeredNamespace(for: namespace)?.adapter else { return }
        for target in await resolveConnectedTargets(in: namespace, targets: targets) {
            for room in rooms where room != target.socketID {
                await adapter.remove(socketID: target.socketID, from: room)
            }
        }
    }

    private func disconnectSocketsLocally(in namespace: String, targets: BroadcastTargets, close: Bool) async {
        let namespace = normalizeNamespace(namespace)
        var closedConnectionIDs: Set<String> = []
        for target in await resolveConnectedTargets(in: namespace, targets: targets) {
            if close, closedConnectionIDs.contains(target.connectionID) {
                continue
            }
            try? await handleDisconnect(
                namespace: namespace,
                on: target.connection,
                reason: .serverNamespaceDisconnect,
                notifyPeer: true,
                closeTransport: close
            )
            if close {
                closedConnectionIDs.insert(target.connectionID)
            }
        }
    }

    private func broadcastLocally(
        in namespace: String,
        targets: BroadcastTargets,
        volatility: EmitVolatility,
        event: String,
        items: [SocketIOValue]
    ) async {
        let namespace = normalizeNamespace(namespace)
        for target in await resolveConnectedTargets(in: namespace, targets: targets) {
            var arguments = [SocketIOValue.string(event)] + items
            if volatility == .reliable,
               var state = connections[target.connectionID],
               var namespaceState = state.connectedNamespaces[namespace],
               var recovery = namespaceState.recovery {
                let offset = String(recovery.nextPacketOffset)
                recovery.nextPacketOffset += 1
                namespaceState.recovery = recovery
                state.connectedNamespaces[namespace] = namespaceState
                connections[target.connectionID] = state
                arguments.append(.string(offset))
            }
            _ = try? await send(
                .event(namespace: namespace, items: arguments, ackID: nil),
                on: target.connection,
                volatility: volatility
            )
        }
    }

    private func queueRecoverableBroadcastPackets(
        in namespace: String,
        targets: BroadcastTargets,
        event: String,
        items: [SocketIOValue]
    ) async {
        for session in await disconnectedRecoverySessions(in: namespace, targets: targets) {
            await queueRecoverablePacket(
                forPrivateSessionID: session.privateSessionID,
                namespace: namespace,
                event: event,
                items: items
            )
        }
    }

    private func disconnectedRecoverySessions(
        in namespace: String,
        targets: BroadcastTargets
    ) async -> [ConnectionStateRecoverySession] {
        guard let recoveryStore else { return [] }
        let disconnectedSessions = (try? await recoveryStore.disconnectedSessions(in: namespace)) ?? []
        return disconnectedSessions.filter { session in
            guard !targets.excludedSocketIDs.contains(session.socketID) else { return false }
            let rooms = Set(session.rooms)
            let matchesIncludedRooms = targets.includedRooms.isEmpty || !rooms.isDisjoint(with: targets.includedRooms)
            guard matchesIncludedRooms else { return false }
            return rooms.isDisjoint(with: targets.excludedRooms)
        }
    }

    private func broadcastLocallyExpectingAcks(
        in namespace: String,
        targets: BroadcastTargets,
        volatility: EmitVolatility,
        event: String,
        items: [SocketIOValue],
        timeout: Duration
    ) async -> LocalBroadcastAckOutcome {
        let namespace = normalizeNamespace(namespace)
        let validTargets = await resolveConnectedTargets(in: namespace, targets: targets)
        let candidateTargets: [ConnectedTarget]
        if volatility == .volatile {
            var writableTargets: [ConnectedTarget] = []
            for target in validTargets where await target.connection.isWritable {
                writableTargets.append(target)
            }
            candidateTargets = writableTargets
        } else {
            candidateTargets = validTargets
        }
        guard !candidateTargets.isEmpty else {
            return .init(responses: [], receivedCount: 0, expectedCount: 0, timedOut: false)
        }

        let arguments = [SocketIOValue.string(event)] + items
        let aggregator = BroadcastAckAggregator(expectedCount: candidateTargets.count)
        var pendingTokens: [BroadcastAckToken] = []
        var outboundPackets: [(index: Int, token: BroadcastAckToken, connection: EngineIOConnection, packet: SocketIOPacket)] = []

        do {
            for (index, target) in candidateTargets.enumerated() {
                guard
                    var state = connections[target.connectionID],
                    var namespaceState = state.connectedNamespaces[namespace]
                else {
                    await aggregator.cancel(index: index)
                    continue
                }

                let ackID = namespaceState.nextAckID
                namespaceState.nextAckID += 1
                namespaceState.pendingAcks[ackID] = { items in
                    await aggregator.receive(index: index, items: items)
                }
                state.connectedNamespaces[namespace] = namespaceState
                connections[target.connectionID] = state
                let token = BroadcastAckToken(connectionID: target.connectionID, namespace: namespace, ackID: ackID)
                pendingTokens.append(token)
                outboundPackets.append((
                    index: index,
                    token: token,
                    connection: target.connection,
                    packet: .event(namespace: namespace, items: arguments, ackID: ackID)
                ))
            }

            for outbound in outboundPackets {
                let accepted = try await send(outbound.packet, on: outbound.connection, volatility: volatility)
                guard !accepted else { continue }
                await expirePendingBroadcastAcks([outbound.token])
                await aggregator.cancel(index: outbound.index)
            }
        } catch {
            await expirePendingBroadcastAcks(pendingTokens)
            return .init(responses: [], receivedCount: 0, expectedCount: candidateTargets.count, timedOut: true)
        }

        guard !pendingTokens.isEmpty else {
            return .init(responses: [], receivedCount: 0, expectedCount: 0, timedOut: false)
        }

        let timeoutTokens = pendingTokens
        do {
            _ = try await aggregator.wait(timeout: timeout) { [weak self] in
                await self?.expirePendingBroadcastAcks(timeoutTokens)
            }
            let indexed = await aggregator.indexedResponses()
            let responses = indexed.map { index, items in
                ClusterSocketAckResponse(
                    socketID: candidateTargets[index].socketID,
                    items: items.map(ClusterSocketIOValue.init)
                )
            }
            return .init(
                responses: responses,
                receivedCount: responses.count,
                expectedCount: candidateTargets.count,
                timedOut: false
            )
        } catch let error as BroadcastAckTimeoutError {
            let indexed = await aggregator.indexedResponses()
            let responses = indexed.map { index, items in
                ClusterSocketAckResponse(
                    socketID: candidateTargets[index].socketID,
                    items: items.map(ClusterSocketIOValue.init)
                )
            }
            return .init(
                responses: responses,
                receivedCount: error.receivedCount,
                expectedCount: error.expectedCount,
                timedOut: true
            )
        } catch {
            return .init(responses: [], receivedCount: 0, expectedCount: candidateTargets.count, timedOut: true)
        }
    }

    private func localSocketSnapshots(in namespace: String, targets: BroadcastTargets) async -> [RemoteSocketSnapshot] {
        let namespace = normalizeNamespace(namespace)
        var snapshots: [RemoteSocketSnapshot] = []
        for target in await resolveConnectedTargets(in: namespace, targets: targets) {
            guard let state = connections[target.connectionID], let namespaceState = state.connectedNamespaces[namespace] else {
                continue
            }
            snapshots.append(.init(
                nodeID: currentNodeID,
                id: target.socketID,
                namespace: namespace,
                handshake: makeRemoteHandshake(from: namespaceState.socket.handshake),
                rooms: Array(await rooms(for: target.socketID, namespace: namespace)).sorted(),
                data: namespaceState.socketState.currentData()
            ))
        }
        return snapshots
    }

    private func makeRemoteHandshake(from handshake: SocketHandshake) -> RemoteSocketHandshake {
        .init(
            engineSessionID: handshake.engineSessionID,
            request: .init(
                method: handshake.request.method.rawValue,
                scheme: handshake.request.scheme,
                authority: handshake.request.authority,
                path: handshake.request.path ?? "/"
            ),
            auth: handshake.auth
        )
    }

    private func makeRemoteSocket(from snapshot: RemoteSocketSnapshot) -> RemoteSocket {
        let currentNodeID = self.currentNodeID
        return RemoteSocket(
            id: snapshot.id,
            namespace: snapshot.namespace,
            handshake: snapshot.handshake,
            rooms: Set(snapshot.rooms),
            data: snapshot.data,
            emitOperation: { [weak self] event, items in
                guard let self else { return }
                if snapshot.nodeID == currentNodeID {
                    await self.emit(event: event, items: items, to: snapshot.id, in: snapshot.namespace)
                } else {
                    try? await self.publishClusterCommand(.init(
                        originNodeID: currentNodeID,
                        targetNodeID: snapshot.nodeID,
                        message: .remoteSocketEmit(
                            namespace: snapshot.namespace,
                            socketID: snapshot.id,
                            event: event,
                            items: items.map(ClusterSocketIOValue.init)
                        )
                    ))
                }
            },
            joinOperation: { [weak self] rooms in
                guard let self else { return }
                if snapshot.nodeID == currentNodeID {
                    await self.join(rooms: rooms, to: snapshot.id, in: snapshot.namespace)
                } else {
                    try? await self.publishClusterCommand(.init(
                        originNodeID: currentNodeID,
                        targetNodeID: snapshot.nodeID,
                        message: .remoteSocketJoin(namespace: snapshot.namespace, socketID: snapshot.id, rooms: rooms)
                    ))
                }
            },
            leaveOperation: { [weak self] rooms in
                guard let self else { return }
                if snapshot.nodeID == currentNodeID {
                    await self.leave(rooms: rooms, from: snapshot.id, in: snapshot.namespace)
                } else {
                    try? await self.publishClusterCommand(.init(
                        originNodeID: currentNodeID,
                        targetNodeID: snapshot.nodeID,
                        message: .remoteSocketLeave(namespace: snapshot.namespace, socketID: snapshot.id, rooms: rooms)
                    ))
                }
            },
            disconnectOperation: { [weak self] close in
                guard let self else { return }
                if snapshot.nodeID == currentNodeID {
                    await self.disconnect(socketID: snapshot.id, in: snapshot.namespace, close: close)
                } else {
                    try? await self.publishClusterCommand(.init(
                        originNodeID: currentNodeID,
                        targetNodeID: snapshot.nodeID,
                        message: .remoteSocketDisconnect(namespace: snapshot.namespace, socketID: snapshot.id, close: close)
                    ))
                }
            }
        )
    }

    private func dispatchServerSideEvent(
        namespace: String,
        event: String,
        items: [SocketIOValue],
        ackOperation: (@Sendable ([SocketIOValue]) async -> Void)?
    ) async {
        let namespace = normalizeNamespace(namespace)
        let handlers = namespaceRegistry.registeredNamespace(for: namespace)?.serverEventHandlers[event] ?? []
        guard !handlers.isEmpty else {
            if let ackOperation {
                await ackOperation([])
            }
            return
        }

        if let ackOperation {
            let ack = ServerSideAck(sendOperation: ackOperation)
            for handler in handlers {
                await handler(.init(name: event, arguments: items), ack)
            }
            try? await ack.send(arguments: [])
        } else {
            for handler in handlers {
                await handler(.init(name: event, arguments: items), nil)
            }
        }
    }

    private func dispatchServerSideEventExpectingAck(
        namespace: String,
        event: String,
        items: [SocketIOValue]
    ) async -> [SocketIOValue] {
        let responseStore = Mutex<[SocketIOValue]?>(nil)
        await dispatchServerSideEvent(namespace: namespace, event: event, items: items) { ackItems in
            responseStore.withLock {
                if $0 == nil {
                    $0 = ackItems
                }
            }
        }
        return responseStore.withLock { $0 ?? [] }
    }

    private func timeoutMilliseconds(_ timeout: Duration) -> Int {
        max(1, Int(timeout.components.seconds * 1_000) + Int(timeout.components.attoseconds / 1_000_000_000_000_000))
    }

    private func resolveConnectedTargets(in namespace: String, targets: BroadcastTargets) async -> [ConnectedTarget] {
        let namespace = normalizeNamespace(namespace)
        guard let adapter = namespaceRegistry.registeredNamespace(for: namespace)?.adapter else { return [] }
        let resolvedSocketIDs = await adapter.resolveTargets(
            including: targets.includedRooms,
            excluding: targets.excludedRooms,
            excludingSocketIDs: targets.excludedSocketIDs
        )
        return resolvedSocketIDs.sorted().compactMap { socketID in
            guard let connectionID = socketIndex[.init(namespace: namespace, socketID: socketID)] else {
                return nil
            }
            guard let state = connections[connectionID], state.connectedNamespaces[namespace] != nil else {
                return nil
            }
            return ConnectedTarget(socketID: socketID, connectionID: connectionID, connection: state.connection)
        }
    }

    private func expirePendingBroadcastAcks(_ tokens: [BroadcastAckToken]) async {
        for token in tokens {
            guard var state = connections[token.connectionID], var namespaceState = state.connectedNamespaces[token.namespace] else {
                continue
            }
            guard namespaceState.pendingAcks.removeValue(forKey: token.ackID) != nil else {
                continue
            }
            namespaceState.ignoredAckIDs.insert(token.ackID)
            state.connectedNamespaces[token.namespace] = namespaceState
            connections[token.connectionID] = state
        }
    }

    private func socketDisconnectReason(from reason: EngineIOCloseReason) -> SocketDisconnectReason {
        switch reason {
        case .protocolViolation:
            .parseError
        case .heartbeatTimeout:
            .pingTimeout
        case .transportClosed, .clientInitiated, .serverInitiated:
            .transportClosed
        }
    }

    private func shouldPersistRecoveryState(for reason: SocketDisconnectReason) -> Bool {
        switch reason {
        case .transportClosed, .transportError, .pingTimeout:
            return configuration.connectionStateRecovery != nil
        case .clientNamespaceDisconnect,
             .serverNamespaceDisconnect,
             .serverShuttingDown,
             .parseError,
             .forcedServerClose,
             .forcedClose,
             .connectTimeout,
             .unknown:
            return false
        }
    }

    private func persistRecoverableSession(
        namespace: String,
        namespaceState: ConnectedNamespaceState
    ) async {
        guard let recovery = configuration.connectionStateRecovery else { return }
        guard let recoveryStore else { return }
        guard let activeRecovery = namespaceState.recovery else { return }

        let session = ConnectionStateRecoverySession(
            privateSessionID: activeRecovery.privateSessionID,
            namespace: namespace,
            socketID: namespaceState.socket.id,
            rooms: Array(await rooms(for: namespaceState.socket.id, namespace: namespace)).sorted(),
            data: namespaceState.socketState.currentData(),
            nextPacketOffset: activeRecovery.nextPacketOffset
        )
        try? await recoveryStore.saveSession(
            session,
            maxDisconnectionDuration: recovery.maxDisconnectionDuration
        )
    }

    private func tearDownNamespace(
        _ namespace: String,
        namespaceState: ConnectedNamespaceState,
        reason: SocketDisconnectReason,
        notifyPeerOn connection: EngineIOConnection?
    ) async {
        await namespaceState.socketState.handleDisconnecting(reason: reason)
        if shouldPersistRecoveryState(for: reason) {
            await persistRecoverableSession(namespace: namespace, namespaceState: namespaceState)
        } else if let privateSessionID = namespaceState.recovery?.privateSessionID {
            try? await recoveryStore?.removeSession(forPrivateSessionID: privateSessionID)
        }
        socketIndex.removeValue(forKey: .init(namespace: namespace, socketID: namespaceState.socket.id))
        await namespaceRegistry.registeredNamespace(for: namespace)?.adapter.remove(socketID: namespaceState.socket.id)
        if let connection {
            _ = try? await send(.disconnect(namespace: namespace), on: connection)
        }
        await namespaceState.socketState.handleDisconnect(reason: reason)

        if shouldAutoCleanupDynamicNamespace(namespace), !hasConnectedSockets(in: namespace) {
            namespaceRegistry.unregisterDynamicNamespace(namespace)
        }
    }

    private func shouldAutoCleanupDynamicNamespace(_ namespace: String) -> Bool {
        guard case .dynamicChild(_, let childLifetimePolicy)? = namespaceRegistry.namespaceKind(for: namespace) else {
            return false
        }
        return childLifetimePolicy == .autoCleanup
    }

    private func hasConnectedSockets(in namespace: String) -> Bool {
        let namespace = normalizeNamespace(namespace)
        return socketIndex.keys.contains(where: { $0.namespace == namespace })
    }
}

private func normalizeNamespace(_ namespace: String) -> String {
    if namespace.isEmpty || namespace == "/" {
        return "/"
    }
    return namespace.hasPrefix("/") ? namespace : "/\(namespace)"
}
