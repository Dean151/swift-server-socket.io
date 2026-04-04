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

import Mutex

private struct SocketRegistrations {
    var handlers: [String: [SocketState.EventHandler]] = [:]
    var middlewares: [SocketMiddleware] = []
    var disconnectingHandlers: [SocketState.DisconnectHandler] = []
    var disconnectHandlers: [SocketState.DisconnectHandler] = []
}

final class SocketState: Sendable {
    typealias EventHandler = @Sendable (SocketEvent, SocketAck?) async -> Void
    typealias DisconnectHandler = @Sendable (SocketDisconnectReason) async -> Void

    let id: String
    let namespace: String
    let handshake: SocketHandshake
    let recovered: Bool

    private let emitOperation: @Sendable (EmitVolatility, String, [SocketIOValue], (@Sendable ([SocketIOValue]) async -> Void)?) async -> Void
    private let disconnectOperation: @Sendable (Bool) async -> Void
    private let registrations = Mutex(SocketRegistrations())
    private let data: Mutex<[String: SocketIOValue]>

    init(
        id: String,
        namespace: String,
        handshake: SocketHandshake,
        recovered: Bool,
        data: [String: SocketIOValue],
        emitOperation: @escaping @Sendable (EmitVolatility, String, [SocketIOValue], (@Sendable ([SocketIOValue]) async -> Void)?) async -> Void,
        disconnectOperation: @escaping @Sendable (Bool) async -> Void
    ) {
        self.id = id
        self.namespace = namespace
        self.handshake = handshake
        self.recovered = recovered
        self.emitOperation = emitOperation
        self.disconnectOperation = disconnectOperation
        self.data = .init(data)
    }

    func on(_ event: String, handler: @escaping EventHandler) {
        registrations.withLock {
            $0.handlers[event, default: []].append(handler)
        }
    }

    func use(_ middleware: @escaping SocketMiddleware) {
        registrations.withLock {
            $0.middlewares.append(middleware)
        }
    }

    func onDisconnecting(_ handler: @escaping DisconnectHandler) {
        registrations.withLock {
            $0.disconnectingHandlers.append(handler)
        }
    }

    func onDisconnect(_ handler: @escaping DisconnectHandler) {
        registrations.withLock {
            $0.disconnectHandlers.append(handler)
        }
    }

    func emit(_ event: String, items: [SocketIOValue], volatility: EmitVolatility = .reliable) async {
        await emitOperation(volatility, event, items, nil)
    }

    func emit(
        _ event: String,
        items: [SocketIOValue],
        volatility: EmitVolatility = .reliable,
        ack: @escaping @Sendable ([SocketIOValue]) async -> Void
    ) async {
        await emitOperation(volatility, event, items, ack)
    }

    func disconnect(close: Bool = false) async {
        await disconnectOperation(close)
    }

    func currentData() -> [String: SocketIOValue] {
        data.withLock { $0 }
    }

    func setData(_ values: [String: SocketIOValue]) {
        data.withLock { $0 = values }
    }

    func mergeData(_ values: [String: SocketIOValue]) {
        data.withLock {
            for (key, value) in values {
                $0[key] = value
            }
        }
    }

    func handleDisconnecting(reason: SocketDisconnectReason) async {
        let handlers = registrations.withLock { $0.disconnectingHandlers }
        for handler in handlers {
            await handler(reason)
        }
    }

    func handleDisconnect(reason: SocketDisconnectReason) async {
        let handlers = registrations.withLock { $0.disconnectHandlers }
        for handler in handlers {
            await handler(reason)
        }
    }

    func handleIncomingEvent(arguments: [SocketIOValue], ack: SocketAck?) async throws {
        guard case .string(let name) = arguments.first else { return }
        let event = SocketEvent(name: name, arguments: Array(arguments.dropFirst()))
        let middlewares = registrations.withLock { $0.middlewares }
        let handlers = registrations.withLock { $0.handlers[name] ?? [] }
        let shouldDispatch = try await runSocketMiddlewares(middlewares, event: event, ack: ack)
        guard shouldDispatch else { return }
        for handler in handlers {
            await handler(event, ack)
        }
    }

    func emitError(_ error: any Error) async {
        let handlers = registrations.withLock { $0.handlers["error"] ?? [] }
        guard !handlers.isEmpty else { return }
        let payload = try? SocketIOValue(encoding: socketErrorPayload(from: error))
        let event = SocketEvent(name: "error", arguments: payload.map { [$0] } ?? [.string(String(describing: error))])
        for handler in handlers {
            await handler(event, nil)
        }
    }
}

/// A connected socket within a namespace.
public struct Socket: Sendable {
    /// Handles a named event and its optional acknowledgement channel.
    public typealias EventHandler = @Sendable (SocketEvent, SocketAck?) async -> Void
    /// Handles a namespace disconnect lifecycle callback.
    public typealias DisconnectHandler = @Sendable (SocketDisconnectReason) async -> Void

    /// The namespace-scoped socket identifier.
    public let id: String
    /// The namespace this socket belongs to.
    public let namespace: String
    /// The handshake information for this socket.
    public let handshake: SocketHandshake

    private let state: SocketState
    private let joinOperation: @Sendable (String) async -> Void
    private let leaveOperation: @Sendable (String) async -> Void
    private let leaveAllOperation: @Sendable () async -> Void
    private let roomsOperation: @Sendable () async -> Set<String>
    private let broadcastEmitOperation: @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue]) async -> Void
    private let broadcastEmitWithAckOperation: @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue], Duration) async throws -> [[SocketIOValue]]
    private let broadcastFetchSocketsOperation: @Sendable (BroadcastTargets) async -> [RemoteSocket]
    private let broadcastSocketsJoinOperation: @Sendable (BroadcastTargets, [String]) async -> Void
    private let broadcastSocketsLeaveOperation: @Sendable (BroadcastTargets, [String]) async -> Void
    private let broadcastDisconnectSocketsOperation: @Sendable (BroadcastTargets, Bool) async -> Void

    init(
        id: String,
        namespace: String,
        handshake: SocketHandshake,
        state: SocketState,
        joinOperation: @escaping @Sendable (String) async -> Void,
        leaveOperation: @escaping @Sendable (String) async -> Void,
        leaveAllOperation: @escaping @Sendable () async -> Void,
        roomsOperation: @escaping @Sendable () async -> Set<String>,
        broadcastEmitOperation: @escaping @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue]) async -> Void,
        broadcastEmitWithAckOperation: @escaping @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue], Duration) async throws -> [[SocketIOValue]],
        broadcastFetchSocketsOperation: @escaping @Sendable (BroadcastTargets) async -> [RemoteSocket],
        broadcastSocketsJoinOperation: @escaping @Sendable (BroadcastTargets, [String]) async -> Void,
        broadcastSocketsLeaveOperation: @escaping @Sendable (BroadcastTargets, [String]) async -> Void,
        broadcastDisconnectSocketsOperation: @escaping @Sendable (BroadcastTargets, Bool) async -> Void
    ) {
        self.id = id
        self.namespace = namespace
        self.handshake = handshake
        self.state = state
        self.joinOperation = joinOperation
        self.leaveOperation = leaveOperation
        self.leaveAllOperation = leaveAllOperation
        self.roomsOperation = roomsOperation
        self.broadcastEmitOperation = broadcastEmitOperation
        self.broadcastEmitWithAckOperation = broadcastEmitWithAckOperation
        self.broadcastFetchSocketsOperation = broadcastFetchSocketsOperation
        self.broadcastSocketsJoinOperation = broadcastSocketsJoinOperation
        self.broadcastSocketsLeaveOperation = broadcastSocketsLeaveOperation
        self.broadcastDisconnectSocketsOperation = broadcastDisconnectSocketsOperation
    }

    /// Registers a raw event handler for a named event.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - handler: The handler invoked when the event is received.
    public func on(_ event: String, handler: @escaping EventHandler) {
        state.on(event, handler: handler)
    }

    /// Whether this socket successfully recovered a previous disconnected session.
    public var recovered: Bool {
        state.recovered
    }

    /// The user-managed data associated with this socket.
    public var data: [String: SocketIOValue] {
        get async {
            state.currentData()
        }
    }

    /// Replaces the user-managed data associated with this socket.
    public func setData(_ data: [String: SocketIOValue]) async {
        state.setData(data)
    }

    /// Merges values into the user-managed data associated with this socket.
    public func mergeData(_ values: [String: SocketIOValue]) async {
        state.mergeData(values)
    }

    /// Registers a typed event handler for a named event.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - type: The payload type to decode from the first event argument.
    ///   - handler: The handler invoked with either the decoded payload or a decoding error.
    public func on<T: Decodable & Sendable>(
        _ event: String,
        payload type: T.Type = T.self,
        handler: @escaping @Sendable (Result<T, SocketEventDecodingError>, SocketAck?) async -> Void
    ) {
        state.on(event) { incoming, ack in
            do {
                let decoded = try incoming.decode(as: T.self)
                await handler(.success(decoded), ack)
            } catch let error as SocketEventDecodingError {
                await handler(.failure(error), ack)
            } catch {
                await handler(.failure(.missingArgument(event: incoming.name, index: 0)), ack)
            }
        }
    }

    /// Registers a typed event handler using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name to observe.
    ///   - handler: The handler invoked with either the decoded payload or a decoding error.
    public func on<T: Decodable & Sendable>(
        _ event: SocketEventName<T>,
        handler: @escaping @Sendable (Result<T, SocketEventDecodingError>, SocketAck?) async -> Void
    ) {
        on(event.rawValue, payload: T.self, handler: handler)
    }

    /// Registers an inbound packet middleware for this socket.
    ///
    /// - Parameter middleware: The middleware to invoke before event handlers.
    public func use(_ middleware: @escaping SocketMiddleware) {
        state.use(middleware)
    }

    /// Registers a lifecycle hook invoked before namespace teardown removes room membership.
    ///
    /// - Parameter handler: The handler invoked while the socket still belongs to its rooms.
    public func onDisconnecting(_ handler: @escaping DisconnectHandler) {
        state.onDisconnecting(handler)
    }

    /// Registers a lifecycle hook invoked after namespace teardown completes.
    ///
    /// - Parameter handler: The handler invoked after the socket has left its rooms.
    public func onDisconnect(_ handler: @escaping DisconnectHandler) {
        state.onDisconnect(handler)
    }

    /// Registers a handler for local socket `error` events.
    ///
    /// Packet middleware failures from `socket.use(...)` are surfaced through this callback.
    ///
    /// - Parameter handler: The handler invoked with the emitted error payload.
    public func onError(_ handler: @escaping @Sendable (SocketErrorPayload) async -> Void) {
        on("error", payload: SocketErrorPayload.self) { result, _ in
            if case .success(let payload) = result {
                await handler(payload)
            }
        }
    }

    /// Emits an event to this socket.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await state.emit(event, items: arguments)
    }

    /// Emits an event to this socket and waits for the peer acknowledgement callback.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    ///   - ack: The acknowledgement handler invoked when the client acknowledges the event.
    public func emit(
        _ event: String,
        arguments: [SocketIOValue] = [],
        ack: @escaping @Sendable ([SocketIOValue]) async -> Void
    ) async {
        await state.emit(event, items: arguments, ack: ack)
    }

    /// Encodes a payload and emits it as a single event argument.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The encodable payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        await state.emit(event, items: [try SocketIOValue(encoding: payload)])
    }

    /// Encodes a payload and emits it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await emit(event.rawValue, payload: payload)
    }

    /// A best-effort emitter for this socket.
    ///
    /// Volatile packets are dropped when the underlying transport cannot flush them immediately.
    public var volatile: SocketEmitOperator {
        SocketEmitOperator { [state] event, items, ack in
            if let ack {
                await state.emit(event, items: items, volatility: .volatile, ack: ack)
            } else {
                await state.emit(event, items: items, volatility: .volatile)
            }
        }
    }

    /// Adds this socket to a room in its namespace.
    ///
    /// - Parameter room: The room to join.
    public func join(_ room: String) async {
        await joinOperation(room)
    }

    /// Removes this socket from a room in its namespace.
    ///
    /// The socket's own private room cannot be left explicitly.
    ///
    /// - Parameter room: The room to leave.
    public func leave(_ room: String) async {
        await leaveOperation(room)
    }

    /// Removes this socket from every joined room except its own private room.
    public func leaveAll() async {
        await leaveAllOperation()
    }

    /// The rooms the socket currently belongs to.
    ///
    /// This always includes the socket's own private room while the socket remains connected.
    public var rooms: Set<String> {
        get async {
            await roomsOperation()
        }
    }

    /// A broadcaster that targets every other socket in the same namespace.
    public var broadcast: BroadcastOperator {
        BroadcastOperator(
            targets: .init(excludedSocketIDs: [id]),
            emitOperation: broadcastEmitOperation,
            emitWithAckOperation: broadcastEmitWithAckOperation,
            fetchSocketsOperation: broadcastFetchSocketsOperation,
            socketsJoinOperation: broadcastSocketsJoinOperation,
            socketsLeaveOperation: broadcastSocketsLeaveOperation,
            disconnectSocketsOperation: broadcastDisconnectSocketsOperation
        )
    }

    /// Creates a broadcaster targeting a room while excluding this socket.
    ///
    /// - Parameter room: The room to include.
    public func to(_ room: String) -> BroadcastOperator {
        broadcast.to(room)
    }

    /// Creates a broadcaster excluding a room while also excluding this socket.
    ///
    /// - Parameter room: The room to exclude.
    public func excluding(_ room: String) -> BroadcastOperator {
        broadcast.excluding(room)
    }


    /// Disconnects this socket from its namespace.
    public func disconnect(_ close: Bool = false) async {
        await state.disconnect(close: close)
    }
}

/// A fluent emitter that targets a single socket.
public struct SocketEmitOperator: Sendable {
    private let emitOperation: @Sendable (String, [SocketIOValue], (@Sendable ([SocketIOValue]) async -> Void)?) async -> Void

    init(
        emitOperation: @escaping @Sendable (String, [SocketIOValue], (@Sendable ([SocketIOValue]) async -> Void)?) async -> Void
    ) {
        self.emitOperation = emitOperation
    }

    /// Emits an event to this socket.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await emitOperation(event, arguments, nil)
    }

    /// Emits an event to this socket and waits for the peer acknowledgement callback.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    ///   - ack: The acknowledgement handler invoked when the client acknowledges the event.
    public func emit(
        _ event: String,
        arguments: [SocketIOValue] = [],
        ack: @escaping @Sendable ([SocketIOValue]) async -> Void
    ) async {
        await emitOperation(event, arguments, ack)
    }

    /// Encodes a payload and emits it as a single event argument.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The encodable payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        await emitOperation(event, [try SocketIOValue(encoding: payload)], nil)
    }

    /// Encodes a payload and emits it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await emit(event.rawValue, payload: payload)
    }
}
