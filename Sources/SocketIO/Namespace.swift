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

/// A namespace-scoped view over the Socket.IO server.
public struct Namespace: Sendable {
    /// The connection handler invoked for newly connected sockets.
    public typealias ConnectionHandler = @Sendable (Socket) async -> Void

    /// The namespace path.
    public let path: String
    private let core: ServerCore

    init(path: String, core: ServerCore) {
        self.path = path
        self.core = core
    }

    /// The namespace path.
    ///
    /// This mirrors Socket.IO's `name` property.
    public var name: String { path }

    /// Registers a handler for sockets that join this namespace.
    ///
    /// - Parameter handler: The handler invoked once the namespace connection is accepted.
    public func onConnection(_ handler: @escaping ConnectionHandler) {
        core.addConnectionHandler(for: path, handler: handler)
    }

    /// Registers a connect-time middleware for this namespace.
    ///
    /// - Parameter middleware: The middleware invoked before a socket joins the namespace.
    public func use(_ middleware: @escaping NamespaceMiddleware) {
        core.addNamespaceMiddleware(for: path, middleware: middleware)
    }

    /// Creates a broadcaster targeting a room in this namespace.
    ///
    /// - Parameter room: The room to include.
    public func to(_ room: String) -> BroadcastOperator {
        broadcaster.to(room)
    }

    /// Creates a broadcaster excluding a room in this namespace.
    ///
    /// - Parameter room: The room to exclude.
    public func excluding(_ room: String) -> BroadcastOperator {
        broadcaster.excluding(room)
    }

    /// A best-effort broadcaster for this namespace.
    ///
    /// Volatile packets are dropped when the underlying transport cannot flush them immediately.
    public var volatile: BroadcastOperator {
        broadcaster.volatile
    }

    /// A broadcaster that only targets sockets connected to the current node.
    public var local: BroadcastOperator {
        broadcaster.local
    }

    /// Broadcasts an event to every socket matched by this namespace broadcaster.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await broadcaster.emit(event, arguments: arguments)
    }

    /// Encodes a payload and broadcasts it as a single event argument.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The encodable payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        try await broadcaster.emit(event, payload: payload)
    }

    /// Encodes a payload and broadcasts it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await broadcaster.emit(event, payload: payload)
    }

    /// Broadcasts an event to every socket matched by this namespace broadcaster and waits for acknowledgements.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    ///   - timeout: How long to wait for all acknowledgements.
    /// - Returns: The ordered acknowledgement payloads returned by the targeted sockets.
    public func emit(
        _ event: String,
        arguments: [SocketIOValue] = [],
        collectingAcksWithin timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        try await broadcaster.emit(event, arguments: arguments, collectingAcksWithin: timeout)
    }

    /// Encodes a payload, broadcasts it to every socket matched by this namespace broadcaster, and waits for acknowledgements.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The encodable payload to send.
    ///   - timeout: How long to wait for all acknowledgements.
    /// - Returns: The ordered acknowledgement payloads returned by the targeted sockets.
    public func emit<T: Encodable>(
        _ event: String,
        payload: T,
        collectingAcksWithin timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        try await broadcaster.emit(event, payload: payload, collectingAcksWithin: timeout)
    }

    /// Encodes a payload, broadcasts it using a typed event name, and waits for acknowledgements.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    ///   - timeout: How long to wait for all acknowledgements.
    public func emit<T: Encodable>(
        _ event: SocketEventName<T>,
        payload: T,
        collectingAcksWithin timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        try await broadcaster.emit(event, payload: payload, collectingAcksWithin: timeout)
    }

    /// Returns the sockets currently connected in this namespace.
    public func fetchSockets() async -> [RemoteSocket] {
        await broadcaster.fetchSockets()
    }

    /// Adds every socket in this namespace to the given rooms.
    ///
    /// - Parameter rooms: The rooms to join.
    public func socketsJoin(_ rooms: [String]) async {
        await core.join(rooms: rooms, in: path, targets: .init())
    }

    /// Adds every socket in this namespace to the given room.
    ///
    /// - Parameter room: The room to join.
    public func socketsJoin(_ room: String) async {
        await socketsJoin([room])
    }

    /// Removes every socket in this namespace from the given rooms.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter rooms: The rooms to leave.
    public func socketsLeave(_ rooms: [String]) async {
        await core.leave(rooms: rooms, in: path, targets: .init())
    }

    /// Removes every socket in this namespace from the given room.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter room: The room to leave.
    public func socketsLeave(_ room: String) async {
        await socketsLeave([room])
    }

    /// Disconnects every socket in this namespace.
    ///
    /// - Parameter close: When `true`, closes the underlying Engine.IO connection as well.
    public func disconnectSockets(_ close: Bool = false) async {
        await core.disconnectSockets(in: path, targets: .init(), close: close)
    }

    /// Emits an event to every other server connected to this namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func serverSideEmit(_ event: String, arguments: [SocketIOValue] = []) async {
        await core.serverSideEmit(in: path, event: event, items: arguments)
    }

    /// Emits an event to every other server connected to this namespace and waits for acknowledgements.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    ///   - timeout: How long to wait for all acknowledgements.
    /// - Returns: The ordered acknowledgement payloads returned by the target nodes.
    public func serverSideEmit(
        _ event: String,
        arguments: [SocketIOValue] = [],
        collectingAcksWithin timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        try await core.serverSideEmitExpectingAcks(in: path, event: event, items: arguments, timeout: timeout)
    }

    /// Registers a handler for server-side events emitted by other nodes in this namespace.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - handler: The handler invoked when the event is received.
    public func onServerEvent(_ event: String, handler: @escaping ServerSideEventHandler) {
        core.addServerEventHandler(for: path, event: event, handler: handler)
    }


    private var broadcaster: BroadcastOperator {
        BroadcastOperator(
            emitOperation: { [core, path] targets, volatility, event, items in
                await core.broadcast(in: path, targets: targets, volatility: volatility, event: event, items: items)
            },
            emitWithAckOperation: { [core, path] targets, volatility, event, items, timeout in
                try await core.broadcastExpectingAcks(
                    in: path,
                    targets: targets,
                    volatility: volatility,
                    event: event,
                    items: items,
                    timeout: timeout
                )
            },
            fetchSocketsOperation: { [core, path] targets in
                await core.fetchSockets(in: path, targets: targets)
            },
            socketsJoinOperation: { [core, path] targets, rooms in
                await core.join(rooms: rooms, in: path, targets: targets)
            },
            socketsLeaveOperation: { [core, path] targets, rooms in
                await core.leave(rooms: rooms, in: path, targets: targets)
            },
            disconnectSocketsOperation: { [core, path] targets, close in
                await core.disconnectSockets(in: path, targets: targets, close: close)
            }
        )
    }
}
