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

import Hummingbird
import ServiceLifecycle

/// A convenience Hummingbird service that hosts a Socket.IO endpoint.
public struct Server: Service {
    /// The Socket.IO endpoint exposed by the server.
    public let endpoint: SocketIOEndpoint
    /// The underlying Hummingbird application.
    public let application: Application<RouterResponder<BasicRequestContext>>

    /// Creates a server bound to a TCP port.
    ///
    /// - Parameters:
    ///   - port: The port to bind on `0.0.0.0`.
    ///   - configuration: The Socket.IO server configuration to apply.
    public init(port: Int, configuration: ServerConfiguration = .init()) {
        self.init(host: "0.0.0.0", port: port, configuration: configuration)
    }

    /// Creates a server bound to a given host and TCP port.
    ///
    /// - Parameters:
    ///   - host: The host to bind.
    ///   - port: The port to bind.
    ///   - configuration: The Socket.IO server configuration to apply.
    public init(host: String, port: Int, configuration: ServerConfiguration = .init()) {
        let endpoint = SocketIOEndpoint(configuration: configuration)
        let router = Router()
        router.add(middleware: LogRequestsMiddleware(.debug))
        endpoint.install(into: router)
        self.endpoint = endpoint
        self.application = Application(
            router: router,
            server: .http1WebSocketUpgrade(configuration: endpoint.webSocketConfiguration) { request, _, logger in
                await endpoint.shouldUpgrade(request: request, logger: logger)
            },
            configuration: .init(address: .hostname(host, port: port))
        )
    }

    /// Starts the underlying Hummingbird application.
    public func run() async throws {
        try await application.runService()
    }

    /// Stops accepting new Socket.IO sessions and closes all currently connected clients.
    ///
    /// This only shuts down the Socket.IO transport layer. The surrounding Hummingbird application
    /// continues to run until its own lifecycle is stopped.
    public func close() async {
        await endpoint.close()
    }

    /// Registers a connection handler for the root namespace.
    ///
    /// - Parameter handler: The handler invoked for each connected socket in `/`.
    public func onConnection(_ handler: @escaping Namespace.ConnectionHandler) {
        endpoint.onConnection(handler)
    }

    /// Registers a connect-time middleware for the root namespace.
    ///
    /// - Parameter middleware: The middleware invoked before a socket joins `/`.
    public func use(_ middleware: @escaping NamespaceMiddleware) {
        endpoint.use(middleware)
    }

    /// Returns a namespace handle for the given path.
    ///
    /// - Parameter path: The namespace path to access.
    public func namespace(_ path: String) -> Namespace {
        endpoint.namespace(path)
    }

    /// Creates a dynamic parent namespace that matches child namespaces with a regular expression.
    ///
    /// - Parameters:
    ///   - pattern: The regular expression used to match child namespace paths.
    ///   - options: The options applied to dynamically created children.
    public func dynamicNamespace<Output>(
        matching pattern: Regex<Output>,
        options: ParentNamespaceOptions = .init()
    ) -> ParentNamespace {
        endpoint.dynamicNamespace(matching: pattern, options: options)
    }

    /// Creates a dynamic parent namespace that matches child namespaces with a predicate.
    ///
    /// - Parameters:
    ///   - predicate: The predicate used to match child namespace paths.
    ///   - options: The options applied to dynamically created children.
    public func dynamicNamespace(
        where predicate: @escaping @Sendable (String) -> Bool,
        options: ParentNamespaceOptions = .init()
    ) -> ParentNamespace {
        endpoint.dynamicNamespace(where: predicate, options: options)
    }

    /// Creates a broadcaster targeting a room in the root namespace.
    ///
    /// - Parameter room: The room to include.
    public func to(_ room: String) -> BroadcastOperator {
        endpoint.to(room)
    }

    /// Creates a broadcaster excluding a room in the root namespace.
    ///
    /// - Parameter room: The room to exclude.
    public func excluding(_ room: String) -> BroadcastOperator {
        endpoint.excluding(room)
    }

    /// A best-effort broadcaster for the root namespace.
    ///
    /// Volatile packets are dropped when the underlying transport cannot flush them immediately.
    public var volatile: BroadcastOperator {
        endpoint.volatile
    }

    /// A broadcaster that only targets sockets connected to the current node.
    public var local: BroadcastOperator {
        endpoint.local
    }

    /// Broadcasts an event to every socket in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await endpoint.namespace("/").emit(event, arguments: arguments)
    }

    /// Encodes a payload and broadcasts it to every socket in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        try await endpoint.namespace("/").emit(event, payload: payload)
    }

    /// Encodes a payload and broadcasts it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await endpoint.namespace("/").emit(event, payload: payload)
    }

    /// Broadcasts an event to every socket in the root namespace and waits for acknowledgements.
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
        try await endpoint.namespace("/").emit(event, arguments: arguments, collectingAcksWithin: timeout)
    }

    /// Encodes a payload, broadcasts it to every socket in the root namespace, and waits for acknowledgements.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The payload to send.
    ///   - timeout: How long to wait for all acknowledgements.
    public func emit<T: Encodable>(
        _ event: String,
        payload: T,
        collectingAcksWithin timeout: Duration
    ) async throws -> [[SocketIOValue]] {
        try await endpoint.namespace("/").emit(event, payload: payload, collectingAcksWithin: timeout)
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
        try await endpoint.namespace("/").emit(event, payload: payload, collectingAcksWithin: timeout)
    }

    /// Returns the sockets currently connected in the root namespace.
    public func fetchSockets() async -> [RemoteSocket] {
        await endpoint.namespace("/").fetchSockets()
    }

    /// Adds every socket in the root namespace to the given rooms.
    ///
    /// - Parameter rooms: The rooms to join.
    public func socketsJoin(_ rooms: [String]) async {
        await endpoint.namespace("/").socketsJoin(rooms)
    }

    /// Adds every socket in the root namespace to the given room.
    ///
    /// - Parameter room: The room to join.
    public func socketsJoin(_ room: String) async {
        await endpoint.namespace("/").socketsJoin(room)
    }

    /// Removes every socket in the root namespace from the given rooms.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter rooms: The rooms to leave.
    public func socketsLeave(_ rooms: [String]) async {
        await endpoint.namespace("/").socketsLeave(rooms)
    }

    /// Removes every socket in the root namespace from the given room.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter room: The room to leave.
    public func socketsLeave(_ room: String) async {
        await endpoint.namespace("/").socketsLeave(room)
    }

    /// Disconnects every socket in the root namespace.
    ///
    /// - Parameter close: When `true`, closes the underlying Engine.IO connection as well.
    public func disconnectSockets(_ close: Bool = false) async {
        await endpoint.namespace("/").disconnectSockets(close)
    }

    /// Emits an event to every other server connected to the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func serverSideEmit(_ event: String, arguments: [SocketIOValue] = []) async {
        await endpoint.namespace("/").serverSideEmit(event, arguments: arguments)
    }

    /// Emits an event to every other server connected to the root namespace and waits for acknowledgements.
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
        try await endpoint.namespace("/").serverSideEmit(event, arguments: arguments, collectingAcksWithin: timeout)
    }

    /// Registers a handler for server-side events emitted by other nodes in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - handler: The handler invoked when the event is received.
    public func onServerEvent(_ event: String, handler: @escaping ServerSideEventHandler) {
        endpoint.namespace("/").onServerEvent(event, handler: handler)
    }
}
