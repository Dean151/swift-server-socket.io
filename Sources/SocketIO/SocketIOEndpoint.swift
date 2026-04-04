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
import HTTPTypes
import Hummingbird
import HummingbirdWebSocket
import Logging

/// Installs Socket.IO routes into an existing Hummingbird router.
public struct SocketIOEndpoint: Sendable {
    /// The configuration used by this endpoint.
    public let configuration: ServerConfiguration

    private let core: ServerCore
    private let transport: EngineIOEndpoint

    /// Creates an endpoint with the provided configuration.
    ///
    /// - Parameter configuration: The Socket.IO server configuration to apply.
    public init(configuration: ServerConfiguration = .init()) {
        let core = ServerCore(configuration: configuration)
        self.configuration = configuration
        self.core = core
        self.transport = EngineIOEndpoint(configuration: .init(
            routing: .init(
                path: configuration.routing.path,
                allowsTrailingSlash: configuration.routing.allowsTrailingSlash
            ),
            heartbeat: .init(
                pingTimeout: configuration.heartbeat.pingTimeout,
                pingInterval: configuration.heartbeat.pingInterval,
                upgradeTimeout: configuration.heartbeat.upgradeTimeout
            ),
            transport: .init(
                transports: configuration.transport.transports,
                allowUpgrades: configuration.transport.allowUpgrades,
                maxPayload: configuration.transport.maxPayload
            ),
            cors: configuration.cors,
            lifecycle: .init(
                onConnect: { connection in
                    await core.transportConnected(connection)
                },
                onMessage: { connection, data in
                    await core.transportReceived(connection, data: data)
                },
                onDisconnect: { connection, reason in
                    await core.transportClosed(connection, reason: reason)
                }
            ),
            policy: .init(
                requestAdmission: { request in
                    if try await configuration.policy.authorizeRequest(request) {
                        return .allow
                    } else {
                        return .reject(.forbidden())
                    }
                }
            )
        ))
    }

    /// The WebSocket server configuration that must be passed to Hummingbird's upgrade server.
    public var webSocketConfiguration: WebSocketServerConfiguration {
        transport.webSocketConfiguration
    }

    /// Registers the Socket.IO HTTP routes into a router.
    ///
    /// - Parameter router: The router that should serve the Socket.IO endpoint.
    public func install(into router: Router<BasicRequestContext>) {
        transport.install(into: router)
    }

    /// Validates and prepares a WebSocket upgrade request.
    ///
    /// - Parameters:
    ///   - request: The incoming HTTP upgrade request.
    ///   - logger: The logger associated with the request.
    /// - Returns: The Hummingbird upgrade decision for the request.
    public func shouldUpgrade(
        request: HTTPRequest,
        logger: Logger
    ) async -> ShouldUpgradeResult<WebSocketDataHandler<HTTP1WebSocketUpgradeChannel.Context>> {
        await transport.shouldUpgrade(request: request, logger: logger)
    }

    /// Stops accepting new Socket.IO sessions and closes all currently connected clients.
    public func close() async {
        await core.prepareForServerShutdown()
        await transport.close()
        await core.stopCluster()
    }

    /// Registers a connection handler for the root namespace.
    ///
    /// - Parameter handler: The handler invoked for each connected socket in `/`.
    public func onConnection(_ handler: @escaping Namespace.ConnectionHandler) {
        core.addConnectionHandler(for: "/", handler: handler)
    }

    /// Registers a connect-time middleware for the root namespace.
    ///
    /// - Parameter middleware: The middleware invoked before a socket joins `/`.
    public func use(_ middleware: @escaping NamespaceMiddleware) {
        core.addNamespaceMiddleware(for: "/", middleware: middleware)
    }

    /// Returns a namespace handle for the given path.
    ///
    /// - Parameter path: The namespace path to access.
    public func namespace(_ path: String) -> Namespace {
        let normalizedPath = normalizedNamespace(path)
        core.registerNamespace(normalizedPath)
        return Namespace(path: normalizedPath, core: core)
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
        let id = core.registerDynamicParentNamespace(
            options: options,
            matcher: { path in
                path.wholeMatch(of: pattern) != nil
            }
        )
        return ParentNamespace(id: id, core: core)
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
        let id = core.registerDynamicParentNamespace(options: options, matcher: predicate)
        return ParentNamespace(id: id, core: core)
    }

    /// Creates a broadcaster targeting a room in the root namespace.
    ///
    /// - Parameter room: The room to include.
    public func to(_ room: String) -> BroadcastOperator {
        namespace("/").to(room)
    }

    /// Creates a broadcaster excluding a room in the root namespace.
    ///
    /// - Parameter room: The room to exclude.
    public func excluding(_ room: String) -> BroadcastOperator {
        namespace("/").excluding(room)
    }

    /// A best-effort broadcaster for the root namespace.
    ///
    /// Volatile packets are dropped when the underlying transport cannot flush them immediately.
    public var volatile: BroadcastOperator {
        namespace("/").volatile
    }

    /// A broadcaster that only targets sockets connected to the current node.
    public var local: BroadcastOperator {
        namespace("/").local
    }

    /// Broadcasts an event to every socket in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await namespace("/").emit(event, arguments: arguments)
    }

    /// Encodes a payload and broadcasts it to every socket in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        try await namespace("/").emit(event, payload: payload)
    }

    /// Encodes a payload and broadcasts it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await namespace("/").emit(event, payload: payload)
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
        try await namespace("/").emit(event, arguments: arguments, collectingAcksWithin: timeout)
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
        try await namespace("/").emit(event, payload: payload, collectingAcksWithin: timeout)
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
        try await namespace("/").emit(event, payload: payload, collectingAcksWithin: timeout)
    }

    /// Returns the sockets currently connected in the root namespace.
    public func fetchSockets() async -> [RemoteSocket] {
        await namespace("/").fetchSockets()
    }

    /// Adds every socket in the root namespace to the given rooms.
    ///
    /// - Parameter rooms: The rooms to join.
    public func socketsJoin(_ rooms: [String]) async {
        await namespace("/").socketsJoin(rooms)
    }

    /// Adds every socket in the root namespace to the given room.
    ///
    /// - Parameter room: The room to join.
    public func socketsJoin(_ room: String) async {
        await namespace("/").socketsJoin(room)
    }

    /// Removes every socket in the root namespace from the given rooms.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter rooms: The rooms to leave.
    public func socketsLeave(_ rooms: [String]) async {
        await namespace("/").socketsLeave(rooms)
    }

    /// Removes every socket in the root namespace from the given room.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter room: The room to leave.
    public func socketsLeave(_ room: String) async {
        await namespace("/").socketsLeave(room)
    }

    /// Disconnects every socket in the root namespace.
    ///
    /// - Parameter close: When `true`, closes the underlying Engine.IO connection as well.
    public func disconnectSockets(_ close: Bool = false) async {
        await namespace("/").disconnectSockets(close)
    }

    /// Emits an event to every other server connected to the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func serverSideEmit(_ event: String, arguments: [SocketIOValue] = []) async {
        await namespace("/").serverSideEmit(event, arguments: arguments)
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
        try await namespace("/").serverSideEmit(event, arguments: arguments, collectingAcksWithin: timeout)
    }

    /// Registers a handler for server-side events emitted by other nodes in the root namespace.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - handler: The handler invoked when the event is received.
    public func onServerEvent(_ event: String, handler: @escaping ServerSideEventHandler) {
        namespace("/").onServerEvent(event, handler: handler)
    }

    private func normalizedNamespace(_ path: String) -> String {
        if path.isEmpty || path == "/" {
            return "/"
        }
        return path.hasPrefix("/") ? path : "/\(path)"
    }
}
