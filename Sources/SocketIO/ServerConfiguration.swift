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
import HTTPTypes
import EngineIO

/// The transport kinds that Socket.IO exposes through Engine.IO.
public typealias Transport = EngineIO.Transport

/// Configuration for a Socket.IO server.
public struct ServerConfiguration: Sendable {
    /// Decides whether an incoming Engine.IO request should be accepted.
    public typealias RequestAuthorizer = EngineIO.ServerConfiguration.RequestAuthorizer
    /// Generates a Socket.IO socket identifier from the Engine.IO session identifier and namespace.
    public typealias SocketIDGenerator = @Sendable (_ engineSessionID: String, _ namespace: String) -> String
    /// Decides whether a namespace connection should be accepted.
    public typealias NamespaceAuthorizer = @Sendable (NamespaceAuthorizationRequest) async throws -> NamespaceAuthorizationResult
    /// Creates the adapter used by a namespace.
    public typealias AdapterFactory = @Sendable (_ namespace: String) -> any SocketIOAdapter
    /// CORS behavior shared with the underlying Engine.IO server.
    public typealias Cors = EngineIO.ServerConfiguration.Cors

    /// Route-matching settings.
    public let routing: Routing
    /// Heartbeat and timeout settings.
    public let heartbeat: Heartbeat
    /// Transport-specific settings.
    public let transport: TransportConfiguration
    /// Namespace-specific settings.
    public let namespaces: Namespaces
    /// Cluster coordination settings.
    public let cluster: Cluster
    /// Connection state recovery settings.
    public let connectionStateRecovery: ConnectionStateRecovery?
    /// Request admission and namespace authorization hooks.
    public let policy: Policy
    /// CORS behavior for HTTP responses.
    public let cors: Cors

    /// Creates a configuration from grouped values.
    ///
    /// - Parameters:
    ///   - routing: Route-matching settings.
    ///   - heartbeat: Heartbeat and timeout settings.
    ///   - transport: Transport-specific settings.
    ///   - namespaces: Namespace-specific settings.
    ///   - cluster: Cluster coordination settings.
    ///   - policy: Request admission and namespace authorization hooks.
    ///   - cors: CORS behavior for HTTP responses.
    public init(
        routing: Routing = .init(),
        heartbeat: Heartbeat = .init(),
        transport: TransportConfiguration = .init(),
        namespaces: Namespaces = .init(),
        cluster: Cluster = .init(),
        connectionStateRecovery: ConnectionStateRecovery? = nil,
        policy: Policy = .init(),
        cors: Cors = .static(.init(allowedOrigin: .all))
    ) {
        self.routing = routing
        self.heartbeat = heartbeat
        self.transport = transport
        self.namespaces = namespaces
        self.cluster = cluster
        self.connectionStateRecovery = connectionStateRecovery
        self.policy = policy
        self.cors = cors
    }

    /// The HTTP path served by Socket.IO.
    public var path: String { routing.path }
    /// Whether both `path` and `path/` are accepted.
    public var addTrailingSlash: Bool { routing.allowsTrailingSlash }
    /// How long to wait for a heartbeat response.
    public var pingTimeout: Duration { heartbeat.pingTimeout }
    /// How often heartbeat pings are sent.
    public var pingInterval: Duration { heartbeat.pingInterval }
    /// How long an upgrade attempt may remain in progress.
    public var upgradeTimeout: Duration { heartbeat.upgradeTimeout }
    /// How long an Engine.IO session may stay idle before joining a namespace.
    public var connectTimeout: Duration { heartbeat.connectTimeout }
    /// The maximum payload size accepted for polling requests.
    public var maxHttpBufferSize: UInt { transport.maxPayload }
    /// The enabled Engine.IO transports.
    public var transports: Transport { transport.transports }
    /// The request admission hook.
    public var authorizeRequest: RequestAuthorizer { policy.authorizeRequest }
    /// The generator used for Socket.IO socket identifiers.
    public var socketIDGenerator: SocketIDGenerator { namespaces.socketIDGenerator }
    /// The namespace authorization hook.
    public var authorizeNamespaceConnection: NamespaceAuthorizer { policy.authorizeNamespaceConnection }
    /// The adapter factory used for namespaces.
    public var adapterFactory: AdapterFactory { namespaces.adapterFactory }
    /// The cluster coordinator used for inter-node communication.
    public var clusterCoordinator: (any SocketIOClusterCoordinator)? { cluster.coordinator }
    /// How long cluster request/response operations wait for peers.
    public var clusterRequestTimeout: Duration { cluster.requestTimeout }
    /// Whether polling sessions may upgrade to WebSocket.
    public var allowUpgrades: Bool { transport.allowUpgrades }
    /// The connection state recovery settings.
    public var recovery: ConnectionStateRecovery? { connectionStateRecovery }
}

extension ServerConfiguration {
    /// Route-matching settings for the Socket.IO endpoint.
    public struct Routing: Sendable {
        /// The HTTP path served by Socket.IO.
        public let path: String
        /// Whether both the base path and its trailing-slash variant are accepted.
        public let allowsTrailingSlash: Bool

        /// Creates route-matching settings.
        ///
        /// - Parameters:
        ///   - path: The HTTP path served by Socket.IO.
        ///   - allowsTrailingSlash: Whether both the base path and its trailing-slash variant are accepted.
        public init(
            path: String = "/socket.io",
            allowsTrailingSlash: Bool = true
        ) {
            self.path = path
            self.allowsTrailingSlash = allowsTrailingSlash
        }
    }

    /// Heartbeat and timeout settings.
    public struct Heartbeat: Sendable {
        /// How long to wait for the client to answer a ping.
        public let pingTimeout: Duration
        /// How often the server sends heartbeat pings.
        public let pingInterval: Duration
        /// How long an upgrade attempt may remain in progress.
        public let upgradeTimeout: Duration
        /// How long an Engine.IO session may remain idle before joining a namespace.
        public let connectTimeout: Duration

        /// Creates heartbeat settings.
        ///
        /// - Parameters:
        ///   - pingTimeout: How long to wait for the client to answer a ping.
        ///   - pingInterval: How often the server sends heartbeat pings.
        ///   - upgradeTimeout: How long an upgrade attempt may remain in progress.
        ///   - connectTimeout: How long an Engine.IO session may remain idle before joining a namespace.
        public init(
            pingTimeout: Duration = .seconds(20),
            pingInterval: Duration = .seconds(30),
            upgradeTimeout: Duration = .seconds(10),
            connectTimeout: Duration = .seconds(45)
        ) {
            self.pingTimeout = pingTimeout
            self.pingInterval = pingInterval
            self.upgradeTimeout = upgradeTimeout
            self.connectTimeout = connectTimeout
        }
    }

    /// Transport-specific settings for the Socket.IO server.
    public struct TransportConfiguration: Sendable {
        /// The transports exposed by the server.
        public let transports: Transport
        /// Whether polling sessions may upgrade to WebSocket.
        public let allowUpgrades: Bool
        /// The maximum payload size accepted for polling requests.
        public let maxPayload: UInt

        /// Creates transport settings.
        ///
        /// - Parameters:
        ///   - transports: The transports exposed by the server.
        ///   - allowUpgrades: Whether polling sessions may upgrade to WebSocket.
        ///   - maxPayload: The maximum polling payload size accepted from the client.
        public init(
            transports: Transport = [.polling, .websocket],
            allowUpgrades: Bool = true,
            maxPayload: UInt = 10_000
        ) {
            self.transports = transports
            self.allowUpgrades = allowUpgrades
            self.maxPayload = maxPayload
        }
    }

    /// Namespace-specific settings.
    public struct Namespaces: Sendable {
        /// The generator used for Socket.IO socket identifiers.
        public let socketIDGenerator: SocketIDGenerator
        /// The adapter factory used for namespaces.
        public let adapterFactory: AdapterFactory

        /// Creates namespace settings.
        ///
        /// - Parameters:
        ///   - socketIDGenerator: The generator used for Socket.IO socket identifiers.
        ///   - adapterFactory: The adapter factory used for namespaces.
        public init(
            socketIDGenerator: @escaping SocketIDGenerator = { _, _ in
                UUID().uuidString.filter { $0 != "-" }
            },
            adapterFactory: @escaping AdapterFactory = { _ in InMemoryAdapter() }
        ) {
            self.socketIDGenerator = socketIDGenerator
            self.adapterFactory = adapterFactory
        }
    }

    /// Cluster coordination settings.
    public struct Cluster: Sendable {
        /// The coordinator used for inter-node communication.
        public let coordinator: (any SocketIOClusterCoordinator)?
        /// How long cluster request/response operations wait for peers.
        public let requestTimeout: Duration
        /// The shared recovery store used for cross-node recovery.
        public let recoveryStore: (any SocketIOConnectionStateRecoveryStore)?

        /// Creates cluster settings.
        ///
        /// - Parameters:
        ///   - coordinator: The coordinator used for inter-node communication.
        ///   - requestTimeout: How long cluster request/response operations wait for peers.
        public init(
            coordinator: (any SocketIOClusterCoordinator)? = nil,
            requestTimeout: Duration = .seconds(5),
            recoveryStore: (any SocketIOConnectionStateRecoveryStore)? = nil
        ) {
            self.coordinator = coordinator
            self.requestTimeout = requestTimeout
            self.recoveryStore = recoveryStore
        }
    }

    /// Request admission and namespace authorization hooks.
    public struct Policy: Sendable {
        /// The request admission hook.
        public let authorizeRequest: RequestAuthorizer
        /// The namespace authorization hook.
        public let authorizeNamespaceConnection: NamespaceAuthorizer

        /// Creates policy hooks.
        ///
        /// - Parameters:
        ///   - authorizeRequest: The request admission hook.
        ///   - authorizeNamespaceConnection: The namespace authorization hook.
        public init(
            authorizeRequest: @escaping RequestAuthorizer = { _ in true },
            authorizeNamespaceConnection: @escaping NamespaceAuthorizer = { _ in .allow }
        ) {
            self.authorizeRequest = authorizeRequest
            self.authorizeNamespaceConnection = authorizeNamespaceConnection
        }
    }
}

/// Context provided when a client attempts to join a namespace.
public struct NamespaceAuthorizationRequest: Sendable {
    /// The Engine.IO session identifier.
    public let engineSessionID: String
    /// The namespace the client is attempting to join.
    public let namespace: String
    /// The optional auth payload provided by the client.
    public let auth: SocketIOValue?
    /// The HTTP request that established the Engine.IO session.
    public let request: HTTPRequest

    /// Creates a namespace authorization request.
    ///
    /// - Parameters:
    ///   - engineSessionID: The Engine.IO session identifier.
    ///   - namespace: The namespace the client is attempting to join.
    ///   - auth: The optional auth payload provided by the client.
    ///   - request: The HTTP request that established the Engine.IO session.
    public init(engineSessionID: String, namespace: String, auth: SocketIOValue?, request: HTTPRequest) {
        self.engineSessionID = engineSessionID
        self.namespace = namespace
        self.auth = auth
        self.request = request
    }
}

/// The result of evaluating a namespace connection.
public enum NamespaceAuthorizationResult: Sendable, Equatable {
    /// Accept the namespace connection.
    case allow
    /// Reject the namespace connection with a payload sent in a `CONNECT_ERROR` packet.
    case deny(SocketIOValue)

    /// Rejects a connection with a standard `{ "message": ... }` payload.
    ///
    /// - Parameter message: The message exposed to the client.
    public static func deny(message: String) -> Self {
        .deny(.object(["message": .string(message)]))
    }
}
