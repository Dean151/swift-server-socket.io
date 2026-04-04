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

/// A serializable snapshot of the HTTP request that opened a remote socket.
public struct RemoteSocketRequest: Sendable, Codable, Equatable {
    /// The HTTP method.
    public let method: String
    /// The request scheme.
    public let scheme: String?
    /// The request authority.
    public let authority: String?
    /// The request path.
    public let path: String

    /// Creates a remote socket request snapshot.
    ///
    /// - Parameters:
    ///   - method: The HTTP method.
    ///   - scheme: The request scheme.
    ///   - authority: The request authority.
    ///   - path: The request path.
    public init(
        method: String,
        scheme: String?,
        authority: String?,
        path: String
    ) {
        self.method = method
        self.scheme = scheme
        self.authority = authority
        self.path = path
    }
}

/// A serializable snapshot of the handshake metadata associated with a remote socket.
public struct RemoteSocketHandshake: Sendable, Codable, Equatable {
    /// The underlying Engine.IO session identifier.
    public let engineSessionID: String
    /// The request snapshot associated with the session.
    public let request: RemoteSocketRequest
    /// The optional auth payload sent by the Socket.IO client.
    public let auth: SocketIOValue?

    /// Creates handshake metadata.
    ///
    /// - Parameters:
    ///   - engineSessionID: The Engine.IO session identifier.
    ///   - request: The request snapshot associated with the session.
    ///   - auth: The optional auth payload sent by the Socket.IO client.
    public init(engineSessionID: String, request: RemoteSocketRequest, auth: SocketIOValue?) {
        self.engineSessionID = engineSessionID
        self.request = request
        self.auth = auth
    }
}

/// A connected socket snapshot that may belong to the current node or another cluster node.
public struct RemoteSocket: Sendable {
    /// The namespace-scoped socket identifier.
    public let id: String
    /// The namespace this socket belongs to.
    public let namespace: String
    /// The handshake metadata captured when the socket was fetched.
    public let handshake: RemoteSocketHandshake
    /// The rooms the socket belonged to when it was fetched.
    public let rooms: Set<String>
    /// The user-managed socket data that was captured when the socket was fetched.
    public let data: [String: SocketIOValue]

    private let emitOperation: @Sendable (String, [SocketIOValue]) async -> Void
    private let joinOperation: @Sendable ([String]) async -> Void
    private let leaveOperation: @Sendable ([String]) async -> Void
    private let disconnectOperation: @Sendable (Bool) async -> Void

    init(
        id: String,
        namespace: String,
        handshake: RemoteSocketHandshake,
        rooms: Set<String>,
        data: [String: SocketIOValue],
        emitOperation: @escaping @Sendable (String, [SocketIOValue]) async -> Void,
        joinOperation: @escaping @Sendable ([String]) async -> Void,
        leaveOperation: @escaping @Sendable ([String]) async -> Void,
        disconnectOperation: @escaping @Sendable (Bool) async -> Void
    ) {
        self.id = id
        self.namespace = namespace
        self.handshake = handshake
        self.rooms = rooms
        self.data = data
        self.emitOperation = emitOperation
        self.joinOperation = joinOperation
        self.leaveOperation = leaveOperation
        self.disconnectOperation = disconnectOperation
    }

    /// Emits an event to this socket.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await emitOperation(event, arguments)
    }

    /// Encodes a payload and emits it as a single event argument.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        await emitOperation(event, [try SocketIOValue(encoding: payload)])
    }

    /// Encodes a payload and emits it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await emit(event.rawValue, payload: payload)
    }

    /// Adds this socket to multiple rooms in its namespace.
    ///
    /// - Parameter rooms: The rooms to join.
    public func join(_ rooms: [String]) async {
        await joinOperation(rooms)
    }

    /// Adds this socket to a room in its namespace.
    ///
    /// - Parameter room: The room to join.
    public func join(_ room: String) async {
        await join([room])
    }

    /// Removes this socket from multiple rooms in its namespace.
    ///
    /// The socket's own private room cannot be left explicitly.
    ///
    /// - Parameter rooms: The rooms to leave.
    public func leave(_ rooms: [String]) async {
        await leaveOperation(rooms)
    }

    /// Removes this socket from a room in its namespace.
    ///
    /// The socket's own private room cannot be left explicitly.
    ///
    /// - Parameter room: The room to leave.
    public func leave(_ room: String) async {
        await leave([room])
    }

    /// Disconnects this socket from its namespace.
    ///
    /// - Parameter close: When `true`, closes the underlying Engine.IO connection as well.
    public func disconnect(_ close: Bool = false) async {
        await disconnectOperation(close)
    }
}
