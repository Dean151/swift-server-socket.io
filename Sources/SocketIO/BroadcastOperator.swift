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

internal struct BroadcastTargets: Sendable {
    var includedRooms: Set<String>
    var excludedRooms: Set<String>
    var excludedSocketIDs: Set<String>
    var isLocalOnly: Bool

    init(
        includedRooms: Set<String> = [],
        excludedRooms: Set<String> = [],
        excludedSocketIDs: Set<String> = [],
        isLocalOnly: Bool = false
    ) {
        self.includedRooms = includedRooms
        self.excludedRooms = excludedRooms
        self.excludedSocketIDs = excludedSocketIDs
        self.isLocalOnly = isLocalOnly
    }
}

/// A fluent broadcaster that targets sockets by room membership.
public struct BroadcastOperator: Sendable {
    private var targets: BroadcastTargets
    private var volatility: EmitVolatility
    private let emitOperation: @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue]) async -> Void
    private let emitWithAckOperation: @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue], Duration) async throws -> [[SocketIOValue]]
    private let fetchSocketsOperation: @Sendable (BroadcastTargets) async -> [RemoteSocket]
    private let socketsJoinOperation: @Sendable (BroadcastTargets, [String]) async -> Void
    private let socketsLeaveOperation: @Sendable (BroadcastTargets, [String]) async -> Void
    private let disconnectSocketsOperation: @Sendable (BroadcastTargets, Bool) async -> Void

    init(
        targets: BroadcastTargets = .init(),
        volatility: EmitVolatility = .reliable,
        emitOperation: @escaping @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue]) async -> Void,
        emitWithAckOperation: @escaping @Sendable (BroadcastTargets, EmitVolatility, String, [SocketIOValue], Duration) async throws -> [[SocketIOValue]],
        fetchSocketsOperation: @escaping @Sendable (BroadcastTargets) async -> [RemoteSocket],
        socketsJoinOperation: @escaping @Sendable (BroadcastTargets, [String]) async -> Void,
        socketsLeaveOperation: @escaping @Sendable (BroadcastTargets, [String]) async -> Void,
        disconnectSocketsOperation: @escaping @Sendable (BroadcastTargets, Bool) async -> Void
    ) {
        self.targets = targets
        self.volatility = volatility
        self.emitOperation = emitOperation
        self.emitWithAckOperation = emitWithAckOperation
        self.fetchSocketsOperation = fetchSocketsOperation
        self.socketsJoinOperation = socketsJoinOperation
        self.socketsLeaveOperation = socketsLeaveOperation
        self.disconnectSocketsOperation = disconnectSocketsOperation
    }

    /// Returns a broadcaster that includes the given room.
    ///
    /// Multiple calls union their target rooms.
    ///
    /// - Parameter room: The room to include.
    public func to(_ room: String) -> Self {
        var copy = self
        copy.targets.includedRooms.insert(room)
        return copy
    }

    /// Returns a broadcaster that excludes the given room.
    ///
    /// - Parameter room: The room to exclude.
    public func excluding(_ room: String) -> Self {
        var copy = self
        copy.targets.excludedRooms.insert(room)
        return copy
    }

    func excluding(socketID: String) -> Self {
        var copy = self
        copy.targets.excludedSocketIDs.insert(socketID)
        return copy
    }

    /// Returns a broadcaster that sends best-effort packets without buffering.
    ///
    /// Volatile packets are dropped when the underlying transport cannot flush them immediately.
    public var volatile: Self {
        var copy = self
        copy.volatility = .volatile
        return copy
    }

    /// Returns a broadcaster that only targets sockets connected to the current node.
    public var local: Self {
        var copy = self
        copy.targets.isLocalOnly = true
        return copy
    }

    /// Broadcasts an event to every socket matched by this operator.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - arguments: The event arguments.
    public func emit(_ event: String, arguments: [SocketIOValue] = []) async {
        await emitOperation(targets, volatility, event, arguments)
    }

    /// Encodes a payload and broadcasts it as a single event argument.
    ///
    /// - Parameters:
    ///   - event: The event name.
    ///   - payload: The encodable payload to send.
    public func emit<T: Encodable>(_ event: String, payload: T) async throws {
        await emitOperation(targets, volatility, event, [try SocketIOValue(encoding: payload)])
    }

    /// Encodes a payload and broadcasts it using a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - payload: The payload to send.
    public func emit<T: Encodable>(_ event: SocketEventName<T>, payload: T) async throws {
        try await emit(event.rawValue, payload: payload)
    }

    /// Broadcasts an event and waits for one acknowledgement per targeted socket.
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
        try await emitWithAckOperation(targets, volatility, event, arguments, timeout)
    }

    /// Encodes a payload, broadcasts it, and waits for acknowledgements.
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
        try await emitWithAckOperation(targets, volatility, event, [try SocketIOValue(encoding: payload)], timeout)
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
        try await emit(event.rawValue, payload: payload, collectingAcksWithin: timeout)
    }

    /// Returns the sockets currently matched by this operator.
    public func fetchSockets() async -> [RemoteSocket] {
        await fetchSocketsOperation(targets)
    }

    /// Adds every matched socket to the given rooms.
    ///
    /// - Parameter rooms: The rooms to join.
    public func socketsJoin(_ rooms: [String]) async {
        await socketsJoinOperation(targets, rooms)
    }

    /// Adds every matched socket to the given room.
    ///
    /// - Parameter room: The room to join.
    public func socketsJoin(_ room: String) async {
        await socketsJoin([room])
    }

    /// Removes every matched socket from the given rooms.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter rooms: The rooms to leave.
    public func socketsLeave(_ rooms: [String]) async {
        await socketsLeaveOperation(targets, rooms)
    }

    /// Removes every matched socket from the given room.
    ///
    /// A socket's private room is preserved.
    ///
    /// - Parameter room: The room to leave.
    public func socketsLeave(_ room: String) async {
        await socketsLeave([room])
    }

    /// Disconnects every matched socket from the operator's namespace.
    ///
    /// - Parameter close: When `true`, closes the underlying Engine.IO connection as well.
    public func disconnectSockets(_ close: Bool = false) async {
        await disconnectSocketsOperation(targets, close)
    }
}
