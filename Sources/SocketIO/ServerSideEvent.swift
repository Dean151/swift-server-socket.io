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

/// Handles a named server-side event and its optional acknowledgement channel.
public typealias ServerSideEventHandler = @Sendable (SocketEvent, ServerSideAck?) async -> Void

/// Thrown when a server-side acknowledgement request times out before every node replies.
public struct ServerSideAckTimeoutError: Error, Sendable, Equatable {
    /// The responses that arrived before the timeout, ordered by node identifier.
    public let responses: [[SocketIOValue]]
    /// The number of responses received before the timeout.
    public let receivedCount: Int
    /// The total number of responses expected.
    public let expectedCount: Int
    /// The number of responses still missing when the timeout elapsed.
    public let missingCount: Int

    /// Creates a timeout error.
    ///
    /// - Parameters:
    ///   - responses: The responses received before the timeout.
    ///   - receivedCount: The number of responses received before the timeout.
    ///   - expectedCount: The total number of responses expected.
    ///   - missingCount: The number of responses still missing.
    public init(
        responses: [[SocketIOValue]],
        receivedCount: Int,
        expectedCount: Int,
        missingCount: Int
    ) {
        self.responses = responses
        self.receivedCount = receivedCount
        self.expectedCount = expectedCount
        self.missingCount = missingCount
    }
}

/// Sends a single acknowledgement response for an incoming server-side event.
public struct ServerSideAck: Sendable {
    private let gate: SocketAckGate
    private let sendOperation: @Sendable ([SocketIOValue]) async -> Void

    init(
        gate: SocketAckGate = .init(),
        sendOperation: @escaping @Sendable ([SocketIOValue]) async -> Void
    ) {
        self.gate = gate
        self.sendOperation = sendOperation
    }

    /// Sends acknowledgement arguments.
    ///
    /// - Parameter items: The acknowledgement arguments.
    public func send(arguments items: [SocketIOValue]) async throws {
        guard await gate.claim() else {
            throw SocketAckError.alreadySent
        }
        await sendOperation(items)
    }

    /// Encodes and sends a single acknowledgement payload.
    ///
    /// - Parameter payload: The encodable payload to send.
    public func send<T: Encodable>(_ payload: T) async throws {
        try await send(arguments: [SocketIOValue(encoding: payload)])
    }
}

