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

/// Errors thrown while using a Socket.IO acknowledgement channel.
public enum SocketAckError: Error, Equatable {
    /// The acknowledgement was already sent.
    case alreadySent
}

actor SocketAckGate {
    private var didSend = false

    func claim() -> Bool {
        guard !didSend else { return false }
        didSend = true
        return true
    }
}

/// Sends a single acknowledgement response for an incoming event.
public struct SocketAck: Sendable {
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
