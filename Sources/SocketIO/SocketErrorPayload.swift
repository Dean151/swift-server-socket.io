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

/// The payload emitted with local socket `error` events.
public struct SocketErrorPayload: Sendable, Equatable, Codable {
    /// The human-readable error message.
    public let message: String
    /// Additional structured payload data when available.
    public let data: SocketIOValue?

    /// Creates an error payload.
    ///
    /// - Parameters:
    ///   - message: The human-readable error message.
    ///   - data: Optional structured payload data.
    public init(message: String, data: SocketIOValue? = nil) {
        self.message = message
        self.data = data
    }
}

func socketErrorPayload(from error: any Error) -> SocketErrorPayload {
    if let middlewareError = error as? any SocketIOMiddlewareError {
        return .init(message: middlewareError.message, data: middlewareError.socketIOData)
    } else {
        return .init(message: String(describing: error))
    }
}

func socketIOPayload(from error: any Error) -> SocketIOValue {
    let payload = socketErrorPayload(from: error)
    var value: [String: SocketIOValue] = ["message": .string(payload.message)]
    if let data = payload.data {
        value["data"] = data
    }
    return .object(value)
}
