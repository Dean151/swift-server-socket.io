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

import HTTPTypes

/// Handshake metadata associated with a connected socket.
public struct SocketHandshake: Sendable {
    /// The underlying Engine.IO session identifier.
    public let engineSessionID: String
    /// The HTTP request that established the Engine.IO session.
    public let request: HTTPRequest
    /// The optional auth payload sent by the Socket.IO client.
    public let auth: SocketIOValue?

    /// Creates handshake metadata.
    ///
    /// - Parameters:
    ///   - engineSessionID: The underlying Engine.IO session identifier.
    ///   - request: The HTTP request that established the Engine.IO session.
    ///   - auth: The optional auth payload sent by the Socket.IO client.
    public init(engineSessionID: String, request: HTTPRequest, auth: SocketIOValue?) {
        self.engineSessionID = engineSessionID
        self.request = request
        self.auth = auth
    }

    /// Decodes the auth payload as a strongly typed value.
    ///
    /// - Parameter type: The type to decode.
    /// - Returns: The decoded auth payload, or `nil` when the client sent no auth payload.
    public func decodeAuth<T: Decodable>(as type: T.Type = T.self) throws -> T? {
        guard let auth else { return nil }
        return try auth.decode(as: T.self)
    }
}
