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

/// Why a Socket.IO socket disconnected from its namespace.
public enum SocketDisconnectReason: Sendable, Equatable {
    /// The client explicitly disconnected from the namespace.
    case clientNamespaceDisconnect
    /// The server explicitly disconnected the socket from the namespace.
    case serverNamespaceDisconnect
    /// The server is shutting down and closed the transport.
    case serverShuttingDown
    /// The transport heartbeat timed out.
    case pingTimeout
    /// The underlying Engine.IO transport closed.
    case transportClosed
    /// The underlying transport reported an error.
    case transportError
    /// The peer sent an invalid Socket.IO packet and the transport was closed.
    case parseError
    /// The connection was force-closed before a namespace could be joined.
    case forcedServerClose
    /// The underlying Engine.IO connection was force-closed.
    case forcedClose
    /// The transport timed out before the namespace could finish connecting.
    case connectTimeout
    /// A close reason that does not map to a known Socket.IO lifecycle reason.
    case unknown(String)
}

extension SocketDisconnectReason {
    /// The official Socket.IO disconnect reason string.
    public var socketIOReason: String {
        switch self {
        case .clientNamespaceDisconnect:
            "client namespace disconnect"
        case .serverNamespaceDisconnect:
            "server namespace disconnect"
        case .serverShuttingDown:
            "server shutting down"
        case .pingTimeout:
            "ping timeout"
        case .transportClosed:
            "transport close"
        case .transportError:
            "transport error"
        case .parseError:
            "parse error"
        case .forcedServerClose:
            "forced server close"
        case .forcedClose:
            "forced close"
        case .connectTimeout:
            "connect timeout"
        case .unknown(let value):
            value
        }
    }
}
