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

/// A typed Socket.IO event name.
public struct SocketEventName<Payload>: RawRepresentable, ExpressibleByStringLiteral, Sendable, Hashable {
    /// The raw event name.
    public let rawValue: String

    /// Creates an event name from its raw string value.
    ///
    /// - Parameter rawValue: The Socket.IO event name.
    public init(rawValue: String) {
        self.rawValue = rawValue
    }

    /// Creates an event name from its raw string value.
    ///
    /// - Parameter rawValue: The Socket.IO event name.
    public init(_ rawValue: String) {
        self.rawValue = rawValue
    }

    public init(stringLiteral value: String) {
        self.rawValue = value
    }
}

/// Errors thrown while decoding inbound event arguments.
public enum SocketEventDecodingError: Error, Sendable, Equatable {
    /// The event did not contain an argument at the requested index.
    case missingArgument(event: String, index: Int)
    /// The argument at the requested index could not be decoded as the requested type.
    case invalidArgument(event: String, index: Int, value: SocketIOValue)
}

/// An event delivered to a socket event handler.
public struct SocketEvent: Sendable, Equatable {
    /// The event name.
    public let name: String
    /// The event arguments.
    public let arguments: [SocketIOValue]

    /// Creates an event value.
    ///
    /// - Parameters:
    ///   - name: The event name.
    ///   - arguments: The event arguments.
    public init(name: String, arguments: [SocketIOValue]) {
        self.name = name
        self.arguments = arguments
    }

    /// Decodes an event argument as a strongly typed value.
    ///
    /// - Parameters:
    ///   - index: The argument index to decode. Defaults to the first argument.
    ///   - type: The type to decode.
    public func decode<T: Decodable>(_ index: Int = 0, as type: T.Type = T.self) throws -> T {
        guard arguments.indices.contains(index) else {
            throw SocketEventDecodingError.missingArgument(event: name, index: index)
        }
        do {
            return try arguments[index].decode(as: T.self)
        } catch {
            throw SocketEventDecodingError.invalidArgument(event: name, index: index, value: arguments[index])
        }
    }

    /// Decodes the first event argument as the payload for a typed event name.
    ///
    /// - Parameters:
    ///   - event: The typed event name.
    ///   - type: The payload type to decode.
    public func decode<T: Decodable>(
        _ event: SocketEventName<T>,
        as type: T.Type = T.self
    ) throws -> T {
        try decode(0, as: T.self)
    }
}
