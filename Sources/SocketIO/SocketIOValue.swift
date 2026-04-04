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
import Hummingbird

/// A Socket.IO-compatible event payload value.
public enum SocketIOValue: Sendable, Equatable {
    /// A string value.
    case string(String)
    /// A numeric value.
    case number(Double)
    /// A Boolean value.
    case bool(Bool)
    /// An object value.
    case object([String: SocketIOValue])
    /// An array value.
    case array([SocketIOValue])
    /// A null value.
    case null
    /// A binary attachment.
    case binary(ByteBuffer)
}

extension SocketIOValue: Codable {
    public init(from decoder: any Decoder) throws {
        let jsonValue = try JSONValue(from: decoder)
        self = .init(jsonValue: jsonValue)
    }

    public func encode(to encoder: any Encoder) throws {
        try jsonValue().encode(to: encoder)
    }
}

/// Errors thrown while encoding or decoding ``SocketIOValue`` values.
public enum SocketIOCodingError: Error, Equatable {
    /// Binary data cannot be represented in the JSON portion of a packet.
    case binaryCannotBeEncodedAsJSON
    /// The JSON payload could not be encoded or decoded.
    case invalidJSON
    /// A binary placeholder object was malformed.
    case invalidPlaceholder
    /// A referenced binary attachment was missing.
    case missingBinaryAttachment(Int)
}

extension SocketIOValue {
    private enum JSONValue: Codable {
        case string(String)
        case number(Double)
        case bool(Bool)
        case object([String: JSONValue])
        case array([JSONValue])
        case null

        init(from decoder: any Decoder) throws {
            let container = try decoder.singleValueContainer()
            if container.decodeNil() {
                self = .null
            } else if let bool = try? container.decode(Bool.self) {
                self = .bool(bool)
            } else if let number = try? container.decode(Double.self) {
                self = .number(number)
            } else if let string = try? container.decode(String.self) {
                self = .string(string)
            } else if let array = try? container.decode([JSONValue].self) {
                self = .array(array)
            } else if let object = try? container.decode([String: JSONValue].self) {
                self = .object(object)
            } else {
                throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value")
            }
        }

        func encode(to encoder: any Encoder) throws {
            var container = encoder.singleValueContainer()
            switch self {
            case .string(let value):
                try container.encode(value)
            case .number(let value):
                try container.encode(value)
            case .bool(let value):
                try container.encode(value)
            case .object(let value):
                try container.encode(value)
            case .array(let value):
                try container.encode(value)
            case .null:
                try container.encodeNil()
            }
        }
    }

    private init(jsonValue: JSONValue) {
        switch jsonValue {
        case .string(let string):
            self = .string(string)
        case .bool(let bool):
            self = .bool(bool)
        case .number(let number):
            self = .number(number)
        case .array(let array):
            self = .array(array.map(Self.init(jsonValue:)))
        case .object(let object):
            self = .object(object.mapValues(Self.init(jsonValue:)))
        case .null:
            self = .null
        }
    }

    private func jsonValue() throws -> JSONValue {
        switch self {
        case .string(let value):
            return .string(value)
        case .number(let value):
            return .number(value)
        case .bool(let value):
            return .bool(value)
        case .object(let value):
            return .object(try value.mapValues { try $0.jsonValue() })
        case .array(let value):
            return .array(try value.map { try $0.jsonValue() })
        case .null:
            return .null
        case .binary:
            throw SocketIOCodingError.binaryCannotBeEncodedAsJSON
        }
    }

    static func jsonString(_ string: String) throws -> SocketIOValue {
        guard let data = string.data(using: .utf8) else {
            throw SocketIOCodingError.invalidJSON
        }
        let jsonValue = try JSONDecoder().decode(JSONValue.self, from: data)
        return .init(jsonValue: jsonValue)
    }

    func encodeJSONString() throws -> String {
        let data = try JSONEncoder.sorted.encode(jsonValue())
        guard let string = String(data: data, encoding: .utf8) else {
            throw SocketIOCodingError.invalidJSON
        }
        return string
    }

    var containsBinary: Bool {
        switch self {
        case .binary:
            return true
        case .object(let value):
            return value.values.contains(where: \.containsBinary)
        case .array(let value):
            return value.contains(where: \.containsBinary)
        case .string, .number, .bool, .null:
            return false
        }
    }

    func replacingBinary(with attachments: inout [ByteBuffer]) -> SocketIOValue {
        switch self {
        case .binary(let buffer):
            let index = attachments.count
            attachments.append(buffer)
            return .object([
                "_placeholder": .bool(true),
                "num": .number(Double(index)),
            ])
        case .array(let value):
            return .array(value.map { $0.replacingBinary(with: &attachments) })
        case .object(let value):
            return .object(value.mapValues { $0.replacingBinary(with: &attachments) })
        case .string, .number, .bool, .null:
            return self
        }
    }

    func resolvingBinary(with attachments: [ByteBuffer]) throws -> SocketIOValue {
        switch self {
        case .array(let value):
            return .array(try value.map { try $0.resolvingBinary(with: attachments) })
        case .object(let value):
            if value["_placeholder"] == .bool(true) {
                guard case .number(let rawIndex)? = value["num"], rawIndex.rounded() == rawIndex else {
                    throw SocketIOCodingError.invalidPlaceholder
                }
                let index = Int(rawIndex)
                guard attachments.indices.contains(index) else {
                    throw SocketIOCodingError.missingBinaryAttachment(index)
                }
                return .binary(attachments[index])
            }
            return .object(try value.mapValues { try $0.resolvingBinary(with: attachments) })
        case .string, .number, .bool, .null, .binary:
            return self
        }
    }

    /// Encodes a Swift value as a Socket.IO payload value.
    ///
    /// - Parameter value: The value to encode.
    public init<T: Encodable>(encoding value: T) throws {
        let data = try JSONEncoder().encode(value)
        let jsonValue = try JSONDecoder().decode(JSONValue.self, from: data)
        self = .init(jsonValue: jsonValue)
    }

    /// Decodes a Socket.IO payload value into a Swift value.
    ///
    /// - Parameter type: The type to decode.
    public func decode<T: Decodable>(as type: T.Type = T.self) throws -> T {
        let data = try JSONEncoder().encode(jsonValue())
        return try JSONDecoder().decode(T.self, from: data)
    }
}

extension SocketIOValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .string(value)
    }
}

extension SocketIOValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .bool(value)
    }
}

extension SocketIOValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) {
        self = .number(Double(value))
    }
}

extension SocketIOValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) {
        self = .number(value)
    }
}

extension SocketIOValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: SocketIOValue...) {
        self = .array(elements)
    }
}

extension SocketIOValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, SocketIOValue)...) {
        self = .object(.init(uniqueKeysWithValues: elements))
    }
}

extension SocketIOValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .null
    }
}

extension Array where Element == SocketIOValue {
    func containsBinary() -> Bool {
        contains(where: \.containsBinary)
    }

    func replacingBinary(with attachments: inout [ByteBuffer]) -> [SocketIOValue] {
        map { $0.replacingBinary(with: &attachments) }
    }

    func resolvingBinary(with attachments: [ByteBuffer]) throws -> [SocketIOValue] {
        try map { try $0.resolvingBinary(with: attachments) }
    }
}

private extension JSONEncoder {
    static let sorted: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }()
}
