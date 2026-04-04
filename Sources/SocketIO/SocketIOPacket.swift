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

import Hummingbird

enum SocketIOPacketType: Int, Equatable {
    case connect = 0
    case disconnect
    case event
    case ack
    case connectError
    case binaryEvent
    case binaryAck
}

enum SocketIOPacketDecodingError: Error, Equatable {
    case invalidPacket(String)
}

enum SocketIOPacket: Equatable {
    case connect(namespace: String, auth: SocketIOValue?)
    case disconnect(namespace: String)
    case event(namespace: String, items: [SocketIOValue], ackID: Int?)
    case ack(namespace: String, items: [SocketIOValue], ackID: Int)
    case connectError(namespace: String, data: SocketIOValue)
}

struct EncodedSocketIOPacket: Equatable {
    let text: String
    let attachments: [ByteBuffer]
}

struct PendingSocketIOPacket {
    let type: SocketIOPacketType
    let namespace: String
    let ackID: Int?
    let payload: SocketIOValue?
    let expectedAttachments: Int

    func complete(with attachments: [ByteBuffer]) throws -> SocketIOPacket {
        guard attachments.count == expectedAttachments else {
            throw SocketIOPacketDecodingError.invalidPacket("Unexpected binary attachment count")
        }

        switch type {
        case .binaryEvent:
            guard case .array(let items)? = payload else {
                throw SocketIOPacketDecodingError.invalidPacket("EVENT payload must be a non-empty array")
            }
            let resolved = try items.resolvingBinary(with: attachments)
            guard !resolved.isEmpty else {
                throw SocketIOPacketDecodingError.invalidPacket("EVENT payload must be a non-empty array")
            }
            return .event(namespace: namespace, items: resolved, ackID: ackID)
        case .binaryAck:
            guard case .array(let items)? = payload, let ackID else {
                throw SocketIOPacketDecodingError.invalidPacket("ACK payload must be an array")
            }
            let resolved = try items.resolvingBinary(with: attachments)
            return .ack(namespace: namespace, items: resolved, ackID: ackID)
        default:
            throw SocketIOPacketDecodingError.invalidPacket("Only binary event packets may await attachments")
        }
    }
}

enum SocketIOPacketDecodeResult {
    case packet(SocketIOPacket)
    case pending(PendingSocketIOPacket)
}

extension SocketIOPacket {
    var namespace: String {
        switch self {
        case .connect(let namespace, _),
             .disconnect(let namespace),
             .event(let namespace, _, _),
             .ack(let namespace, _, _),
             .connectError(let namespace, _):
            return namespace
        }
    }

    static func decode(from text: String) throws -> SocketIOPacketDecodeResult {
        guard let first = text.first, let rawType = Int(String(first)), let type = SocketIOPacketType(rawValue: rawType) else {
            throw SocketIOPacketDecodingError.invalidPacket("Unknown packet type")
        }

        var index = text.index(after: text.startIndex)
        var expectedAttachments = 0
        if type == .binaryEvent || type == .binaryAck {
            let start = index
            while index < text.endIndex, text[index].isNumber {
                index = text.index(after: index)
            }
            guard index < text.endIndex, text[index] == "-", start != index else {
                throw SocketIOPacketDecodingError.invalidPacket("Missing binary attachment count")
            }
            expectedAttachments = Int(text[start..<index]) ?? 0
            index = text.index(after: index)
        }

        let namespace: String
        if index < text.endIndex, text[index] == "/" {
            let start = index
            while index < text.endIndex, text[index] != "," {
                index = text.index(after: index)
            }
            namespace = String(text[start..<index])
            if index < text.endIndex, text[index] == "," {
                index = text.index(after: index)
            }
        } else {
            namespace = "/"
        }

        let ackStart = index
        while index < text.endIndex, text[index].isNumber {
            index = text.index(after: index)
        }
        let ackID = ackStart == index ? nil : Int(text[ackStart..<index])

        let payloadText = String(text[index...])
        let payload: SocketIOValue?
        if payloadText.isEmpty {
            payload = nil
        } else {
            payload = try .jsonString(payloadText)
        }

        switch type {
        case .connect:
            return .packet(.connect(namespace: namespace, auth: payload))
        case .disconnect:
            return .packet(.disconnect(namespace: namespace))
        case .connectError:
            guard let payload else {
                throw SocketIOPacketDecodingError.invalidPacket("CONNECT_ERROR payload must be an object")
            }
            return .packet(.connectError(namespace: namespace, data: payload))
        case .event:
            guard case .array(let items)? = payload, !items.isEmpty else {
                throw SocketIOPacketDecodingError.invalidPacket("EVENT payload must be a non-empty array")
            }
            return .packet(.event(namespace: namespace, items: items, ackID: ackID))
        case .ack:
            guard case .array(let items)? = payload, let ackID else {
                throw SocketIOPacketDecodingError.invalidPacket("ACK payload must be an array")
            }
            return .packet(.ack(namespace: namespace, items: items, ackID: ackID))
        case .binaryEvent, .binaryAck:
            guard let payload else {
                throw SocketIOPacketDecodingError.invalidPacket("Binary packet payload is required")
            }
            return .pending(.init(
                type: type,
                namespace: namespace,
                ackID: ackID,
                payload: payload,
                expectedAttachments: expectedAttachments
            ))
        }
    }

    func encode() throws -> EncodedSocketIOPacket {
        switch self {
        case .connect(let namespace, let auth):
            var text = String(SocketIOPacketType.connect.rawValue)
            text += encodeNamespace(namespace, includeSeparator: namespace != "/" || auth != nil)
            if let auth {
                text += try auth.encodeJSONString()
            }
            return .init(text: text, attachments: [])
        case .disconnect(let namespace):
            let text = String(SocketIOPacketType.disconnect.rawValue) + encodeNamespace(namespace, includeSeparator: namespace != "/")
            return .init(text: text, attachments: [])
        case .connectError(let namespace, let data):
            var text = String(SocketIOPacketType.connectError.rawValue)
            text += encodeNamespace(namespace, includeSeparator: namespace != "/" || true)
            text += try data.encodeJSONString()
            return .init(text: text, attachments: [])
        case .event(let namespace, let items, let ackID):
            return try encodeEventLike(
                namespace: namespace,
                items: items,
                ackID: ackID,
                textType: .event,
                binaryType: .binaryEvent
            )
        case .ack(let namespace, let items, let ackID):
            return try encodeEventLike(
                namespace: namespace,
                items: items,
                ackID: ackID,
                textType: .ack,
                binaryType: .binaryAck
            )
        }
    }

    private func encodeEventLike(
        namespace: String,
        items: [SocketIOValue],
        ackID: Int?,
        textType: SocketIOPacketType,
        binaryType: SocketIOPacketType
    ) throws -> EncodedSocketIOPacket {
        var attachments: [ByteBuffer] = []
        let payload = items.replacingBinary(with: &attachments)
        var text = String((attachments.isEmpty ? textType : binaryType).rawValue)
        if !attachments.isEmpty {
            text += "\(attachments.count)-"
        }
        let includeSeparator = namespace != "/"
        text += encodeNamespace(namespace, includeSeparator: includeSeparator)
        if let ackID {
            text += String(ackID)
        }
        text += try SocketIOValue.array(payload).encodeJSONString()
        return .init(text: text, attachments: attachments)
    }

    private func encodeNamespace(_ namespace: String, includeSeparator: Bool) -> String {
        guard namespace != "/" else { return "" }
        return includeSeparator ? "\(namespace)," : namespace
    }
}
