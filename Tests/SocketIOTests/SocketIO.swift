import Testing
import Foundation
import Hummingbird
import HummingbirdTesting
@testable import SocketIO

private struct EngineOpenPayload: Decodable {
    let sid: String
}

private func decodeEngineOpenSID(from body: ByteBuffer) throws -> String {
    let text = String(buffer: body)
    #expect(text.first == "0")
    let payload = Data(text.dropFirst().utf8)
    return try JSONDecoder().decode(EngineOpenPayload.self, from: payload).sid
}

private func decodeSocketPackets(from body: ByteBuffer) throws -> [SocketIOPacket] {
    let text = String(buffer: body)
    guard !text.isEmpty else { return [] }

    var decoded: [SocketIOPacket] = []
    for encoded in text.split(separator: "\u{1e}", omittingEmptySubsequences: false).map(String.init) {
        guard encoded.first == "4" else {
            continue
        }
        switch try SocketIOPacket.decode(from: String(encoded.dropFirst())) {
        case .packet(let packet):
            decoded.append(packet)
        case .pending:
            Issue.record("Unexpected pending binary packet in polling response")
        }
    }
    return decoded
}

private func decodeSocketPacketsFromPollingPayload(_ body: ByteBuffer) throws -> [SocketIOPacket] {
    let string = String(buffer: body)
    guard !string.isEmpty else { return [] }

    enum PollingPacket {
        case text(String)
        case binary(ByteBuffer)
    }

    let packets: [PollingPacket] = try string
        .split(separator: "\u{1e}", omittingEmptySubsequences: false)
        .map(String.init)
        .map { value in
            if value.first == "b" {
                let encoded = String(value.dropFirst())
                guard let data = Data(base64Encoded: encoded) else {
                    throw SocketIOPacketDecodingError.invalidPacket("Invalid polling binary payload")
                }
                return .binary(.init(bytes: data))
            } else {
                return .text(value)
            }
        }

    var decoded: [SocketIOPacket] = []
    var pending: PendingSocketIOPacket?
    var attachments: [ByteBuffer] = []

    for packet in packets {
        switch packet {
        case .text(let text):
            guard text.first == "4" else {
                continue
            }
            switch try SocketIOPacket.decode(from: String(text.dropFirst())) {
            case .packet(let socketPacket):
                decoded.append(socketPacket)
            case .pending(let pendingPacket):
                pending = pendingPacket
                attachments.removeAll(keepingCapacity: true)
            }
        case .binary(let buffer):
            guard let pendingPacket = pending else {
                Issue.record("Unexpected binary attachment without pending packet")
                continue
            }
            attachments.append(buffer)
            if attachments.count == pendingPacket.expectedAttachments {
                decoded.append(try pendingPacket.complete(with: attachments))
                pending = nil
                attachments.removeAll(keepingCapacity: true)
            }
        }
    }

    return decoded
}

private func handshake(
    with client: any TestClientProtocol,
    uri: String = "/socket.io?EIO=4&transport=polling"
) async throws -> String {
    let response = try await client.execute(uri: uri, method: .get)
    #expect(response.status == .ok)
    return try decodeEngineOpenSID(from: response.body)
}

private func postPolling(
    _ payload: String,
    sid: String,
    with client: any TestClientProtocol
) async throws -> TestResponse {
    try await client.execute(
        uri: "/socket.io?EIO=4&transport=polling&sid=\(sid)",
        method: .post,
        headers: [.contentType: "text/plain; charset=UTF-8"],
        body: .init(string: payload)
    )
}

private func poll(
    sid: String,
    with client: any TestClientProtocol
) async throws -> TestResponse {
    try await client.execute(uri: "/socket.io?EIO=4&transport=polling&sid=\(sid)", method: .get)
}

private func connectRootSocket(
    with client: any TestClientProtocol
) async throws -> (sid: String, packets: [SocketIOPacket]) {
    let sid = try await handshake(with: client)
    _ = try await postPolling("40", sid: sid, with: client)
    let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
    return (sid, packets)
}

private func encodeClientSocketPayload(_ packet: SocketIOPacket) throws -> String {
    let encoded = try packet.encode()
    return "4\(encoded.text)"
}

private extension ServerConfiguration {
    init(
        path: String = "/socket.io",
        addTrailingSlash: Bool = true,
        pingTimeout: Duration = .seconds(20),
        pingInterval: Duration = .seconds(30),
        upgradeTimeout: Duration = .seconds(10),
        connectTimeout: Duration = .seconds(45),
        maxHttpBufferSize: UInt = 10_000,
        authorizeRequest: @escaping RequestAuthorizer = { _ in true },
        transports: Transport = [.polling, .websocket],
        socketIDGenerator: @escaping SocketIDGenerator = { _, _ in
            UUID().uuidString.filter { $0 != "-" }
        },
        authorizeNamespaceConnection: @escaping NamespaceAuthorizer = { _ in .allow },
        adapterFactory: @escaping AdapterFactory = { _ in InMemoryAdapter() },
        cluster: Cluster = .init(),
        connectionStateRecovery: ConnectionStateRecovery? = nil,
        allowUpgrades: Bool = true,
        cors: Cors = .static(.init(allowedOrigin: .all))
    ) {
        self.init(
            routing: .init(path: path, allowsTrailingSlash: addTrailingSlash),
            heartbeat: .init(
                pingTimeout: pingTimeout,
                pingInterval: pingInterval,
                upgradeTimeout: upgradeTimeout,
                connectTimeout: connectTimeout
            ),
            transport: .init(
                transports: transports,
                allowUpgrades: allowUpgrades,
                maxPayload: maxHttpBufferSize
            ),
            namespaces: .init(
                socketIDGenerator: socketIDGenerator,
                adapterFactory: adapterFactory
            ),
            cluster: cluster,
            connectionStateRecovery: connectionStateRecovery,
            policy: .init(
                authorizeRequest: authorizeRequest,
                authorizeNamespaceConnection: authorizeNamespaceConnection
            ),
            cors: cors
        )
    }

    var addTrailingSlash: Bool { routing.allowsTrailingSlash }
    var maxHttpBufferSize: UInt { transport.maxPayload }
}

private actor SocketStore {
    private var sockets: [Socket] = []

    func append(_ socket: Socket) {
        sockets.append(socket)
    }

    func all() -> [Socket] {
        sockets
    }
}

private actor StringRecorder {
    private var values: [String] = []

    func append(_ value: String) {
        values.append(value)
    }

    func all() -> [String] {
        values
    }
}

private actor SocketErrorRecorder {
    private var values: [SocketErrorPayload] = []

    func append(_ value: SocketErrorPayload) {
        values.append(value)
    }

    func all() -> [SocketErrorPayload] {
        values
    }
}

private actor Counter {
    private var value = 0

    func increment() {
        value += 1
    }

    func current() -> Int {
        value
    }
}

private actor InMemoryClusterBus {
    private struct RegisteredNode {
        let onCommand: @Sendable (Data) async -> Void
        let onResponse: @Sendable (Data) async -> Void
    }

    private var nodes: [String: RegisteredNode] = [:]

    func register(
        nodeID: String,
        onCommand: @escaping @Sendable (Data) async -> Void,
        onResponse: @escaping @Sendable (Data) async -> Void
    ) {
        nodes[nodeID] = .init(onCommand: onCommand, onResponse: onResponse)
    }

    func unregister(nodeID: String) {
        nodes.removeValue(forKey: nodeID)
    }

    func otherNodeIDs(for nodeID: String) -> Set<String> {
        Set(nodes.keys.filter { $0 != nodeID })
    }

    func publishCommand(_ data: Data) async {
        for node in nodes.values {
            await node.onCommand(data)
        }
    }

    func publishResponse(_ data: Data, to nodeID: String) async {
        guard let node = nodes[nodeID] else { return }
        await node.onResponse(data)
    }
}

private actor InMemoryClusterCoordinator: SocketIOClusterCoordinator {
    let nodeID: String

    private let bus: InMemoryClusterBus
    private let recoveryStore: InMemoryConnectionStateRecoveryStore
    private var started = false

    init(
        nodeID: String,
        bus: InMemoryClusterBus,
        recoveryStore: InMemoryConnectionStateRecoveryStore = .init()
    ) {
        self.nodeID = nodeID
        self.bus = bus
        self.recoveryStore = recoveryStore
    }

    func start(
        onCommand: @escaping @Sendable (Data) async -> Void,
        onResponse: @escaping @Sendable (Data) async -> Void
    ) async throws {
        guard !started else { return }
        started = true
        await bus.register(nodeID: nodeID, onCommand: onCommand, onResponse: onResponse)
    }

    func stop() async {
        guard started else { return }
        started = false
        await bus.unregister(nodeID: nodeID)
    }

    func otherNodeIDs() async throws -> Set<String> {
        await bus.otherNodeIDs(for: nodeID)
    }

    func publishCommand(_ data: Data) async throws {
        await bus.publishCommand(data)
    }

    func publishResponse(_ data: Data, to nodeID: String) async throws {
        await bus.publishResponse(data, to: nodeID)
    }
}

extension InMemoryClusterCoordinator: SocketIOConnectionStateRecoveryStoreFactory {
    nonisolated func makeConnectionStateRecoveryStore() -> any SocketIOConnectionStateRecoveryStore {
        recoveryStore
    }
}

private struct DisconnectSnapshot: Equatable, Sendable {
    let namespace: String
    let phase: String
    let reason: SocketDisconnectReason
    let rooms: Set<String>
}

private actor DisconnectRecorder {
    private var snapshots: [DisconnectSnapshot] = []

    func append(_ snapshot: DisconnectSnapshot) {
        snapshots.append(snapshot)
    }

    func all() -> [DisconnectSnapshot] {
        snapshots
    }
}

private struct RecoverySnapshot: Equatable, Sendable {
    let id: String
    let recovered: Bool
    let rooms: Set<String>
    let data: [String: SocketIOValue]
}

private actor RecoveryRecorder {
    private var snapshots: [RecoverySnapshot] = []

    func append(_ snapshot: RecoverySnapshot) {
        snapshots.append(snapshot)
    }

    func all() -> [RecoverySnapshot] {
        snapshots
    }
}

@Test func encodesAndDecodesBinaryEventPackets() throws {
    let attachment = ByteBuffer(bytes: [0x01, 0x02, 0x03, 0x04])
    let packet = SocketIOPacket.event(namespace: "/", items: [.string("bin"), .binary(attachment)], ackID: 7)

    let encoded = try packet.encode()
    #expect(encoded.attachments.count == 1)
    #expect(encoded.text.hasPrefix("51-7"))

    let decoded = try SocketIOPacket.decode(from: encoded.text)
    switch decoded {
    case .packet:
        Issue.record("Expected binary packet to await attachments")
    case .pending(let pendingPacket):
        let completed = try pendingPacket.complete(with: encoded.attachments)
        #expect(completed == packet)
    }
}

@Test func rootNamespaceConnectsAndEchoesAuthAndAcknowledgements() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        maxHttpBufferSize: 1_000_000
    ))

    server.onConnection { socket in
        await socket.emit("auth", arguments: [socket.handshake.auth ?? .null])
        socket.on("message") { event, _ in
            await socket.emit("message-back", arguments: event.arguments)
        }
        socket.on("message-with-ack") { event, ack in
            try? await ack?.send(arguments: event.arguments)
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)

        let connectResponse = try await postPolling("40{\"token\":\"123\"}", sid: sid, with: client)
        #expect(connectResponse.status == .ok)

        let approvalResponse = try await poll(sid: sid, with: client)
        let packets = try decodeSocketPackets(from: approvalResponse.body)
        #expect(packets.count == 2)
        #expect(packets[0].namespace == "/")
        switch packets[0] {
        case .connect(_, let auth):
            guard case .object(let payload)? = auth else {
                Issue.record("Expected connect payload")
                return
            }
            #expect(payload["sid"] != nil)
        default:
            Issue.record("Expected connect packet")
        }
        #expect(packets[1] == .event(namespace: "/", items: [.string("auth"), .object(["token": .string("123")])], ackID: nil))

        let messageResponse = try await postPolling("42[\"message\",\"hello\"]", sid: sid, with: client)
        #expect(messageResponse.status == .ok)
        let echoed = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(echoed == [.event(namespace: "/", items: [.string("message-back"), .string("hello")], ackID: nil)])

        let ackPost = try await postPolling("421[\"message-with-ack\",\"hello\"]", sid: sid, with: client)
        #expect(ackPost.status == .ok)
        let ackPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(ackPackets == [.ack(namespace: "/", items: [.string("hello")], ackID: 1)])
    }
}

@Test func customNamespaceConnectsWithoutImplicitRootNamespace() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))
    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await socket.emit("auth", arguments: [socket.handshake.auth ?? .null])
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40/custom,{\"token\":\"abc\"}", sid: sid, with: client)
        let response = try await poll(sid: sid, with: client)
        let packets = try decodeSocketPackets(from: response.body)
        #expect(packets.count == 2)
        switch packets[0] {
        case .connect(let namespace, let payload):
            #expect(namespace == "/custom")
            guard case .object(let object)? = payload else {
                Issue.record("Expected connect payload")
                return
            }
            #expect(object["sid"] != nil)
        default:
            Issue.record("Expected connect packet")
        }
        #expect(packets[1] == .event(namespace: "/custom", items: [.string("auth"), .object(["token": .string("abc")])], ackID: nil))
    }
}

@Test func invalidEventBeforeNamespaceConnectClosesSession() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("42[\"oops\"]", sid: sid, with: client)
        let response = try await poll(sid: sid, with: client)
        #expect(response.status == .badRequest)
        #expect(String(buffer: response.body) == "Unknown session id")
    }
}

@Test func connectTimeoutClosesIdleEngineSession() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .milliseconds(50)
    ))

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        try await Task.sleep(for: .milliseconds(120))
        let response = try await poll(sid: sid, with: client)
        #expect(response.status == .badRequest)
        #expect(String(buffer: response.body) == "Unknown session id")
    }
}

@Test func rejectedNamespaceConnectionReturnsConnectError() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        authorizeNamespaceConnection: { request in
            guard request.namespace == "/restricted" else { return .allow }
            return .deny(message: "Forbidden namespace")
        }
    ))
    server.namespace("/restricted").onConnection { _ in }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        let response = try await postPolling("40/restricted,{\"role\":\"guest\"}", sid: sid, with: client)
        #expect(response.status == .ok)

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connectError(namespace: "/restricted", data: .object(["message": .string("Forbidden namespace")]))
        ])
    }
    }

@Test func rootNamespaceMiddlewareRejectsConnectionWithTypedPayload() async throws {
    let server = Server(port: 8080)
    server.use { _, _ in
        throw MiddlewareError("Forbidden middleware", data: .object(["reason": .string("denied")]))
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        let response = try await postPolling("40", sid: sid, with: client)
        #expect(response.status == .ok)

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connectError(namespace: "/", data: .object([
                "message": .string("Forbidden middleware"),
                "data": .object(["reason": .string("denied")]),
            ])),
        ])
    }
}

@Test func namespaceMiddlewareRejectionDoesNotAffectOtherNamespaces() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    let custom = server.namespace("/custom")
    custom.use { _, _ in
        throw MiddlewareError("Blocked custom namespace")
    }
    server.onConnection { socket in
        await socket.emit("root-ready", arguments: [.bool(true)])
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        let rootPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(rootPackets == [
            .connect(namespace: "/", auth: .object(["sid": rootPackets[0].connectPayloadSID!])),
            .event(namespace: "/", items: [.string("root-ready"), .bool(true)], ackID: nil),
        ])

        _ = try await postPolling("40/custom", sid: sid, with: client)
        let customPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(customPackets == [
            .connectError(namespace: "/custom", data: .object(["message": .string("Blocked custom namespace")])),
        ])
    }
}

@Test func namespaceAuthorizationRunsBeforeNamespaceMiddleware() async throws {
    let middlewareCounter = Counter()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        authorizeNamespaceConnection: { request in
            guard request.namespace == "/restricted" else { return .allow }
            return .deny(message: "Denied before middleware")
        }
    ))

    let restricted = server.namespace("/restricted")
    restricted.use { _, next in
        await middlewareCounter.increment()
        try await next()
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40/restricted", sid: sid, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connectError(namespace: "/restricted", data: .object(["message": .string("Denied before middleware")])),
        ])
    }

    #expect(await middlewareCounter.current() == 0)
}

@Test func socketMiddlewareRunsBeforeHandlersInRegistrationOrder() async throws {
    let recorder = StringRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        socket.use { event, _, next in
            await recorder.append("mw1:\(event.name)")
            try await next()
        }
        socket.use { event, _, next in
            await recorder.append("mw2:\(event.name)")
            try await next()
        }
        socket.on("message") { _, _ in
            await recorder.append("handler:message")
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("42[\"message\",\"hello\"]", sid: sid, with: client)
    }

    #expect(await recorder.all() == ["mw1:message", "mw2:message", "handler:message"])
}

@Test func socketMiddlewareCanShortCircuitEventDispatch() async throws {
    let recorder = StringRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        socket.use { event, _, _ in
            await recorder.append("blocked:\(event.name)")
        }
        socket.on("message") { _, _ in
            await recorder.append("handler")
            await socket.emit("message-back", arguments: [.string("unexpected")])
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let response = try await postPolling("42[\"message\",\"hello\"]", sid: sid, with: client)
        #expect(response.status == .ok)

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets.isEmpty)
    }

    #expect(await recorder.all() == ["blocked:message"])
}

@Test func socketMiddlewareFailureEmitsErrorAndKeepsSessionAlive() async throws {
    let errors = SocketErrorRecorder()
    let rawErrors = StringRecorder()
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
        socket.onError { payload in
            await errors.append(payload)
        }
        socket.on("error") { event, _ in
            do {
                let payload = try event.arguments.first?.decode(as: SocketErrorPayload.self)
                if let payload {
                    await rawErrors.append(payload.message)
                } else {
                    Issue.record("Expected socket error payload")
                }
            } catch {
                Issue.record("Failed to decode socket error payload: \(error)")
            }
        }
        socket.use { event, _, next in
            if event.name == "message" {
                throw MiddlewareError("blocked", data: .object(["code": .number(401)]))
            }
            try await next()
        }
        socket.on("message") { _, _ in
            Issue.record("Handler should not run after middleware rejection")
        }
        socket.on("followup") { event, _ in
            await socket.emit("followup-ok", arguments: event.arguments)
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("42[\"message\",\"hello\"]", sid: sid, with: client)
        _ = try await postPolling("42[\"followup\",\"still-connected\"]", sid: sid, with: client)

        let response = try await poll(sid: sid, with: client)
        #expect(response.status == .ok)
        let packets = try decodeSocketPackets(from: response.body)
        #expect(packets == [
            .event(namespace: "/", items: [.string("followup-ok"), .string("still-connected")], ackID: nil)
        ])
    }

    #expect(await errors.all() == [
        .init(message: "blocked", data: .object(["code": .number(401)])),
    ])
    #expect(await rawErrors.all() == ["blocked"])
    #expect(await disconnects.all().isEmpty)
}

@Test func namespaceMiddlewareDoubleNextReturnsConnectError() async throws {
    let server = Server(port: 8080)
    server.use { _, next in
        try await next()
        try await next()
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connectError(namespace: "/", data: .object(["message": .string("Middleware next() may only be called once")])),
        ])
    }
}

@Test func serverReceivesAcknowledgementForEmittedEvent() async throws {
    actor Recorder {
        var values: [[SocketIOValue]] = []
        func append(_ items: [SocketIOValue]) { values.append(items) }
    }

    let recorder = Recorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await socket.emit("needs-ack", arguments: [.string("hello")]) { items in
            await recorder.append(items)
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets.count == 2)
        #expect(packets[1] == .event(namespace: "/", items: [.string("needs-ack"), .string("hello")], ackID: 0))

        let ackResponse = try await postPolling("430[\"done\"]", sid: sid, with: client)
        #expect(ackResponse.status == .ok)
    }

    #expect(await recorder.values == [[.string("done")]])
}

@Test func broadcastAckReturnsSingleResponse() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let ackTask = Task {
            try await server.emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
        }

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets.count == 1)
        guard case .event(_, _, let ackID?) = packets[0] else {
            Issue.record("Expected broadcast event with ack ID")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("client-1")], ackID: ackID)),
            sid: sid,
            with: client
        )

        #expect(try await ackTask.value == [[.string("client-1")]])
    }
}

@Test func broadcastAckReturnsResponsesInSortedSocketIDOrder() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID }
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 2)

        let ackTask = Task {
            try await server.emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
        }

        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        guard case .event(_, _, let firstAckID?) = firstPackets.first,
              case .event(_, _, let secondAckID?) = secondPackets.first
        else {
            Issue.record("Expected ack IDs for both targets")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("second-client")], ackID: secondAckID)),
            sid: secondSID,
            with: client
        )
        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("first-client")], ackID: firstAckID)),
            sid: firstSID,
            with: client
        )

        let expected = [
            (firstSID, [SocketIOValue.string("first-client")]),
            (secondSID, [SocketIOValue.string("second-client")]),
        ]
            .sorted { $0.0 < $1.0 }
            .map(\.1)

        #expect(try await ackTask.value == expected)
    }
}

@Test func socketBroadcastAckExcludesOriginatingSocket() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID }
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let connected = await sockets.all()
        let origin = try #require(connected.first { $0.id == firstSID })

        let ackTask = Task {
            try await origin.broadcast.emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
        }

        let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        guard case .event(_, _, let ackID?) = secondPackets.first else {
            Issue.record("Expected only the non-origin socket to receive an ack request")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("peer")], ackID: ackID)),
            sid: secondSID,
            with: client
        )

        #expect(try await ackTask.value == [[.string("peer")]])
    }
}

@Test func broadcastAckRespectsRoomInclusionAndExclusion() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID }
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: thirdSID, with: client)
        _ = try await poll(sid: thirdSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 3)

        let firstSocket = try #require(connected.first { $0.id == firstSID })
        let secondSocket = try #require(connected.first { $0.id == secondSID })
        let thirdSocket = try #require(connected.first { $0.id == thirdSID })

        await firstSocket.join("alpha")
        await secondSocket.join("alpha")
        await secondSocket.join("beta")
        await thirdSocket.join("beta")

        let ackTask = Task {
            try await server.to("alpha").excluding("beta").emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
        }

        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        guard case .event(_, _, let ackID?) = firstPackets.first else {
            Issue.record("Expected only the alpha-only socket to receive the ack request")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("alpha-only")], ackID: ackID)),
            sid: firstSID,
            with: client
        )

        #expect(try await ackTask.value == [[.string("alpha-only")]])
    }
}

@Test func broadcastAckTimeoutThrowsPartialResponsesAndIgnoresLateAcks() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID }
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 2)

        let firstExpected = [
            (firstSID, [SocketIOValue.string("first-response")]),
            (secondSID, [SocketIOValue.string("second-response")]),
        ]
            .sorted { $0.0 < $1.0 }

        let ackTask = Task {
            try await server.emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .milliseconds(100))
        }

        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        guard case .event(_, _, let firstAckID?) = firstPackets.first,
              case .event(_, _, let secondAckID?) = secondPackets.first
        else {
            Issue.record("Expected ack IDs for both broadcast targets")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("first-response")], ackID: firstAckID)),
            sid: firstSID,
            with: client
        )

        do {
            _ = try await ackTask.value
            Issue.record("Expected broadcast ack timeout")
        } catch let error as BroadcastAckTimeoutError {
            let expectedResponses = firstExpected
                .filter { $0.1 == [.string("first-response")] }
                .map(\.1)
            #expect(error.responses == expectedResponses)
            #expect(error.receivedCount == 1)
            #expect(error.expectedCount == 2)
            #expect(error.missingCount == 1)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("second-response")], ackID: secondAckID)),
            sid: secondSID,
            with: client
        )

        let secondTask = Task {
            try await server.emit("needs-ack-2", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
        }

        let followUpFirstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let followUpSecondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        guard case .event(_, _, let followUpFirstAckID?) = followUpFirstPackets.first,
              case .event(_, _, let followUpSecondAckID?) = followUpSecondPackets.first
        else {
            Issue.record("Expected follow-up ack IDs for both targets")
            return
        }

        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("first-follow-up")], ackID: followUpFirstAckID)),
            sid: firstSID,
            with: client
        )
        _ = try await postPolling(
            try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("second-follow-up")], ackID: followUpSecondAckID)),
            sid: secondSID,
            with: client
        )

        let expectedFollowUp = [
            (firstSID, [SocketIOValue.string("first-follow-up")]),
            (secondSID, [SocketIOValue.string("second-follow-up")]),
        ]
            .sorted { $0.0 < $1.0 }
            .map(\.1)

        #expect(try await secondTask.value == expectedFollowUp)
    }
}

@Test func broadcastAckReturnsEmptyResponsesWhenNoSocketsMatch() async throws {
    let server = Server(port: 8080)
    let responses = try await server.emit("needs-ack", arguments: [.string("hello")], collectingAcksWithin: .seconds(1))
    #expect(responses.isEmpty)
}

@Test func disconnectingOneNamespaceLeavesOtherNamespaceActive() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        socket.on("message") { event, _ in
            await socket.emit("message-back", arguments: event.arguments)
        }
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await socket.emit("custom-ready", arguments: [.bool(true)])
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)

        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        _ = try await postPolling("40/custom", sid: sid, with: client)
        let customPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(customPackets.count == 2)
        switch customPackets[0] {
        case .connect(let namespace, let payload):
            #expect(namespace == "/custom")
            guard case .object(let value)? = payload else {
                Issue.record("Expected namespace connect payload")
                return
            }
            #expect(value["sid"] != nil)
        default:
            Issue.record("Expected namespace connect packet")
        }
        #expect(customPackets[1] == .event(namespace: "/custom", items: [.string("custom-ready"), .bool(true)], ackID: nil))

        _ = try await postPolling("41/custom,", sid: sid, with: client)
        let disconnectPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(disconnectPackets.isEmpty)

        _ = try await postPolling("42[\"message\",\"still-root\"]", sid: sid, with: client)
        let rootPackets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(rootPackets == [.event(namespace: "/", items: [.string("message-back"), .string("still-root")], ackID: nil)])
    }
}

@Test func disconnectLifecycleHooksPreserveRoomsUntilTeardown() async throws {
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await socket.join("shared")
        socket.onDisconnecting { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnecting",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("41", sid: sid, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets.isEmpty)
    }

    let snapshots = await disconnects.all()
    #expect(snapshots.count == 2)
    #expect(snapshots[0].phase == "disconnecting")
    #expect(snapshots[0].reason == .clientNamespaceDisconnect)
    #expect(snapshots[0].rooms.count == 2)
    #expect(snapshots[0].rooms.contains("shared"))
    #expect(snapshots[0].rooms.contains(where: { $0 != "shared" }))
    #expect(snapshots[1] == .init(namespace: "/", phase: "disconnect", reason: .clientNamespaceDisconnect, rooms: []))
}

@Test func serverInitiatedDisconnectTriggersLifecycleHooks() async throws {
    let sockets = SocketStore()
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
        await socket.join("shared")
        socket.onDisconnecting { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnecting",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let connected = await sockets.all()
        let socket = try #require(connected.first)
        await socket.disconnect()

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [.disconnect(namespace: "/")])
    }

    let snapshots = await disconnects.all()
    #expect(snapshots.count == 2)
    #expect(snapshots[0].phase == "disconnecting")
    #expect(snapshots[0].reason == .serverNamespaceDisconnect)
    #expect(snapshots[0].rooms.contains("shared"))
    #expect(snapshots[1] == .init(namespace: "/", phase: "disconnect", reason: .serverNamespaceDisconnect, rooms: []))
}

@Test func transportClosureTriggersLifecycleHooksForEachNamespace() async throws {
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await socket.join("root-room")
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await socket.join("custom-room")
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("40/custom", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        await server.close()
    }

    let snapshots = await disconnects.all().sorted { lhs, rhs in
        if lhs.namespace == rhs.namespace {
            return lhs.phase < rhs.phase
        }
        return lhs.namespace < rhs.namespace
    }
    #expect(snapshots == [
        .init(namespace: "/", phase: "disconnect", reason: .serverShuttingDown, rooms: []),
        .init(namespace: "/custom", phase: "disconnect", reason: .serverShuttingDown, rooms: []),
    ])
}

@Test func socketDisconnectWithCloseClosesTransportAndOtherNamespaces() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("40/custom", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        let rootSocket = try #require(root.first)
        let customSocket = try #require(customNamespaceSockets.first)

        await rootSocket.disconnect(true)

        let drain = try await poll(sid: sid, with: client)
        if drain.status == .ok {
            let drainPackets = try decodeSocketPackets(from: drain.body)
            #expect(drainPackets == [.disconnect(namespace: "/")])

            let followup = try await poll(sid: sid, with: client)
            #expect(followup.status == .badRequest)
            #expect(String(buffer: followup.body) == "Unknown session id")
        } else {
            #expect(drain.status == .badRequest)
            #expect(String(buffer: drain.body) == "Unknown session id")
        }

        #expect(await rootSocket.rooms.isEmpty)
        #expect(await customSocket.rooms.isEmpty)
    }

    let snapshots = await disconnects.all().sorted { lhs, rhs in
        lhs.namespace < rhs.namespace
    }
    #expect(snapshots == [
        .init(namespace: "/", phase: "disconnect", reason: .serverNamespaceDisconnect, rooms: []),
        .init(namespace: "/custom", phase: "disconnect", reason: .forcedClose, rooms: []),
    ])
}

@Test func invalidPacketTriggersParseErrorDisconnectHooks() async throws {
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)
        _ = try await postPolling("42[123]", sid: sid, with: client)

        let response = try await poll(sid: sid, with: client)
        #expect(response.status == .badRequest)
        #expect(String(buffer: response.body) == "Unknown session id")
    }

    #expect(await disconnects.all() == [
        .init(namespace: "/", phase: "disconnect", reason: .parseError, rooms: []),
    ])
}

@Test func binaryEventsRoundTripThroughPollingRuntime() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        maxHttpBufferSize: 1_000_000
    ))

    server.onConnection { socket in
        socket.on("binary") { event, _ in
            await socket.emit("binary-back", arguments: event.arguments)
        }
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let outgoing = try SocketIOPacket.event(
            namespace: "/",
            items: [.string("binary"), .binary(.init(bytes: [0x01, 0x02, 0x03]))],
            ackID: nil
        ).encode()
        let attachmentData = Data(outgoing.attachments[0].readableBytesView)
        let payload = "4\(outgoing.text)\u{1e}b\(attachmentData.base64EncodedString())"

        let response = try await postPolling(payload, sid: sid, with: client)
        #expect(response.status == .ok)

        let packets = try decodeSocketPacketsFromPollingPayload(try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .event(namespace: "/", items: [.string("binary-back"), .binary(.init(bytes: [0x01, 0x02, 0x03]))], ackID: nil)
        ])
    }
}

@Test func roomsTrackMembershipAndRootBroadcastsOnlyReachJoinedSockets() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 2)

        let first = connected[0]
        let second = connected[1]

        await first.join("updates")
        #expect(await first.rooms == Set([first.id, "updates"]))
        #expect(await second.rooms == Set([second.id]))

        await server.to("updates").emit("news", arguments: [.string("hello")])
        await second.emit("control", arguments: [.string("second")])

        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstPackets == [
            .event(namespace: "/", items: [.string("news"), .string("hello")], ackID: nil)
        ])
        #expect(secondPackets == [
            .event(namespace: "/", items: [.string("control"), .string("second")], ackID: nil)
        ])

        await first.leave("updates")
        #expect(await first.rooms == Set([first.id]))

        await server.to("updates").emit("news", arguments: [.string("goodbye")])
        await first.emit("control", arguments: [.string("first")])
        await second.emit("control", arguments: [.string("second")])

        let firstAfterLeave = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondAfterLeave = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstAfterLeave == [
            .event(namespace: "/", items: [.string("control"), .string("first")], ackID: nil)
        ])
        #expect(secondAfterLeave == [
            .event(namespace: "/", items: [.string("control"), .string("second")], ackID: nil)
        ])

        await first.join("updates")
        await first.leaveAll()
        #expect(await first.rooms == Set([first.id]))
    }
}

@Test func bulkSocketOpsMutateRoomsUsingBroadcastTargets() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: thirdSID, with: client)
        _ = try await poll(sid: thirdSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 3)

        let first = connected[0]
        let second = connected[1]
        let third = connected[2]

        await first.join("alpha")
        await second.join("alpha")
        await second.join("gamma")
        await third.join("gamma")

        await server.to("alpha").excluding("gamma").socketsJoin("beta")
        #expect(await first.rooms == Set([first.id, "alpha", "beta"]))
        #expect(await second.rooms == Set([second.id, "alpha", "gamma"]))
        #expect(await third.rooms == Set([third.id, "gamma"]))

        await server.to("beta").emit("beta-news", arguments: [.string("first-only")])
        await second.emit("control", arguments: [.string("second")])
        await third.emit("control", arguments: [.string("third")])

        let firstBetaPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondBetaPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdBetaPackets = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstBetaPackets == [
            .event(namespace: "/", items: [.string("beta-news"), .string("first-only")], ackID: nil)
        ])
        #expect(secondBetaPackets == [
            .event(namespace: "/", items: [.string("control"), .string("second")], ackID: nil)
        ])
        #expect(thirdBetaPackets == [
            .event(namespace: "/", items: [.string("control"), .string("third")], ackID: nil)
        ])

        await server.to("alpha").socketsJoin("beta")
        #expect(await first.rooms == Set([first.id, "alpha", "beta"]))
        #expect(await second.rooms == Set([second.id, "alpha", "beta", "gamma"]))
        #expect(await third.rooms == Set([third.id, "gamma"]))

        await server.to("alpha").excluding("gamma").socketsLeave("beta")
        #expect(await first.rooms == Set([first.id, "alpha"]))
        #expect(await second.rooms == Set([second.id, "alpha", "beta", "gamma"]))
        #expect(await third.rooms == Set([third.id, "gamma"]))

        await server.to("beta").emit("beta-news", arguments: [.string("second-only")])
        await first.emit("control", arguments: [.string("first")])
        await third.emit("control", arguments: [.string("third")])

        let firstAfterLeavePackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondAfterLeavePackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdAfterLeavePackets = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstAfterLeavePackets == [
            .event(namespace: "/", items: [.string("control"), .string("first")], ackID: nil)
        ])
        #expect(secondAfterLeavePackets == [
            .event(namespace: "/", items: [.string("beta-news"), .string("second-only")], ackID: nil)
        ])
        #expect(thirdAfterLeavePackets == [
            .event(namespace: "/", items: [.string("control"), .string("third")], ackID: nil)
        ])
    }
}

@Test func serverDirectBulkRoomOpsAffectAllRootSockets() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: thirdSID, with: client)
        _ = try await poll(sid: thirdSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 3)

        await server.socketsJoin("shared")
        #expect(await connected[0].rooms == Set([connected[0].id, "shared"]))
        #expect(await connected[1].rooms == Set([connected[1].id, "shared"]))
        #expect(await connected[2].rooms == Set([connected[2].id, "shared"]))

        await server.to("shared").emit("joined", arguments: [.bool(true)])
        let firstJoinedPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondJoinedPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdJoinedPackets = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstJoinedPackets == [
            .event(namespace: "/", items: [.string("joined"), .bool(true)], ackID: nil)
        ])
        #expect(secondJoinedPackets == [
            .event(namespace: "/", items: [.string("joined"), .bool(true)], ackID: nil)
        ])
        #expect(thirdJoinedPackets == [
            .event(namespace: "/", items: [.string("joined"), .bool(true)], ackID: nil)
        ])

        await server.socketsLeave("shared")
        #expect(await connected[0].rooms == Set([connected[0].id]))
        #expect(await connected[1].rooms == Set([connected[1].id]))
        #expect(await connected[2].rooms == Set([connected[2].id]))

        await server.to("shared").emit("after-leave", arguments: [.bool(true)])
        #expect(try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body).isEmpty)
        #expect(try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body).isEmpty)
        #expect(try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body).isEmpty)
    }
}

@Test func socketScopedBulkSocketOpsExcludeOriginatingSocket() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: thirdSID, with: client)
        _ = try await poll(sid: thirdSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 3)

        let first = connected[0]
        let second = connected[1]
        let third = connected[2]

        await second.join("alpha")
        await third.join("alpha")

        await first.to("alpha").socketsJoin("beta")
        #expect(await first.rooms == Set([first.id]))
        #expect(await second.rooms == Set([second.id, "alpha", "beta"]))
        #expect(await third.rooms == Set([third.id, "alpha", "beta"]))

        await first.to("beta").disconnectSockets()
        let secondDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(secondDisconnectPackets == [.disconnect(namespace: "/")])
        #expect(thirdDisconnectPackets == [.disconnect(namespace: "/")])

        #expect(await second.rooms.isEmpty)
        #expect(await third.rooms.isEmpty)

        await first.emit("control", arguments: [.string("still-connected")])
        let firstControlPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(firstControlPackets == [
            .event(namespace: "/", items: [.string("control"), .string("still-connected")], ackID: nil)
        ])
    }
}

@Test func socketAndNamespaceBroadcastsSupportUnionAndExclusionSemantics() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: thirdSID, with: client)
        _ = try await poll(sid: thirdSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 3)

        await connected[0].join("alpha")
        await connected[1].join("alpha")
        await connected[1].join("beta")
        await connected[2].join("beta")

        await connected[0].to("beta").emit("fanout", arguments: [.string("hello")])
        await connected[0].emit("control", arguments: [.string("first")])

        let firstFanout = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondFanout = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdFanout = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstFanout == [
            .event(namespace: "/", items: [.string("control"), .string("first")], ackID: nil)
        ])
        #expect(secondFanout == [
            .event(namespace: "/", items: [.string("fanout"), .string("hello")], ackID: nil)
        ])
        #expect(thirdFanout == [
            .event(namespace: "/", items: [.string("fanout"), .string("hello")], ackID: nil)
        ])

        await server.to("alpha").to("beta").emit("union", arguments: [.string("rooms")])

        let firstUnion = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondUnion = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdUnion = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstUnion == [
            .event(namespace: "/", items: [.string("union"), .string("rooms")], ackID: nil)
        ])
        #expect(secondUnion == [
            .event(namespace: "/", items: [.string("union"), .string("rooms")], ackID: nil)
        ])
        #expect(thirdUnion == [
            .event(namespace: "/", items: [.string("union"), .string("rooms")], ackID: nil)
        ])

        await server.to("alpha").to("beta").excluding("beta").emit("filtered", arguments: [.string("only-alpha")])
        await connected[1].emit("control", arguments: [.string("second")])
        await connected[2].emit("control", arguments: [.string("third")])

        let firstFiltered = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondFiltered = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        let thirdFiltered = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(firstFiltered == [
            .event(namespace: "/", items: [.string("filtered"), .string("only-alpha")], ackID: nil)
        ])
        #expect(secondFiltered == [
            .event(namespace: "/", items: [.string("control"), .string("second")], ackID: nil)
        ])
        #expect(thirdFiltered == [
            .event(namespace: "/", items: [.string("control"), .string("third")], ackID: nil)
        ])
    }
}

@Test func bulkDisconnectWithCloseClosesTransportAndOtherNamespaces() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let disconnects = DisconnectRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
        socket.onDisconnect { reason in
            await disconnects.append(.init(
                namespace: socket.namespace,
                phase: "disconnect",
                reason: reason,
                rooms: await socket.rooms
            ))
        }
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)
        _ = try await postPolling("40/custom", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        #expect(root.count == 2)
        #expect(customNamespaceSockets.count == 1)

        let firstRoot = root[0]
        let secondRoot = root[1]
        let secondCustom = customNamespaceSockets[0]

        await secondRoot.join("alpha")
        await server.to("alpha").disconnectSockets(true)

        let drain = try await poll(sid: secondSID, with: client)
        if drain.status == .ok {
            let drainPackets = try decodeSocketPackets(from: drain.body)
            #expect(drainPackets == [.disconnect(namespace: "/")])

            let followup = try await poll(sid: secondSID, with: client)
            #expect(followup.status == .badRequest)
            #expect(String(buffer: followup.body) == "Unknown session id")
        } else {
            #expect(drain.status == .badRequest)
            #expect(String(buffer: drain.body) == "Unknown session id")
        }

        #expect(await secondRoot.rooms.isEmpty)
        #expect(await secondCustom.rooms.isEmpty)

        await firstRoot.emit("control", arguments: [.string("still-connected")])
        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(firstPackets == [
            .event(namespace: "/", items: [.string("control"), .string("still-connected")], ackID: nil)
        ])
    }

    let snapshots = await disconnects.all().sorted { lhs, rhs in
        lhs.namespace < rhs.namespace
    }
    #expect(snapshots == [
        .init(namespace: "/", phase: "disconnect", reason: .serverNamespaceDisconnect, rooms: []),
        .init(namespace: "/custom", phase: "disconnect", reason: .forcedClose, rooms: []),
    ])
}

@Test func serverDirectDisconnectOnlyAffectsRootNamespace() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("40/custom", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)
        _ = try await postPolling("40/custom", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        #expect(root.count == 2)
        #expect(customNamespaceSockets.count == 2)

        let firstCustom = customNamespaceSockets[0]
        let secondCustom = customNamespaceSockets[1]

        await server.disconnectSockets()

        let firstRootDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondRootDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstRootDisconnectPackets == [.disconnect(namespace: "/")])
        #expect(secondRootDisconnectPackets == [.disconnect(namespace: "/")])

        #expect(await root[0].rooms.isEmpty)
        #expect(await root[1].rooms.isEmpty)
        #expect(await firstCustom.rooms == Set([firstCustom.id]))
        #expect(await secondCustom.rooms == Set([secondCustom.id]))

        await custom.emit("custom-still-connected", arguments: [.bool(true)])
        let firstCustomPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondCustomPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstCustomPackets == [
            .event(namespace: "/custom", items: [.string("custom-still-connected"), .bool(true)], ackID: nil)
        ])
        #expect(secondCustomPackets == [
            .event(namespace: "/custom", items: [.string("custom-still-connected"), .bool(true)], ackID: nil)
        ])
    }
}

@Test func volatileDirectEmitDropsWithoutPendingPoll() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 1)

        await connected[0].volatile.emit("volatile-drop", arguments: [.string("value")])
        await connected[0].emit("control", arguments: [.string("fallback")])

        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .event(namespace: "/", items: [.string("control"), .string("fallback")], ackID: nil)
        ])
    }
}

@Test func volatileDirectEmitFlushesWithPendingPoll() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40", sid: sid, with: client)
        _ = try await poll(sid: sid, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 1)

        async let pendingPoll = poll(sid: sid, with: client)
        try await Task.sleep(for: .milliseconds(50))
        await connected[0].volatile.emit("volatile-flush", arguments: [.string("value")])

        let pendingPollResponse = try await pendingPoll
        let packets = try decodeSocketPackets(from: pendingPollResponse.body)
        #expect(packets == [
            .event(namespace: "/", items: [.string("volatile-flush"), .string("value")], ackID: nil)
        ])
    }
}

@Test func volatileBroadcastsOnlyReachWritableTargets() async throws {
    let sockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await sockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let connected = await sockets.all()
        #expect(connected.count == 2)

        let first = connected[0]

        await server.volatile.emit("root-drop", arguments: [.string("value")])
        await first.emit("control", arguments: [.string("first-fallback")])
        await connected[1].emit("control", arguments: [.string("second-fallback")])

        let droppedFirst = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let droppedSecond = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(droppedFirst == [
            .event(namespace: "/", items: [.string("control"), .string("first-fallback")], ackID: nil)
        ])
        #expect(droppedSecond == [
            .event(namespace: "/", items: [.string("control"), .string("second-fallback")], ackID: nil)
        ])

        async let firstPendingNamespacePoll = poll(sid: firstSID, with: client)
        async let secondPendingNamespacePoll = poll(sid: secondSID, with: client)
        try await Task.sleep(for: .milliseconds(50))
        await server.namespace("/").volatile.emit("namespace-volatile", arguments: [.string("all")])

        let firstPendingNamespaceResponse = try await firstPendingNamespacePoll
        let secondPendingNamespaceResponse = try await secondPendingNamespacePoll
        let firstNamespacePackets = try decodeSocketPackets(from: firstPendingNamespaceResponse.body)
        let secondNamespacePackets = try decodeSocketPackets(from: secondPendingNamespaceResponse.body)
        #expect(firstNamespacePackets == [
            .event(namespace: "/", items: [.string("namespace-volatile"), .string("all")], ackID: nil)
        ])
        #expect(secondNamespacePackets == [
            .event(namespace: "/", items: [.string("namespace-volatile"), .string("all")], ackID: nil)
        ])

        async let secondPendingBroadcastPoll = poll(sid: secondSID, with: client)
        try await Task.sleep(for: .milliseconds(50))
        await first.broadcast.volatile.emit("socket-broadcast-volatile", arguments: [.string("second-only")])
        await first.emit("control", arguments: [.string("first-fallback")])

        let secondPendingBroadcastResponse = try await secondPendingBroadcastPoll
        let secondBroadcastPackets = try decodeSocketPackets(from: secondPendingBroadcastResponse.body)
        let firstBroadcastPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(secondBroadcastPackets == [
            .event(namespace: "/", items: [.string("socket-broadcast-volatile"), .string("second-only")], ackID: nil)
        ])
        #expect(firstBroadcastPackets == [
            .event(namespace: "/", items: [.string("control"), .string("first-fallback")], ackID: nil)
        ])
    }
}

@Test func namespaceBulkDisconnectLeavesOtherNamespacesConnected() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("40/custom", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40/custom", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        #expect(root.count == 1)
        #expect(customNamespaceSockets.count == 2)

        let firstRoot = root[0]
        let firstCustom = customNamespaceSockets[0]
        let secondCustom = customNamespaceSockets[1]

        await firstCustom.join("shared")
        await secondCustom.join("shared")

        await custom.to("shared").disconnectSockets()

        let firstDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstDisconnectPackets == [.disconnect(namespace: "/custom")])
        #expect(secondDisconnectPackets == [.disconnect(namespace: "/custom")])

        #expect(await firstCustom.rooms.isEmpty)
        #expect(await secondCustom.rooms.isEmpty)

        await firstRoot.emit("control", arguments: [.string("root-still-connected")])
        let firstRootPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(firstRootPackets == [
            .event(namespace: "/", items: [.string("control"), .string("root-still-connected")], ackID: nil)
        ])
    }
}

@Test func namespaceDirectDisconnectOnlyAffectsThatNamespace() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("40/custom", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40/custom", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        #expect(root.count == 1)
        #expect(customNamespaceSockets.count == 2)

        let firstRoot = root[0]
        let firstCustom = customNamespaceSockets[0]
        let secondCustom = customNamespaceSockets[1]

        await custom.disconnectSockets()

        let firstDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondDisconnectPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstDisconnectPackets == [.disconnect(namespace: "/custom")])
        #expect(secondDisconnectPackets == [.disconnect(namespace: "/custom")])

        #expect(await firstCustom.rooms.isEmpty)
        #expect(await secondCustom.rooms.isEmpty)

        await firstRoot.emit("root-still-connected", arguments: [.bool(true)])
        let firstRootPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(firstRootPackets == [
            .event(namespace: "/", items: [.string("root-still-connected"), .bool(true)], ackID: nil)
        ])
    }
}

@Test func roomsRemainNamespaceScopedAndDisconnectRemovesMembership() async throws {
    let rootSockets = SocketStore()
    let customSockets = SocketStore()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(60),
        pingInterval: .seconds(60),
        connectTimeout: .seconds(2)
    ))

    server.onConnection { socket in
        await rootSockets.append(socket)
    }

    let custom = server.namespace("/custom")
    custom.onConnection { socket in
        await customSockets.append(socket)
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("40/custom", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40", sid: secondSID, with: client)
        _ = try await poll(sid: secondSID, with: client)

        let root = await rootSockets.all()
        let customNamespaceSockets = await customSockets.all()
        #expect(root.count == 2)
        #expect(customNamespaceSockets.count == 1)

        let firstRoot = root[0]
        let secondRoot = root[1]
        let firstCustom = customNamespaceSockets[0]

        await firstRoot.join("shared")
        await secondRoot.join("shared")
        await firstCustom.join("shared")

        await custom.to("shared").emit("custom-room", arguments: [.string("value")])
        await secondRoot.emit("control", arguments: [.string("second-root")])

        let firstCustomPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondCustomPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstCustomPackets == [
            .event(namespace: "/custom", items: [.string("custom-room"), .string("value")], ackID: nil)
        ])
        #expect(secondCustomPackets == [
            .event(namespace: "/", items: [.string("control"), .string("second-root")], ackID: nil)
        ])

        await firstCustom.disconnect()
        let disconnectPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(disconnectPackets == [.disconnect(namespace: "/custom")])
        #expect(await firstCustom.rooms.isEmpty)

        await server.to("shared").emit("root-room", arguments: [.string("still-root")])

        let firstRootPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondRootPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstRootPackets == [
            .event(namespace: "/", items: [.string("root-room"), .string("still-root")], ackID: nil)
        ])
        #expect(secondRootPackets == [
            .event(namespace: "/", items: [.string("root-room"), .string("still-root")], ackID: nil)
        ])

        await custom.to("shared").emit("custom-room", arguments: [.string("after-disconnect")])
        await firstRoot.emit("control", arguments: [.string("first-root")])
        await secondRoot.emit("control", arguments: [.string("second-root")])
        let firstAfterDisconnect = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        let secondAfterDisconnect = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(firstAfterDisconnect == [
            .event(namespace: "/", items: [.string("control"), .string("first-root")], ackID: nil)
        ])
        #expect(secondAfterDisconnect == [
            .event(namespace: "/", items: [.string("control"), .string("second-root")], ackID: nil)
        ])
    }
}

@Test func exactNamespaceTakesPrecedenceOverDynamicParent() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    let parent = server.dynamicNamespace(where: { $0.hasPrefix("/project-") })
    parent.onConnection { socket in
        await socket.emit("dynamic-ready", arguments: [.string(socket.namespace)])
    }

    server.namespace("/project-static").onConnection { socket in
        await socket.emit("static-ready", arguments: [.string(socket.namespace)])
    }

    try await server.application.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40/project-static", sid: sid, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connect(namespace: "/project-static", auth: .object(["sid": packets[0].connectPayloadSID!])),
            .event(namespace: "/project-static", items: [.string("static-ready"), .string("/project-static")], ackID: nil),
        ])
    }
}

@Test func dynamicNamespacesReuseChildrenAndSnapshotParentHandlers() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    let parent = server.dynamicNamespace(matching: try! Regex("^/tenant-[0-9]+$"))
    parent.onConnection { socket in
        await socket.emit("ready", arguments: [.string(socket.namespace)])
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-1", sid: firstSID, with: client)
        let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstSID, with: client).body)
        #expect(firstPackets == [
            .connect(namespace: "/tenant-1", auth: .object(["sid": firstPackets[0].connectPayloadSID!])),
            .event(namespace: "/tenant-1", items: [.string("ready"), .string("/tenant-1")], ackID: nil),
        ])

        parent.onConnection { socket in
            await socket.emit("late", arguments: [.bool(true)])
        }

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-1", sid: secondSID, with: client)
        let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(secondPackets == [
            .connect(namespace: "/tenant-1", auth: .object(["sid": secondPackets[0].connectPayloadSID!])),
            .event(namespace: "/tenant-1", items: [.string("ready"), .string("/tenant-1")], ackID: nil),
        ])

        let thirdSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-2", sid: thirdSID, with: client)
        let thirdPackets = try decodeSocketPackets(from: try await poll(sid: thirdSID, with: client).body)
        #expect(thirdPackets == [
            .connect(namespace: "/tenant-2", auth: .object(["sid": thirdPackets[0].connectPayloadSID!])),
            .event(namespace: "/tenant-2", items: [.string("ready"), .string("/tenant-2")], ackID: nil),
            .event(namespace: "/tenant-2", items: [.string("late"), .bool(true)], ackID: nil),
        ])
    }
}

@Test func dynamicNamespacesHonorAuthorizationBeforeParentMiddleware() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        authorizeNamespaceConnection: { request in
            if request.namespace == "/project-auth-denied" {
                return .deny(.string("auth-denied"))
            }
            return .allow
        }
    ))

    let parent = server.dynamicNamespace(where: { $0.hasPrefix("/project-") })
    parent.use { socket, next in
        if socket.namespace == "/project-middleware-denied" {
            throw MiddlewareError("middleware-denied")
        }
        try await next()
    }
    parent.onConnection { socket in
        await socket.emit("ready", arguments: [.string(socket.namespace)])
    }

    try await server.application.test(.router) { client in
        let allowedSID = try await handshake(with: client)
        _ = try await postPolling("40/project-allowed", sid: allowedSID, with: client)
        let allowedPackets = try decodeSocketPackets(from: try await poll(sid: allowedSID, with: client).body)
        #expect(allowedPackets == [
            .connect(namespace: "/project-allowed", auth: .object(["sid": allowedPackets[0].connectPayloadSID!])),
            .event(namespace: "/project-allowed", items: [.string("ready"), .string("/project-allowed")], ackID: nil),
        ])

        let authDeniedSID = try await handshake(with: client)
        _ = try await postPolling("40/project-auth-denied", sid: authDeniedSID, with: client)
        let authDeniedPackets = try decodeSocketPackets(from: try await poll(sid: authDeniedSID, with: client).body)
        #expect(authDeniedPackets == [
            .connectError(namespace: "/project-auth-denied", data: .string("auth-denied"))
        ])

        let middlewareDeniedSID = try await handshake(with: client)
        _ = try await postPolling("40/project-middleware-denied", sid: middlewareDeniedSID, with: client)
        let middlewareDeniedPackets = try decodeSocketPackets(from: try await poll(sid: middlewareDeniedSID, with: client).body)
        #expect(middlewareDeniedPackets == [
            .connectError(
                namespace: "/project-middleware-denied",
                data: .object(["message": .string("middleware-denied")])
            )
        ])
    }
}

@Test func dynamicNamespacesStayRegisteredAfterLastDisconnectByDefault() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    let parent = server.dynamicNamespace(where: { $0 == "/tenant-persist" })
    parent.onConnection { socket in
        await socket.emit("ready", arguments: [.string(socket.namespace)])
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-persist", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("41/tenant-persist,", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        parent.onConnection { socket in
            await socket.emit("late", arguments: [.bool(true)])
        }

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-persist", sid: secondSID, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(packets == [
            .connect(namespace: "/tenant-persist", auth: .object(["sid": packets[0].connectPayloadSID!])),
            .event(namespace: "/tenant-persist", items: [.string("ready"), .string("/tenant-persist")], ackID: nil),
        ])
    }
}

@Test func dynamicNamespacesAutoCleanupAndRecreateChildrenFromParentSnapshot() async throws {
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2)
    ))

    let parent = server.dynamicNamespace(
        where: { $0 == "/tenant-ephemeral" },
        options: .init(childLifetimePolicy: .autoCleanup)
    )
    parent.onConnection { socket in
        await socket.emit("ready", arguments: [.string(socket.namespace)])
    }

    try await server.application.test(.router) { client in
        let firstSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-ephemeral", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)
        _ = try await postPolling("41/tenant-ephemeral,", sid: firstSID, with: client)
        _ = try await poll(sid: firstSID, with: client)

        parent.onConnection { socket in
            await socket.emit("late", arguments: [.bool(true)])
        }

        let secondSID = try await handshake(with: client)
        _ = try await postPolling("40/tenant-ephemeral", sid: secondSID, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: secondSID, with: client).body)
        #expect(packets == [
            .connect(namespace: "/tenant-ephemeral", auth: .object(["sid": packets[0].connectPayloadSID!])),
            .event(namespace: "/tenant-ephemeral", items: [.string("ready"), .string("/tenant-ephemeral")], ackID: nil),
            .event(namespace: "/tenant-ephemeral", items: [.string("late"), .bool(true)], ackID: nil),
        ])
    }
}

@Test func clusterBroadcastsReachRemoteNodesAndLocalScopesStayLocal() async throws {
    let bus = InMemoryClusterBus()
    let firstSockets = SocketStore()
    let secondSockets = SocketStore()
    let first = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-1", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))
    let second = Server(port: 8081, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-2", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))

    first.onConnection { socket in
        await firstSockets.append(socket)
    }
    second.onConnection { socket in
        await secondSockets.append(socket)
    }

    try await first.application.test(.router) { firstClient in
        try await second.application.test(.router) { secondClient in
            let firstConnection = try await connectRootSocket(with: firstClient)
            let secondConnection = try await connectRootSocket(with: secondClient)
            let firstSocket = try #require(await firstSockets.all().first)
            let secondSocket = try #require(await secondSockets.all().first)

            await firstSocket.join("shared")
            await secondSocket.join("shared")

            await first.to("shared").emit("cluster", arguments: [.string("all")])
            let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstConnection.sid, with: firstClient).body)
            let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondConnection.sid, with: secondClient).body)
            #expect(firstPackets == [
                .event(namespace: "/", items: [.string("cluster"), .string("all")], ackID: nil)
            ])
            #expect(secondPackets == [
                .event(namespace: "/", items: [.string("cluster"), .string("all")], ackID: nil)
            ])

            await first.local.to("shared").emit("local", arguments: [.bool(true)])
            let firstLocalPackets = try decodeSocketPackets(from: try await poll(sid: firstConnection.sid, with: firstClient).body)
            let secondLocalPackets = try decodeSocketPackets(from: try await poll(sid: secondConnection.sid, with: secondClient).body)
            #expect(firstLocalPackets == [
                .event(namespace: "/", items: [.string("local"), .bool(true)], ackID: nil)
            ])
            #expect(secondLocalPackets.isEmpty)
        }
    }
}

@Test func clusterFetchSocketsReturnsRemoteSocketsAndAllowsRemoteCommands() async throws {
    let bus = InMemoryClusterBus()
    let firstSockets = SocketStore()
    let secondSockets = SocketStore()
    let first = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-1", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))
    let second = Server(port: 8081, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-2", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))

    first.onConnection { socket in
        await firstSockets.append(socket)
    }
    second.onConnection { socket in
        await secondSockets.append(socket)
    }

    try await first.application.test(.router) { firstClient in
        try await second.application.test(.router) { secondClient in
            let firstConnection = try await connectRootSocket(with: firstClient)
            let secondConnection = try await connectRootSocket(with: secondClient)
            let firstSocket = try #require(await firstSockets.all().first)
            let secondSocket = try #require(await secondSockets.all().first)

            await firstSocket.join("shared")
            await secondSocket.join("shared")

            let fetched = await first.to("shared").fetchSockets()
            #expect(fetched.count == 2)
            let remote = try #require(fetched.first(where: { $0.id == secondSocket.id }))
            #expect(remote.rooms.contains("shared"))

            await remote.emit("remote-ping", arguments: [.string("from-node-1")])
            let remotePackets = try decodeSocketPackets(from: try await poll(sid: secondConnection.sid, with: secondClient).body)
            #expect(remotePackets == [
                .event(namespace: "/", items: [.string("remote-ping"), .string("from-node-1")], ackID: nil)
            ])

            await remote.join("beta")
            #expect(await secondSocket.rooms.contains("beta"))

            await remote.leave("beta")
            #expect(await secondSocket.rooms.contains("beta") == false)

            await remote.disconnect()
            let disconnectPackets = try decodeSocketPackets(from: try await poll(sid: secondConnection.sid, with: secondClient).body)
            #expect(disconnectPackets == [.disconnect(namespace: "/")])

            await firstSocket.emit("still-here", arguments: [.bool(true)])
            let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstConnection.sid, with: firstClient).body)
            #expect(firstPackets == [
                .event(namespace: "/", items: [.string("still-here"), .bool(true)], ackID: nil)
            ])
        }
    }
}

@Test func clusterBroadcastAcknowledgementsAggregateAcrossNodes() async throws {
    let bus = InMemoryClusterBus()
    let first = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID },
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-1", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))
    let second = Server(port: 8081, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        socketIDGenerator: { engineSID, _ in engineSID },
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-2", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))

    try await first.application.test(.router) { firstClient in
        try await second.application.test(.router) { secondClient in
            let firstConnection = try await connectRootSocket(with: firstClient)
            let secondConnection = try await connectRootSocket(with: secondClient)

            let ackTask = Task {
                try await first.emit("needs-ack", arguments: [.string("cluster")], collectingAcksWithin: .seconds(1))
            }

            let firstPackets = try decodeSocketPackets(from: try await poll(sid: firstConnection.sid, with: firstClient).body)
            let secondPackets = try decodeSocketPackets(from: try await poll(sid: secondConnection.sid, with: secondClient).body)
            guard case .event(_, _, let firstAckID?) = firstPackets.first,
                  case .event(_, _, let secondAckID?) = secondPackets.first
            else {
                Issue.record("Expected ack IDs for both cluster targets")
                return
            }

            _ = try await postPolling(
                try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("second-node")], ackID: secondAckID)),
                sid: secondConnection.sid,
                with: secondClient
            )
            _ = try await postPolling(
                try encodeClientSocketPayload(.ack(namespace: "/", items: [.string("first-node")], ackID: firstAckID)),
                sid: firstConnection.sid,
                with: firstClient
            )

            let expected = [
                (firstConnection.sid, [SocketIOValue.string("first-node")]),
                (secondConnection.sid, [SocketIOValue.string("second-node")]),
            ]
                .sorted { $0.0 < $1.0 }
                .map(\.1)
            #expect(try await ackTask.value == expected)
        }
    }
}

@Test func serverSideEmitTravelsAcrossClusterAndCollectsAcknowledgements() async throws {
    let bus = InMemoryClusterBus()
    let firstRecorder = StringRecorder()
    let secondRecorder = StringRecorder()
    let first = Server(port: 8080, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-1", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))
    let second = Server(port: 8081, configuration: .init(
        pingTimeout: .seconds(10),
        pingInterval: .seconds(10),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-2", bus: bus),
            requestTimeout: .milliseconds(200)
        )
    ))

    first.onServerEvent("cluster-ping") { event, _ in
        await firstRecorder.append(event.arguments.first.map(String.init(describing:)) ?? "missing")
    }
    second.onServerEvent("cluster-ping") { event, ack in
        await secondRecorder.append(event.arguments.first.map(String.init(describing:)) ?? "missing")
        try? await ack?.send(arguments: [.string("pong-node-2")])
    }

    try await first.application.test(.router) { firstClient in
        try await second.application.test(.router) { secondClient in
            _ = try await connectRootSocket(with: firstClient)
            _ = try await connectRootSocket(with: secondClient)

            await first.serverSideEmit("cluster-ping", arguments: [.string("hello")])
            #expect(await firstRecorder.all().isEmpty)
            #expect(await secondRecorder.all() == ["string(\"hello\")"])

            let ackResponses = try await first.serverSideEmit(
                "cluster-ping",
                arguments: [.string("again")],
                collectingAcksWithin: .seconds(1)
            )
            #expect(ackResponses == [[.string("pong-node-2")]])
        }
    }
}

@Test func connectionStateRecoveryRestoresSocketStateAndReplaysMissedPackets() async throws {
    let sockets = SocketStore()
    let recovered = RecoveryRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .milliseconds(50),
        pingInterval: .milliseconds(50),
        connectTimeout: .seconds(2),
        connectionStateRecovery: .init(maxDisconnectionDuration: .seconds(5))
    ))

    server.onConnection { socket in
        if socket.recovered {
            await recovered.append(.init(
                id: socket.id,
                recovered: socket.recovered,
                rooms: await socket.rooms,
                data: await socket.data
            ))
        } else {
            await socket.join("shared")
            await socket.setData(["role": .string("admin")])
            await sockets.append(socket)
        }
    }

    try await server.application.test(.router) { client in
        let initialConnection = try await connectRootSocket(with: client)
        let initialPID = try #require(initialConnection.packets.first?.connectPayloadPID)
        let initialSID = try #require(initialConnection.packets.first?.connectPayloadSID)
        guard case .string(let expectedSocketID) = initialSID else {
            Issue.record("Expected string socket id")
            return
        }
        let socket = try #require(await sockets.all().first)

        await socket.emit("prime", arguments: [.string("ready")])
        let primePackets = try decodeSocketPackets(from: try await poll(sid: initialConnection.sid, with: client).body)
        let primePacket = try #require(primePackets.first)
        guard case .event(_, let primeItems, nil) = primePacket else {
            Issue.record("Expected initial recovery priming event")
            return
        }
        let previousOffset = try #require(primeItems.last)

        try await Task.sleep(for: .milliseconds(220))
        await server.to("shared").emit("missed", arguments: [.string("value")])

        let recoveredSID = try await handshake(with: client)
        _ = try await postPolling(
            "40{\"pid\":\(try initialPID.encodeJSONString()),\"offset\":\(try previousOffset.encodeJSONString())}",
            sid: recoveredSID,
            with: client
        )

        let recoveredPackets = try decodeSocketPackets(from: try await poll(sid: recoveredSID, with: client).body)
        #expect(recoveredPackets.count == 2)
        #expect(recoveredPackets.first?.connectPayloadSID == initialSID)
        #expect(recoveredPackets.first?.connectPayloadPID == initialPID)

        guard case .event(_, let replayItems, nil) = try #require(recoveredPackets.last) else {
            Issue.record("Expected replayed recovery event")
            return
        }
        #expect(Array(replayItems.prefix(2)) == [.string("missed"), .string("value")])
        #expect(replayItems.last != previousOffset)

        let recoveredSnapshots = await recovered.all()
        #expect(recoveredSnapshots.count == 1)
        #expect(recoveredSnapshots[0].id == expectedSocketID)
        #expect(recoveredSnapshots[0].recovered)
        #expect(recoveredSnapshots[0].rooms.contains("shared"))
        #expect(recoveredSnapshots[0].data["role"] == .string("admin"))
    }
}

@Test func connectionStateRecoverySkipsNamespaceMiddlewaresByDefault() async throws {
    let middlewareCounter = Counter()
    let sockets = SocketStore()
    let recovered = RecoveryRecorder()
    let server = Server(port: 8080, configuration: .init(
        pingTimeout: .milliseconds(50),
        pingInterval: .milliseconds(50),
        connectTimeout: .seconds(2),
        connectionStateRecovery: .init(maxDisconnectionDuration: .seconds(5))
    ))

    server.use { _, next in
        await middlewareCounter.increment()
        try await next()
    }

    server.onConnection { socket in
        if socket.recovered {
            await recovered.append(.init(
                id: socket.id,
                recovered: socket.recovered,
                rooms: await socket.rooms,
                data: await socket.data
            ))
        } else {
            await socket.join("shared")
            await socket.setData(["scope": .string("restored")])
            await sockets.append(socket)
        }
    }

    try await server.application.test(.router) { client in
        let initialConnection = try await connectRootSocket(with: client)
        let initialPID = try #require(initialConnection.packets.first?.connectPayloadPID)
        let socket = try #require(await sockets.all().first)

        await socket.emit("prime", arguments: [.bool(true)])
        let primePackets = try decodeSocketPackets(from: try await poll(sid: initialConnection.sid, with: client).body)
        let primePacket = try #require(primePackets.first)
        guard case .event(_, let primeItems, nil) = primePacket else {
            Issue.record("Expected initial recovery priming event")
            return
        }
        let previousOffset = try #require(primeItems.last)

        try await Task.sleep(for: .milliseconds(220))
        await server.to("shared").emit("replay", arguments: [.string("payload")])

        let recoveredSID = try await handshake(with: client)
        _ = try await postPolling(
            "40{\"pid\":\(try initialPID.encodeJSONString()),\"offset\":\(try previousOffset.encodeJSONString())}",
            sid: recoveredSID,
            with: client
        )
        _ = try await poll(sid: recoveredSID, with: client)

        #expect(await middlewareCounter.current() == 1)
        let recoveredSnapshots = await recovered.all()
        #expect(recoveredSnapshots.count == 1)
        #expect(recoveredSnapshots[0].data["scope"] == .string("restored"))
    }
}

@Test func connectionStateRecoveryCanRestoreAcrossClusterNodes() async throws {
    let bus = InMemoryClusterBus()
    let recoveryStore = InMemoryConnectionStateRecoveryStore()
    let firstSockets = SocketStore()
    let recovered = RecoveryRecorder()
    let first = Server(port: 8080, configuration: .init(
        pingTimeout: .milliseconds(50),
        pingInterval: .milliseconds(50),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-1", bus: bus, recoveryStore: recoveryStore),
            requestTimeout: .milliseconds(200)
        ),
        connectionStateRecovery: .init(maxDisconnectionDuration: .seconds(5))
    ))
    let second = Server(port: 8081, configuration: .init(
        pingTimeout: .milliseconds(50),
        pingInterval: .milliseconds(50),
        connectTimeout: .seconds(2),
        cluster: .init(
            coordinator: InMemoryClusterCoordinator(nodeID: "node-2", bus: bus, recoveryStore: recoveryStore),
            requestTimeout: .milliseconds(200)
        ),
        connectionStateRecovery: .init(maxDisconnectionDuration: .seconds(5))
    ))

    first.onConnection { socket in
        if socket.recovered {
            await recovered.append(.init(
                id: socket.id,
                recovered: socket.recovered,
                rooms: await socket.rooms,
                data: await socket.data
            ))
        } else {
            await socket.join("shared")
            await socket.setData(["node": .string("node-1")])
            await firstSockets.append(socket)
        }
    }
    second.onConnection { socket in
        if socket.recovered {
            await recovered.append(.init(
                id: socket.id,
                recovered: socket.recovered,
                rooms: await socket.rooms,
                data: await socket.data
            ))
        }
    }

    try await first.application.test(.router) { firstClient in
        try await second.application.test(.router) { secondClient in
            let initialConnection = try await connectRootSocket(with: firstClient)
            let initialPID = try #require(initialConnection.packets.first?.connectPayloadPID)
            let initialSID = try #require(initialConnection.packets.first?.connectPayloadSID)
            guard case .string(let expectedSocketID) = initialSID else {
                Issue.record("Expected string socket id")
                return
            }
            let firstSocket = try #require(await firstSockets.all().first)

            await firstSocket.emit("prime", arguments: [.string("cluster")])
            let primePackets = try decodeSocketPackets(from: try await poll(sid: initialConnection.sid, with: firstClient).body)
            let primePacket = try #require(primePackets.first)
            guard case .event(_, let primeItems, nil) = primePacket else {
                Issue.record("Expected initial recovery priming event")
                return
            }
            let previousOffset = try #require(primeItems.last)

            try await Task.sleep(for: .milliseconds(220))
            await second.to("shared").emit("cluster-replay", arguments: [.string("value")])

            let recoveredSID = try await handshake(with: secondClient)
            _ = try await postPolling(
                "40{\"pid\":\(try initialPID.encodeJSONString()),\"offset\":\(try previousOffset.encodeJSONString())}",
                sid: recoveredSID,
                with: secondClient
            )

            let recoveredPackets = try decodeSocketPackets(from: try await poll(sid: recoveredSID, with: secondClient).body)
            #expect(recoveredPackets.count == 2)
            #expect(recoveredPackets.first?.connectPayloadSID == initialSID)
            #expect(recoveredPackets.first?.connectPayloadPID == initialPID)

            guard case .event(_, let replayItems, nil) = try #require(recoveredPackets.last) else {
                Issue.record("Expected replayed cluster recovery event")
                return
            }
            #expect(Array(replayItems.prefix(2)) == [.string("cluster-replay"), .string("value")])

            let recoveredSnapshots = await recovered.all()
            #expect(recoveredSnapshots.count == 1)
            #expect(recoveredSnapshots[0].recovered)
            #expect(recoveredSnapshots[0].id == expectedSocketID)
            #expect(recoveredSnapshots[0].rooms.contains("shared"))
            #expect(recoveredSnapshots[0].data["node"] == .string("node-1"))
        }
    }
}

@Test func endpointInstallsIntoExistingRouter() async throws {
    let endpoint = SocketIOEndpoint(configuration: .init(
        heartbeat: .init(
            pingTimeout: .seconds(10),
            pingInterval: .seconds(10),
            connectTimeout: .seconds(2)
        )
    ))

    endpoint.namespace("/custom").onConnection { socket in
        await socket.emit("custom-ready", arguments: [.bool(true)])
    }

    let router = Router()
    endpoint.install(into: router)
    let app = Application(router: router)

    try await app.test(.router) { client in
        let sid = try await handshake(with: client)
        _ = try await postPolling("40/custom", sid: sid, with: client)
        let packets = try decodeSocketPackets(from: try await poll(sid: sid, with: client).body)
        #expect(packets == [
            .connect(namespace: "/custom", auth: .object(["sid": packets[0].connectPayloadSID!])),
            .event(namespace: "/custom", items: [.string("custom-ready"), .bool(true)], ackID: nil),
        ])
    }
}

@Test func groupedServerConfigurationRetainsConvenienceAccessors() async throws {
    let configuration = ServerConfiguration(
        routing: .init(path: "/ws", allowsTrailingSlash: false),
        heartbeat: .init(
            pingTimeout: .seconds(11),
            pingInterval: .seconds(22),
            upgradeTimeout: .seconds(33),
            connectTimeout: .seconds(44)
        ),
        transport: .init(
            transports: [.websocket],
            allowUpgrades: false,
            maxPayload: 55
        ),
        namespaces: .init(
            socketIDGenerator: { engineSID, namespace in
                "\(namespace)#\(engineSID)"
            },
            adapterFactory: { _ in InMemoryAdapter() }
        ),
        policy: .init(
            authorizeRequest: { _ in true },
            authorizeNamespaceConnection: { request in
                .deny(.string(request.namespace))
            }
        )
    )

    #expect(configuration.path == "/ws")
    #expect(configuration.addTrailingSlash == false)
    #expect(configuration.pingTimeout == .seconds(11))
    #expect(configuration.pingInterval == .seconds(22))
    #expect(configuration.upgradeTimeout == .seconds(33))
    #expect(configuration.connectTimeout == .seconds(44))
    #expect(configuration.maxHttpBufferSize == 55)
    #expect(configuration.transports == [.websocket])
    #expect(configuration.allowUpgrades == false)
    #expect(configuration.socketIDGenerator("engine", "/chat") == "/chat#engine")

    let authorization = try await configuration.authorizeNamespaceConnection(.init(
        engineSessionID: "engine",
        namespace: "/chat",
        auth: nil,
        request: .init(method: .get, scheme: "http", authority: "localhost", path: "/ws")
    ))
    #expect(authorization == .deny(.string("/chat")))
}

private extension SocketIOPacket {
    var connectPayloadSID: SocketIOValue? {
        guard case .connect(_, let payload) = self else { return nil }
        guard case .object(let object)? = payload else { return nil }
        return object["sid"]
    }

    var connectPayloadPID: SocketIOValue? {
        guard case .connect(_, let payload) = self else { return nil }
        guard case .object(let object)? = payload else { return nil }
        return object["pid"]
    }
}
