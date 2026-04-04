# swift-server-socket.io

Socket.IO v5 server implementation in Swift, built on top of Hummingbird.

This package provides:

- `SocketIO`: the server library
- `SocketIORedisAdapter`: a Redis-backed cluster coordinator for multi-node deployments
- `SocketIOTestApp`: a small demo server used for protocol testing

## Status

The current implementation includes:

- Socket.IO v5 namespace connect/disconnect flow
- event and acknowledgement packets
- binary event and binary acknowledgement packet encoding/decoding
- namespace-scoped rooms with `join`, `leave`, and `leaveAll`
- fluent broadcasting with `to`, `excluding`, and `socket.broadcast`
- a pluggable adapter layer with a built-in `InMemoryAdapter`
- cluster-aware broadcasting with `local`, `fetchSockets`, remote socket control, and `serverSideEmit`
- an optional Redis-backed cluster coordinator for horizontal scaling
- Engine.IO-style polling and WebSocket transport support
- a Swift-first API with raw `SocketIOValue` access and typed `Codable` helpers

## Installation

Add the package dependency to your `Package.swift`:

```swift
.package(url: "https://github.com/thomasauger/swift-server-socket.io.git", exact: "5.0.0-beta.1")
```

Then depend on the `SocketIO` product:

```swift
.product(name: "SocketIO", package: "swift-server-socket.io")
```

For Redis-backed horizontal scaling, also depend on:

```swift
.product(name: "SocketIORedisAdapter", package: "swift-server-socket.io")
```

## Quick Start

```swift
import SocketIO

@main
struct App {
    static func main() async throws {
        let server = Server(
            port: 3000,
            configuration: .init(
                heartbeat: .init(
                    pingTimeout: .milliseconds(200),
                    pingInterval: .milliseconds(300),
                    connectTimeout: .milliseconds(1_000)
                ),
                transport: .init(maxPayload: 1_000_000)
            )
        )

        server.onConnection { socket in
            await socket.emit("auth", arguments: [socket.handshake.auth ?? .null])

            socket.on("message") { event, _ in
                await socket.emit("message-back", arguments: event.arguments)
            }

            socket.on("message-with-ack") { event, ack in
                try? await ack?.send(arguments: event.arguments)
            }

            await socket.join("chat")
            await socket.broadcast.to("chat").emit("joined", arguments: [.string(socket.id)])
        }

        let custom = server.namespace("/custom")
        custom.onConnection { socket in
            await socket.emit("auth", arguments: [socket.handshake.auth ?? .null])
        }

        await server.to("chat").emit("server-ready", arguments: [.bool(true)])

        try await server.run()
    }
}
```

Use a custom adapter by providing namespace settings in `ServerConfiguration`:

```swift
let configuration = ServerConfiguration(
    namespaces: .init(
        adapterFactory: { _ in InMemoryAdapter() }
    )
)
```

Use the Redis coordinator to enable cross-node broadcasts and server-side events:

```swift
import SocketIO
import SocketIORedisAdapter

let cluster = RedisClusterCoordinator(
    nodeID: "api-1",
    configuration: try .init(url: "redis://127.0.0.1:6379")
)

let server = Server(
    port: 3000,
    configuration: .init(
        cluster: .init(coordinator: cluster)
    )
)
```

Cluster-aware APIs include:

- `server.local`, `namespace.local`, and `socket.broadcast.local`
- `fetchSockets()`
- `RemoteSocket` operations: `emit`, `join`, `leave`, and `disconnect`
- `serverSideEmit(...)` with optional acknowledgement collection

When deploying multiple Socket.IO nodes behind a load balancer, sticky sessions are still required for Engine.IO polling and transport upgrades.

## Development

Run the Swift test suite:

```bash
swift test
```

Run the Docker-based protocol tests:

```bash
docker compose run --build --rm socket-test npm test
```

## References

- Socket.IO protocol specification: <https://github.com/socketio/socket.io-protocol>
- Socket.IO website: <https://socket.io/>
