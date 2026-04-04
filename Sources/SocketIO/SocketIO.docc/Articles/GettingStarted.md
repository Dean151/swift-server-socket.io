# Getting Started

Set up `SocketIO` when you need the Socket.IO protocol layer in a Swift server. The package handles namespace connections, event delivery, acknowledgements, and room-based broadcasts while delegating transport details to Engine.IO.

## Create A Server

For a standalone server, create ``Server`` with a ``ServerConfiguration`` and run it:

```swift
import SocketIO

@main
struct App {
    static func main() async throws {
        let server = Server(
            port: 3000,
            configuration: .init(
                heartbeat: .init(
                    pingTimeout: .seconds(20),
                    pingInterval: .seconds(30),
                    connectTimeout: .seconds(5)
                )
            )
        )

        server.onConnection { socket in
            socket.on("message") { event, ack in
                try? await ack?.send(arguments: event.arguments)
            }
        }

        server.namespace("/chat").onConnection { socket in
            await socket.join("lobby")
            await socket.emit("ready", arguments: [.bool(true)])
        }

        try await server.run()
    }
}
```

## Embed In An Existing Hummingbird App

If you already have a Hummingbird application, install ``SocketIOEndpoint`` into the router and forward upgrade requests using ``SocketIOEndpoint/webSocketConfiguration`` and ``SocketIOEndpoint/shouldUpgrade(request:logger:)``.

```swift
import Hummingbird
import SocketIO

let endpoint = SocketIOEndpoint(configuration: .init())
let router = Router()
endpoint.install(into: router)

let app = Application(
    router: router,
    server: .http1WebSocketUpgrade(configuration: endpoint.webSocketConfiguration) { request, _, logger in
        await endpoint.shouldUpgrade(request: request, logger: logger)
    }
)
```

## Customize Behavior

Use ``ServerConfiguration`` to tune how the server behaves:

- Adjust routing with ``ServerConfiguration/Routing``
- Configure heartbeat and namespace join timeouts with ``ServerConfiguration/Heartbeat``
- Restrict transport exposure with ``ServerConfiguration/TransportConfiguration``
- Customize socket identifiers and adapters with ``ServerConfiguration/Namespaces``
- Gate requests and namespace joins with ``ServerConfiguration/Policy``

## Next Steps

- Use ``Namespace`` and ``BroadcastOperator`` to target rooms and namespaces
- Use ``SocketHandshake`` and ``NamespaceAuthorizationRequest`` when you need auth-aware behavior
- Use ``SocketIOValue`` when you need raw payload access alongside `Codable` helpers
