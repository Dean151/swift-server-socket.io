# ``SocketIO``

Socket.IO server support for Swift applications built on top of Hummingbird and Engine.IO.

`SocketIO` provides a server-side implementation of the Socket.IO v5 protocol, including namespace joins, event delivery, acknowledgements, room-based broadcasting, and namespace authorization.

## Overview

Use ``Server`` when you want a ready-to-run Hummingbird service, or use ``SocketIOEndpoint`` when you want to install Socket.IO into an existing router and upgrade pipeline.

The library centers around:

- ``ServerConfiguration`` for routing, transport, namespace, and policy configuration
- ``Socket`` for interacting with a connected client
- ``Namespace`` and ``BroadcastOperator`` for namespace-scoped broadcasting
- ``RemoteSocket`` for fetched local or cross-node socket snapshots
- ``NamespaceMiddleware``, ``SocketMiddleware``, and ``MiddlewareNext`` for connect-time and packet middleware
- ``SocketIOValue`` for raw event payload values and typed `Codable` conversion

## Topics

### Essentials

- <doc:GettingStarted>

### Server Setup

- ``Server``
- ``SocketIOEndpoint``
- ``ServerConfiguration``

### Namespaces And Rooms

- ``Namespace``
- ``BroadcastOperator``
- ``BroadcastAckTimeoutError``
- ``SocketIOAdapter``
- ``InMemoryAdapter``
- ``RemoteSocket``
- ``RemoteSocketHandshake``
- ``SocketIOClusterCoordinator``
- ``ServerSideAck``
- ``ServerSideAckTimeoutError``

### Socket Lifecycle

- ``Socket``
- ``SocketHandshake``
- ``SocketEvent``
- ``SocketAck``
- ``SocketAckError``
- ``SocketDisconnectReason``
- ``NamespaceMiddleware``
- ``SocketMiddleware``
- ``MiddlewareNext``
- ``MiddlewareError``
- ``SocketIOMiddlewareError``

### Payloads And Authorization

- ``SocketIOValue``
- ``SocketIOCodingError``
- ``NamespaceAuthorizationRequest``
- ``NamespaceAuthorizationResult``
