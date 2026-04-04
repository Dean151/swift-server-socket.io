// swift-tools-version: 6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-server-socket.io",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "SocketIO", targets: ["SocketIO"]),
        .library(name: "SocketIORedisAdapter", targets: ["SocketIORedisAdapter"]),
        .executable(name: "SocketIOTestApp", targets: ["SocketIOTestApp"]),
    ],
    dependencies: [
        .package(url: "https://github.com/Dean151/swift-server-engine.io", exact: "4.0.0-beta.2"),
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.4.1"),
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.3.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.76.0"),
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.2.0"),
        .package(url: "https://github.com/hummingbird-project/hummingbird-websocket.git", from: "2.1.0"),
        .package(url: "https://github.com/swhitty/swift-mutex.git", from: "0.0.6"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.0.0"),
    ],
    targets: [
        .target(name: "SocketIO", dependencies: [
            .product(name: "EngineIO", package: "swift-server-engine.io"),
            .product(name: "HTTPTypes", package: "swift-http-types"),
            .product(name: "Hummingbird", package: "hummingbird"),
            .product(name: "HummingbirdWebSocket", package: "hummingbird-websocket"),
            .product(name: "Logging", package: "swift-log"),
            .product(name: "Mutex", package: "swift-mutex"),
            .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
        ]),
        .target(name: "SocketIORedisAdapter", dependencies: [
            "SocketIO",
            .product(name: "NIOCore", package: "swift-nio"),
            .product(name: "NIOPosix", package: "swift-nio"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .testTarget(name: "SocketIOTests", dependencies: [
            "SocketIO",
            .product(name: "HummingbirdTesting", package: "hummingbird"),
        ]),
        .executableTarget(name: "SocketIOTestApp", dependencies: ["SocketIO"], path: "Demo/SocketIO"),
    ],
    swiftLanguageModes: [.v6]
)
