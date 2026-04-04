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

/// Advances a middleware chain to the next registered middleware.
public struct MiddlewareNext: Sendable {
    private let operation: @Sendable () async throws -> Void

    init(operation: @escaping @Sendable () async throws -> Void) {
        self.operation = operation
    }

    /// Continues the middleware chain.
    public func callAsFunction() async throws {
        try await operation()
    }
}

/// A connect-time middleware invoked before a namespace connection is accepted.
public typealias NamespaceMiddleware = @Sendable (Socket, MiddlewareNext) async throws -> Void

/// A packet middleware invoked before an inbound event reaches event handlers.
public typealias SocketMiddleware = @Sendable (SocketEvent, SocketAck?, MiddlewareNext) async throws -> Void

/// Describes an error that should be translated into Socket.IO middleware payload data.
public protocol SocketIOMiddlewareError: Error {
    /// The message exposed to the remote peer.
    var message: String { get }
    /// Optional additional payload data exposed to the remote peer.
    var socketIOData: SocketIOValue? { get }
}

/// A convenience middleware error with a standard message and optional payload.
public struct MiddlewareError: SocketIOMiddlewareError, Equatable {
    /// The message exposed to the remote peer.
    public let message: String
    /// Optional additional payload data exposed to the remote peer.
    public let data: SocketIOValue?

    /// Creates a middleware error.
    ///
    /// - Parameters:
    ///   - message: The message exposed to the remote peer.
    ///   - data: Optional additional payload data.
    public init(_ message: String, data: SocketIOValue? = nil) {
        self.message = message
        self.data = data
    }

    public var socketIOData: SocketIOValue? { data }
}

enum MiddlewareExecutionError: SocketIOMiddlewareError, Equatable {
    case nextCalledMultipleTimes

    var message: String {
        switch self {
        case .nextCalledMultipleTimes:
            "Middleware next() may only be called once"
        }
    }

    var socketIOData: SocketIOValue? { nil }
}

private actor MiddlewareNextGate {
    private var didCall = false

    func claim() -> Bool {
        guard !didCall else { return false }
        didCall = true
        return true
    }

    func wasCalled() -> Bool {
        didCall
    }
}

private actor MiddlewareNextResult {
    private var didReachEnd = false

    func store(_ value: Bool) {
        didReachEnd = value
    }

    func value() -> Bool {
        didReachEnd
    }
}

private func makeNext(
    _ operation: @escaping @Sendable () async throws -> Void
) -> MiddlewareNext {
    let gate = MiddlewareNextGate()
    return MiddlewareNext {
        guard await gate.claim() else {
            throw MiddlewareExecutionError.nextCalledMultipleTimes
        }
        try await operation()
    }
}

private func runMiddlewareChain(
    count: Int,
    step: @escaping @Sendable (Int, MiddlewareNext) async throws -> Void
) async throws -> Bool {
    actor Runner {
        let count: Int
        let step: @Sendable (Int, MiddlewareNext) async throws -> Void

        init(
            count: Int,
            step: @escaping @Sendable (Int, MiddlewareNext) async throws -> Void
        ) {
            self.count = count
            self.step = step
        }

        func run(_ index: Int) async throws -> Bool {
            guard index < count else { return true }

            let gate = MiddlewareNextGate()
            let result = MiddlewareNextResult()
            let next = MiddlewareNext {
                guard await gate.claim() else {
                    throw MiddlewareExecutionError.nextCalledMultipleTimes
                }
                await result.store(try await self.run(index + 1))
            }

            try await step(index, next)
            guard await gate.wasCalled() else {
                return false
            }
            return await result.value()
        }
    }

    return try await Runner(count: count, step: step).run(0)
}

func runNamespaceMiddlewares(
    _ middlewares: [NamespaceMiddleware],
    socket: Socket
) async throws {
    _ = try await runMiddlewareChain(count: middlewares.count) { index, next in
        try await middlewares[index](socket, next)
    }
}

func runSocketMiddlewares(
    _ middlewares: [SocketMiddleware],
    event: SocketEvent,
    ack: SocketAck?
) async throws -> Bool {
    try await runMiddlewareChain(count: middlewares.count) { index, next in
        try await middlewares[index](event, ack, next)
    }
}
