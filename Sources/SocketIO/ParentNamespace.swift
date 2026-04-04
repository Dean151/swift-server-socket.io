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

/// The lifetime policy for dynamically created child namespaces.
public enum DynamicChildLifetimePolicy: Sendable {
    /// Keep child namespaces registered after their last socket disconnects.
    case keepRegistered
    /// Remove child namespaces after their last socket disconnects.
    case autoCleanup
}

/// Options that control how a parent namespace materializes child namespaces.
public struct ParentNamespaceOptions: Sendable {
    /// The lifetime policy applied to children created by this parent namespace.
    public let childLifetimePolicy: DynamicChildLifetimePolicy

    /// Creates parent namespace options.
    ///
    /// - Parameter childLifetimePolicy: The lifetime policy applied to dynamically created children.
    public init(childLifetimePolicy: DynamicChildLifetimePolicy = .keepRegistered) {
        self.childLifetimePolicy = childLifetimePolicy
    }
}

/// A dynamic parent namespace that can materialize child namespaces on demand.
public struct ParentNamespace: Sendable {
    /// The connection handler invoked for newly connected child sockets.
    public typealias ConnectionHandler = Namespace.ConnectionHandler

    private let id: Int
    private let core: ServerCore

    init(id: Int, core: ServerCore) {
        self.id = id
        self.core = core
    }

    /// Registers a handler for sockets that join child namespaces created by this parent.
    ///
    /// The handler is copied into each child namespace when that child is first created.
    ///
    /// - Parameter handler: The handler invoked once a child namespace connection is accepted.
    public func onConnection(_ handler: @escaping ConnectionHandler) {
        core.addConnectionHandler(forParentNamespace: id, handler: handler)
    }

    /// Registers a connect-time middleware for child namespaces created by this parent.
    ///
    /// The middleware is copied into each child namespace when that child is first created.
    ///
    /// - Parameter middleware: The middleware invoked before a child socket joins its namespace.
    public func use(_ middleware: @escaping NamespaceMiddleware) {
        core.addNamespaceMiddleware(forParentNamespace: id, middleware: middleware)
    }

    /// Registers a server-side event handler for child namespaces created by this parent.
    ///
    /// The handler is copied into each child namespace when that child is first created.
    ///
    /// - Parameters:
    ///   - event: The event name to observe.
    ///   - handler: The handler invoked when the event is received.
    public func onServerEvent(_ event: String, handler: @escaping ServerSideEventHandler) {
        core.addServerEventHandler(forParentNamespace: id, event: event, handler: handler)
    }
}
