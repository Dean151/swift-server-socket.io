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

/// Coordinates Socket.IO cluster traffic between server nodes.
public protocol SocketIOClusterCoordinator: Sendable {
    /// The unique identifier for the current server node.
    var nodeID: String { get }

    /// Starts the coordinator and begins delivering cluster messages.
    ///
    /// - Parameters:
    ///   - onCommand: Invoked for broadcast cluster commands.
    ///   - onResponse: Invoked for responses targeted at this node.
    func start(
        onCommand: @escaping @Sendable (Data) async -> Void,
        onResponse: @escaping @Sendable (Data) async -> Void
    ) async throws

    /// Stops the coordinator and releases any underlying resources.
    func stop() async

    /// Returns the other nodes that are currently participating in the cluster.
    func otherNodeIDs() async throws -> Set<String>

    /// Publishes a broadcast command to the cluster.
    ///
    /// The current node may receive its own published command depending on the transport.
    func publishCommand(_ data: Data) async throws

    /// Publishes a response directly to a specific node.
    func publishResponse(_ data: Data, to nodeID: String) async throws
}

