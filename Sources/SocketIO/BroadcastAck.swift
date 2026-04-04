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

/// Thrown when a broadcast acknowledgement request times out before every target replies.
public struct BroadcastAckTimeoutError: Error, Sendable, Equatable {
    /// The responses that arrived before the timeout, ordered by target socket ID.
    public let responses: [[SocketIOValue]]
    /// The number of responses received before the timeout.
    public let receivedCount: Int
    /// The total number of acknowledgements expected.
    public let expectedCount: Int
    /// The number of acknowledgements still missing when the timeout elapsed.
    public let missingCount: Int

    /// Creates a broadcast acknowledgement timeout error.
    ///
    /// - Parameters:
    ///   - responses: The responses received before the timeout.
    ///   - receivedCount: The number of responses received before the timeout.
    ///   - expectedCount: The total number of acknowledgements expected.
    ///   - missingCount: The number of acknowledgements still missing.
    public init(
        responses: [[SocketIOValue]],
        receivedCount: Int,
        expectedCount: Int,
        missingCount: Int
    ) {
        self.responses = responses
        self.receivedCount = receivedCount
        self.expectedCount = expectedCount
        self.missingCount = missingCount
    }
}

actor BroadcastAckAggregator {
    private var expectedCount: Int
    private var responses: [Int: [SocketIOValue]] = [:]
    private var continuation: CheckedContinuation<[[SocketIOValue]], Error>?
    private var finished = false

    init(expectedCount: Int) {
        self.expectedCount = expectedCount
    }

    func receive(index: Int, items: [SocketIOValue]) {
        guard !finished, responses[index] == nil else { return }
        responses[index] = items
        guard responses.count == expectedCount else { return }
        finished = true
        continuation?.resume(returning: orderedResponses())
        continuation = nil
    }

    func cancel(index: Int) {
        guard !finished, responses[index] == nil, expectedCount > 0 else { return }
        expectedCount -= 1
        if expectedCount == 0 || responses.count == expectedCount {
            finished = true
            continuation?.resume(returning: orderedResponses())
            continuation = nil
        }
    }

    func failForTimeout() {
        guard !finished else { return }
        finished = true
        continuation?.resume(throwing: BroadcastAckTimeoutError(
            responses: orderedResponses(),
            receivedCount: responses.count,
            expectedCount: expectedCount,
            missingCount: expectedCount - responses.count
        ))
        continuation = nil
    }

    func wait(
        timeout: Duration,
        onTimeout: @escaping @Sendable () async -> Void
    ) async throws -> [[SocketIOValue]] {
        guard expectedCount > 0 else { return [] }
        if finished {
            return orderedResponses()
        }

        let timeoutTask = Task {
            do {
                try await Task.sleep(for: timeout)
            } catch {
                return
            }
            await onTimeout()
            failForTimeout()
        }

        do {
            let result = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[[SocketIOValue]], Error>) in
                if finished {
                    continuation.resume(returning: orderedResponses())
                } else {
                    self.continuation = continuation
                }
            }
            timeoutTask.cancel()
            return result
        } catch {
            timeoutTask.cancel()
            throw error
        }
    }

    private func orderedResponses() -> [[SocketIOValue]] {
        responses.keys.sorted().compactMap { responses[$0] }
    }

    func indexedResponses() -> [(Int, [SocketIOValue])] {
        responses.keys.sorted().compactMap { index in
            guard let response = responses[index] else { return nil }
            return (index, response)
        }
    }
}
