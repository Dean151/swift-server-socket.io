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

import SocketIO

@main
struct App {
    static func main() async throws {
        let server = Server(port: 3000, configuration: .init(
            heartbeat: .init(
                pingTimeout: .milliseconds(200),
                pingInterval: .milliseconds(300)
            ),
            transport: .init(maxPayload: 1_000_000)
        ))

        server.onConnection { socket in
            await socket.emit("auth", arguments: [socket.handshake.auth ?? .object([:])])

            socket.on("message") { event, _ in
                await socket.emit("message-back", arguments: event.arguments)
            }

            socket.on("message-with-ack") { event, ack in
                try? await ack?.send(arguments: event.arguments)
            }
        }

        let customNamespace = server.namespace("/custom")
        customNamespace.onConnection { socket in
            await socket.emit("auth", arguments: [socket.handshake.auth ?? .object([:])])
        }

        try await server.run()
    }
}
