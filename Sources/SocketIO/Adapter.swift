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

/// Resolves namespace room membership and broadcast targets.
public protocol SocketIOAdapter: Sendable {
    /// Adds a socket to a room.
    func add(socketID: String, to room: String) async
    /// Removes a socket from a room.
    func remove(socketID: String, from room: String) async
    /// Removes a socket from every room.
    func remove(socketID: String) async
    /// Returns the rooms currently joined by a socket.
    func rooms(for socketID: String) async -> Set<String>
    /// Resolves the sockets matched by a broadcast operation.
    func resolveTargets(
        including includedRooms: Set<String>,
        excluding excludedRooms: Set<String>,
        excludingSocketIDs: Set<String>
    ) async -> Set<String>
}

/// The default in-memory adapter used by a namespace.
public actor InMemoryAdapter: SocketIOAdapter {
    private var roomMembers: [String: Set<String>] = [:]
    private var socketRooms: [String: Set<String>] = [:]

    /// Creates an empty in-memory adapter.
    public init() {}

    public func add(socketID: String, to room: String) {
        roomMembers[room, default: []].insert(socketID)
        socketRooms[socketID, default: []].insert(room)
    }

    public func remove(socketID: String, from room: String) {
        roomMembers[room]?.remove(socketID)
        if roomMembers[room]?.isEmpty == true {
            roomMembers.removeValue(forKey: room)
        }

        socketRooms[socketID]?.remove(room)
        if socketRooms[socketID]?.isEmpty == true {
            socketRooms.removeValue(forKey: socketID)
        }
    }

    public func remove(socketID: String) {
        let rooms = socketRooms.removeValue(forKey: socketID) ?? []
        for room in rooms {
            roomMembers[room]?.remove(socketID)
            if roomMembers[room]?.isEmpty == true {
                roomMembers.removeValue(forKey: room)
            }
        }
    }

    public func rooms(for socketID: String) -> Set<String> {
        socketRooms[socketID] ?? []
    }

    public func resolveTargets(
        including includedRooms: Set<String>,
        excluding excludedRooms: Set<String>,
        excludingSocketIDs: Set<String>
    ) -> Set<String> {
        var targets: Set<String>
        if includedRooms.isEmpty {
            targets = Set(socketRooms.keys)
        } else {
            targets = []
            for room in includedRooms {
                targets.formUnion(roomMembers[room] ?? [])
            }
        }

        for room in excludedRooms {
            targets.subtract(roomMembers[room] ?? [])
        }
        targets.subtract(excludingSocketIDs)
        return targets
    }
}
