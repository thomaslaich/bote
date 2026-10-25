$version: "2"

// Redis Streams example: the room receives posted messages on one stream,
// answers status requests on another, and publishes posted messages to a
// channel stream.
namespace chat

use bote#channels
use bote#event
use bote#redisStream
use bote#redisStreamsJson

/// The chat room API: clients post messages and read posted messages.
@title("Chat Room API")
@redisStreamsJson
@channels([ChatMessages])
service ChatRoom {
    version: "1.0.0"
    operations: [
        PostMessage
        GetRoomStatus
    ]
}

/// Post a chat message to the room.
@redisStream(key: "chat:posts", maxLen: 10000)
operation PostMessage {
    input := {
        roomId: String

        userId: String

        @length(min: 1, max: 4000)
        body: String
    }
}

/// Get the current status of a room through Redis Streams request/reply.
@redisStream(key: "chat:room-status", maxLen: 10000)
operation GetRoomStatus {
    input := {
        roomId: String
    }
    output := {
        roomId: String
        activeUsers: Integer
    }
}

/// Chat messages posted to the room.
@redisStream(key: "chat:messages", maxLen: 10000)
union ChatMessages {
    chatMessage: ChatMessagePosted
}

/// A chat message was posted to a room.
@event
structure ChatMessagePosted {
    roomId: String

    userId: String

    @length(min: 1, max: 4000)
    body: String

    sentAt: Timestamp
}
