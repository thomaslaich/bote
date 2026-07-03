$version: "2"

// Redis Streams example: XADD and XREAD capabilities on one stream.
namespace chat

use bote#command
use bote#event
use bote#redisStreamAdd
use bote#redisStreamRead
use bote#redisStreamsJson

/// The chat room API: clients post messages and read posted messages.
@title("Chat Room API")
@redisStreamsJson
service ChatRoom {
    version: "1.0.0"
    operations: [
        PostMessage
        ReadMessages
    ]
}

/// Post a chat message to the room stream.
@redisStreamAdd(stream: "chat:messages", maxLen: 10000)
operation PostMessage {
    input: PostChatMessage
}

/// Read posted chat messages from the room stream.
@redisStreamRead(stream: "chat:messages", maxLen: 10000)
operation ReadMessages {
    output := {
        messages: ChatMessageSubscription
    }
}

/// Command to post a chat message to a room.
@command
structure PostChatMessage {
    roomId: String

    userId: String

    @length(min: 1, max: 4000)
    body: String
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

/// The client's subscription view of posted chat messages.
@streaming
union ChatMessageSubscription {
    chatMessage: ChatMessagePosted
}
