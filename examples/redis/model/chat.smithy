$version: "2"

// Redis Streams example using operation-level stream addresses.
namespace chat

use bote#command
use bote#event
use bote#invocation
use bote#redisStream
use bote#redisStreamsJson
use bote#subscription

/// The chat room API: clients post messages and subscribe to posted messages.
@title("Chat Room API")
@redisStreamsJson
service ChatRoom {
    version: "1.0.0"
    operations: [
        PostMessage
        SubscribeToMessages
    ]
}

/// Post a chat message to the room stream.
@invocation
@redisStream(name: "chat:messages", maxLen: 10000)
operation PostMessage {
    input: PostChatMessage
}

/// Subscribe to posted chat messages from the room stream.
@subscription
@redisStream(name: "chat:messages", maxLen: 10000)
operation SubscribeToMessages {
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
