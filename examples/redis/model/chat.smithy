$version: "2"

// Redis Streams example on the unified @channel model. The stream is a marker
// channel shape carrying @redisStream. Producer and consumer bind to it with
// @channel, exactly like the Kafka example; only the address trait differs.
namespace chat

use bote#channel
use bote#messaging
use bote#receive
use bote#redisStream
use bote#send

/// The chat-messages stream (channel). Its name and config are declared once here.
@redisStream(name: "chat:messages", maxLen: 10000)
structure ChatMessagesStream {}

/// A chat producer: posts messages to the chat stream.
@title("Chat Producer API")
@messaging
service ChatProducer {
    version: "1.0.0"
    operations: [
        PostMessage
    ]
}

/// A chat consumer: reads messages from the chat stream.
@title("Chat Consumer API")
@messaging
service ChatConsumer {
    version: "1.0.0"
    operations: [
        ConsumeMessages
    ]
}

/// Post a chat message to the room stream.
@send
@channel(ChatMessagesStream)
operation PostMessage {
    input: ChatMessage
}

/// Consume chat messages from the room stream.
@receive
@channel(ChatMessagesStream)
operation ConsumeMessages {
    output := {
        messages: ChatMessageSubscription
    }
}

/// A message posted to a chat room.
structure ChatMessage {
    roomId: String

    userId: String

    @length(min: 1, max: 4000)
    body: String

    sentAt: Timestamp
}

/// The consumer's subscription — receiver-side streaming view, not shared contract.
@streaming
union ChatMessageSubscription {
    chatMessage: ChatMessage
}
