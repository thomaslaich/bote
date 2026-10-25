$version: "2"

namespace tests.redisstreams

use bote#channels
use bote#event
use bote#redisStream
use bote#redisStreamsJson

@redisStreamsJson
@channels([ChatEvents])
service ChatRoom {
    version: "1.0.0"
    operations: [PostMessage, DeleteMessage]
}

@redisStream(key: "tests.streams:commands", maxLen: 10000)
operation PostMessage {
    input := {
        roomId: String
        userId: String

        @length(min: 1, max: 4000)
        body: String
    }
}

@redisStream(key: "tests.streams:commands", maxLen: 10000)
operation DeleteMessage {
    input := {
        roomId: String
        messageId: String
    }
}

@redisStream(key: "tests.streams:events", maxLen: 10000)
union ChatEvents {
    messagePosted: MessagePosted
}

@event
structure MessagePosted {
    roomId: String
    userId: String

    @length(min: 1, max: 4000)
    body: String

    sentAt: Timestamp
}
