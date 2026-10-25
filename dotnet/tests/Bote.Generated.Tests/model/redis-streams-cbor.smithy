$version: "2"

namespace tests.redisstreamscbor

use bote#channels
use bote#event
use bote#redisStream
use bote#redisStreamsCbor

@redisStreamsCbor
@channels([ChatEvents])
service ChatRoom {
    version: "1.0.0"
    operations: [PostMessage, DeleteMessage]
}

@redisStream(key: "tests.streamscbor:commands", maxLen: 10000)
operation PostMessage {
    input := {
        roomId: String
        userId: String

        @length(min: 1, max: 4000)
        body: String
    }
}

@redisStream(key: "tests.streamscbor:commands", maxLen: 10000)
operation DeleteMessage {
    input := {
        roomId: String
        messageId: String
    }
}

@redisStream(key: "tests.streamscbor:events", maxLen: 10000)
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
