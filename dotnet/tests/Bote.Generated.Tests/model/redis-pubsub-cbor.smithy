$version: "2"

namespace tests.redispubsubcbor

use bote#channels
use bote#event
use bote#redisChannel
use bote#redisPubSubCbor

@redisPubSubCbor
@channels([ChatEvents])
service ChatRoom {
    version: "1.0.0"
}

@redisChannel(name: "tests.pubsubcbor:events")
union ChatEvents {
    messagePosted: MessagePosted
}

@event
structure MessagePosted {
    roomId: String
    userId: String
    body: String
    sentAt: Timestamp
}
