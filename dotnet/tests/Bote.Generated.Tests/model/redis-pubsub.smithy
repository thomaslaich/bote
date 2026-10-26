$version: "2"

namespace tests.redispubsub

use bote#channels
use bote#event
use bote#redisChannel
use bote#redisPubSubJson

@redisPubSubJson
@channels([ChatEvents])
service ChatRoom {
    version: "1.0.0"
}

@redisChannel(name: "tests.pubsub:events")
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
