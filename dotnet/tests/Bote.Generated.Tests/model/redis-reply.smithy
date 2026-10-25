$version: "2"

namespace tests.redisreply

use bote#redisStream
use bote#redisStreamsJson

@redisStreamsJson
service Inventory {
    version: "1.0.0"
    operations: [GetStock]
}

@redisStream(key: "tests.reply:queries", maxLen: 10000)
operation GetStock {
    input := {
        productId: String
    }

    output := {
        productId: String
        available: Integer
    }
}
