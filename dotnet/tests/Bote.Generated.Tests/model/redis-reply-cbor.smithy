$version: "2"

namespace tests.redisreplycbor

use bote#redisStream
use bote#redisStreamsCbor

@redisStreamsCbor
service Inventory {
    version: "1.0.0"
    operations: [GetStock]
}

@redisStream(key: "tests.replycbor:queries", maxLen: 10000)
operation GetStock {
    input := {
        productId: String
    }

    output := {
        productId: String
        available: Integer
    }
}
