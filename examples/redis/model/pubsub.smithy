$version: "2"

// Redis Pub/Sub example: the service broadcasts presence changes to every
// subscriber. Pub/Sub keeps no messages, so the service has no commands.
namespace presence

use bote#channels
use bote#event
use bote#redisChannel
use bote#redisPubSubJson

/// Presence API: clients subscribe to presence changes.
@title("Presence API")
@redisPubSubJson
@channels([PresenceUpdates])
service Presence { version: "1.0.0" }

/// Presence changes.
@redisChannel(name: "presence")
union PresenceUpdates {
    presenceChanged: PresenceChanged
}

/// A user's presence changed.
@event
structure PresenceChanged {
    userId: String
    status: PresenceStatus
    sentAt: Timestamp
}

enum PresenceStatus {
    ONLINE
    AWAY
    OFFLINE
}
