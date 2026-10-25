$version: "2"

// Redis Pub/Sub example: the service receives presence commands on one
// Pub/Sub channel and publishes presence changes on another.
namespace presence

use bote#channels
use bote#event
use bote#redisChannel
use bote#redisPubSubJson

/// Presence API: clients set their presence and subscribe to presence changes.
@title("Presence API")
@redisPubSubJson
@channels([PresenceUpdates])
service Presence {
    version: "1.0.0"
    operations: [
        SetPresence
    ]
}

/// Set a user's presence.
@redisChannel(name: "presence.commands")
operation SetPresence {
    input: SetPresenceCommand
}

/// Command to set a user's presence.
@input
structure SetPresenceCommand {
    userId: String
    status: PresenceStatus
}

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
