$version: "2"

// Redis Pub/Sub example: PUBLISH and SUBSCRIBE capabilities on one channel.
namespace presence

use bote#command
use bote#event
use bote#redisPubSubJson
use bote#redisPublish
use bote#redisSubscribe

/// Presence API: clients set their presence and subscribe to presence changes.
@title("Presence API")
@redisPubSubJson
service Presence {
    version: "1.0.0"
    operations: [
        SetPresence
        SubscribeToPresence
    ]
}

/// Set a user's presence.
@redisPublish(channel: "presence")
operation SetPresence {
    input: SetPresenceCommand
}

/// Subscribe to presence changes on the presence channel.
@redisSubscribe(channel: "presence")
operation SubscribeToPresence {
    output := {
        updates: PresenceSubscription
    }
}

/// Command to set a user's presence.
@command
structure SetPresenceCommand {
    userId: String
    status: PresenceStatus
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

/// The client's subscription view of presence changes.
@streaming
union PresenceSubscription {
    presenceChanged: PresenceChanged
}
