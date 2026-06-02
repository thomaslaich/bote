$version: "2"

// Redis Pub/Sub example on the unified @channel model. The channel owns nothing but
// its name (Pub/Sub is ephemeral — no retention, no config), so the marker shape is
// genuinely thin. It still binds the same way (@channel) and has a home for
// channel-level docs, which is the payoff of keeping it a shape.
namespace presence

use bote#channel
use bote#messaging
use bote#receive
use bote#redisChannel
use bote#send

/// The presence channel. Pub/Sub is ephemeral, so the channel is just an address.
@redisChannel(name: "presence")
structure PresenceChannel {}

/// Publishes user presence updates.
@title("Presence Publisher API")
@messaging
service PresencePublisher {
    version: "1.0.0"
    operations: [
        PublishPresence
    ]
}

/// Subscribes to user presence updates.
@title("Presence Subscriber API")
@messaging
service PresenceSubscriber {
    version: "1.0.0"
    operations: [
        SubscribePresence
    ]
}

/// Publish a presence update to the presence channel.
@send
@channel(PresenceChannel)
operation PublishPresence {
    input: PresenceUpdate
}

/// Subscribe to presence updates on the presence channel.
@receive
@channel(PresenceChannel)
operation SubscribePresence {
    output := {
        updates: PresenceSubscription
    }
}

/// A user's presence changing.
structure PresenceUpdate {
    userId: String
    status: PresenceStatus
    sentAt: Timestamp
}

enum PresenceStatus {
    ONLINE
    AWAY
    OFFLINE
}

/// The subscriber's streaming subscription — receiver-side, not shared contract.
@streaming
union PresenceSubscription {
    presenceUpdate: PresenceUpdate
}
