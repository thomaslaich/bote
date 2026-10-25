using System.Formats.Cbor;
using Bote.Redis;

namespace Bote.Tests;

public sealed class RedisReplyEnvelopeTests
{
    [Fact]
    public void JsonReplyRoundTrips()
    {
        var bytes = RedisReplyEnvelope.Encode(
            "c-1",
            new MessagePayload("{\"available\":42}"u8.ToArray())
        );

        Assert.Equal(
            "{\"correlation_id\":\"c-1\",\"data\":{\"available\":42}}",
            System.Text.Encoding.UTF8.GetString(bytes)
        );
        var (correlationId, payload) = RedisReplyEnvelope.Decode(bytes, "application/json");
        Assert.Equal("c-1", correlationId);
        Assert.Equal("{\"available\":42}"u8.ToArray(), payload);
    }

    [Fact]
    public void CborReplyIsACborMapWithTheEncodedReply()
    {
        var reply = new CborWriter();
        reply.WriteStartMap(1);
        reply.WriteTextString("available");
        reply.WriteInt32(42);
        reply.WriteEndMap();
        var replyBytes = reply.Encode();

        var bytes = RedisReplyEnvelope.Encode(
            "c-1",
            new MessagePayload(replyBytes, ContentType: "application/cbor")
        );

        var envelope = new CborReader(bytes);
        Assert.Equal(2, envelope.ReadStartMap());
        Assert.Equal("correlation_id", envelope.ReadTextString());
        Assert.Equal("c-1", envelope.ReadTextString());
        Assert.Equal("data", envelope.ReadTextString());
        Assert.Equal(replyBytes, envelope.ReadEncodedValue().ToArray());
        var (correlationId, payload) = RedisReplyEnvelope.Decode(bytes, "application/cbor");
        Assert.Equal("c-1", correlationId);
        Assert.Equal(replyBytes, payload);
    }
}
