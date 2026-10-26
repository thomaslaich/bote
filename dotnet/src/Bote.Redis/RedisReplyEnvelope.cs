using System.Formats.Cbor;
using System.Text;
using System.Text.Json;

namespace Bote.Redis;

/// <summary>
/// The Pub/Sub message that carries a reply and its correlation ID. It uses the reply's own
/// encoding: a JSON object for JSON protocols, a CBOR map for CBOR ones.
/// </summary>
internal static class RedisReplyEnvelope
{
    internal const string CborContentType = "application/cbor";

    private const string CorrelationIdKey = "correlation_id";
    private const string DataKey = "data";

    internal static byte[] Encode(string correlationId, MessagePayload reply) =>
        reply.ContentType == CborContentType
            ? EncodeCbor(correlationId, reply.Value)
            : EncodeJson(correlationId, reply.Value);

    internal static (string CorrelationId, byte[] Payload) Decode(
        byte[] bytes,
        string? contentType
    ) => contentType == CborContentType ? DecodeCbor(bytes) : DecodeJson(bytes);

    private static byte[] EncodeJson(string correlationId, byte[] payload)
    {
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream))
        {
            json.WriteStartObject();
            json.WriteString(CorrelationIdKey, correlationId);
            json.WritePropertyName(DataKey);
            json.WriteRawValue(payload);
            json.WriteEndObject();
        }
        return stream.ToArray();
    }

    private static (string CorrelationId, byte[] Payload) DecodeJson(byte[] bytes)
    {
        using var json = JsonDocument.Parse(bytes);
        var root = json.RootElement;
        return (
            root.GetProperty(CorrelationIdKey).GetString()
                ?? throw new JsonException("Missing reply correlation ID."),
            Encoding.UTF8.GetBytes(root.GetProperty(DataKey).GetRawText())
        );
    }

    private static byte[] EncodeCbor(string correlationId, byte[] payload)
    {
        var cbor = new CborWriter();
        cbor.WriteStartMap(2);
        cbor.WriteTextString(CorrelationIdKey);
        cbor.WriteTextString(correlationId);
        cbor.WriteTextString(DataKey);
        cbor.WriteEncodedValue(payload);
        cbor.WriteEndMap();
        return cbor.Encode();
    }

    private static (string CorrelationId, byte[] Payload) DecodeCbor(byte[] bytes)
    {
        var cbor = new CborReader(bytes);
        string? correlationId = null;
        byte[]? payload = null;
        var count = cbor.ReadStartMap();
        for (var i = 0; count is null ? cbor.PeekState() != CborReaderState.EndMap : i < count; i++)
        {
            switch (cbor.ReadTextString())
            {
                case CorrelationIdKey:
                    correlationId = cbor.ReadTextString();
                    break;
                case DataKey:
                    payload = cbor.ReadEncodedValue().ToArray();
                    break;
                default:
                    cbor.SkipValue();
                    break;
            }
        }
        cbor.ReadEndMap();
        return (
            correlationId ?? throw new CborContentException("Missing reply correlation ID."),
            payload ?? throw new CborContentException("Missing reply data.")
        );
    }
}
