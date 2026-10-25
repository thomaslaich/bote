namespace Bote.Messaging;

/// <summary>Encoded contract data. The transport owns delivery and settlement metadata.</summary>
public sealed record MessagePayload(
    byte[] Value,
    string? Key = null,
    IReadOnlyDictionary<string, byte[]>? Headers = null,
    string? ContentType = null
);
