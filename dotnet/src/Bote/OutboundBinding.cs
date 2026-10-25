namespace Bote;

/// <summary>
/// How one kind of message goes onto the wire: its address and its encoder. Generated code
/// creates one per command and per event and hands it to an <see cref="IMessageSender"/>.
/// </summary>
public class OutboundBinding<T>(
    string serviceId,
    string operationId,
    string address,
    Func<T, MessagePayload> encode,
    long? maxLength = null
)
{
    public string ServiceId { get; } = serviceId;
    public string OperationId { get; } = operationId;
    public string Address { get; } = address;
    public Func<T, MessagePayload> Encode { get; } = encode;

    /// <summary>The approximate length a Redis stream is trimmed to on append, if any.</summary>
    public long? MaxLength { get; } = maxLength;
}

/// <summary>An <see cref="OutboundBinding{T}"/> whose receiver answers, with the decoder for the answer.</summary>
public sealed class OutboundBinding<TRequest, TReply>(
    string serviceId,
    string operationId,
    string address,
    Func<TRequest, MessagePayload> encode,
    Func<MessagePayload, TReply> decodeReply,
    long? maxLength = null
) : OutboundBinding<TRequest>(serviceId, operationId, address, encode, maxLength)
{
    public Func<MessagePayload, TReply> DecodeReply { get; } = decodeReply;
}

public interface IMessageSender
{
    Task SendAsync<T>(
        OutboundBinding<T> outbound,
        T message,
        CancellationToken cancellationToken = default
    );
}

public interface IMessageRequestSender
{
    Task<TReply> RequestAsync<TRequest, TReply>(
        OutboundBinding<TRequest, TReply> outbound,
        TRequest request,
        CancellationToken cancellationToken = default
    );
}
