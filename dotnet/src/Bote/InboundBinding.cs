using Microsoft.Extensions.DependencyInjection;

namespace Bote;

/// <summary>
/// How one kind of message comes off the wire: its address, its decoder, and the handler it goes
/// to. Generated code creates one per command and per channel; a consumer holds them by address
/// and dispatches each delivery. Successful completion permits settlement.
/// </summary>
public sealed class InboundBinding
{
    private readonly Type handlerType;
    private readonly Func<
        MessagePayload,
        IServiceProvider,
        CancellationToken,
        Task<MessagePayload?>
    > dispatch;

    private InboundBinding(
        string serviceId,
        string operationId,
        string address,
        Type handlerType,
        bool hasReply,
        Func<MessagePayload, IServiceProvider, CancellationToken, Task<MessagePayload?>> dispatch
    )
    {
        ServiceId = serviceId;
        OperationId = operationId;
        Address = address;
        HasReply = hasReply;
        this.handlerType = handlerType;
        this.dispatch = dispatch;
    }

    public string ServiceId { get; }
    public string OperationId { get; }
    public string Address { get; }

    /// <summary>Whether the handler answers, and the transport publishes the reply before settlement.</summary>
    public bool HasReply { get; }

    /// <summary>A message the handler consumes without answering.</summary>
    public static InboundBinding Create<T, THandler>(
        string serviceId,
        string operationId,
        string address,
        Func<MessagePayload, T> decode,
        Func<THandler, T, CancellationToken, Task> handle
    )
        where THandler : notnull
    {
        ArgumentNullException.ThrowIfNull(decode);
        ArgumentNullException.ThrowIfNull(handle);
        return new(
            serviceId,
            operationId,
            address,
            typeof(THandler),
            hasReply: false,
            async (payload, services, cancellationToken) =>
            {
                var message = decode(payload);
                await handle(services.GetRequiredService<THandler>(), message, cancellationToken)
                    .ConfigureAwait(false);
                return null;
            }
        );
    }

    /// <summary>A request the handler answers; the reply is encoded for the transport to publish.</summary>
    public static InboundBinding CreateReply<TRequest, TReply, THandler>(
        string serviceId,
        string operationId,
        string address,
        Func<MessagePayload, TRequest> decode,
        Func<TReply, MessagePayload> encodeReply,
        Func<THandler, TRequest, CancellationToken, Task<TReply>> handle
    )
        where THandler : notnull
    {
        ArgumentNullException.ThrowIfNull(decode);
        ArgumentNullException.ThrowIfNull(encodeReply);
        ArgumentNullException.ThrowIfNull(handle);
        return new(
            serviceId,
            operationId,
            address,
            typeof(THandler),
            hasReply: true,
            async (payload, services, cancellationToken) =>
            {
                var request = decode(payload);
                var reply = await handle(
                        services.GetRequiredService<THandler>(),
                        request,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                return encodeReply(reply);
            }
        );
    }

    /// <summary>Checks that the handler is registered, without constructing it in the root container.</summary>
    public void Validate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (!services.GetRequiredService<IServiceProviderIsService>().IsService(handlerType))
            throw new InvalidOperationException(
                $"Operation {OperationId} requires a {handlerType.Name} registration."
            );
    }

    /// <summary>Decodes the payload and calls the handler, resolved from <paramref name="services"/>.</summary>
    public Task<MessagePayload?> DispatchAsync(
        MessagePayload payload,
        IServiceProvider services,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(services);
        return dispatch(payload, services, cancellationToken);
    }
}
