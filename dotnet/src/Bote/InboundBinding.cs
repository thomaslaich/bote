using Microsoft.Extensions.DependencyInjection;

namespace Bote;

/// <summary>
/// How one kind of message comes off the wire: its address, its decoder, and the handler it goes
/// to. Generated code creates one per command and per channel; a consumer holds them by address
/// and dispatches each delivery. Successful completion permits settlement.
/// </summary>
public sealed class InboundBinding
{
    /// <summary>The header, or Redis Streams entry field, that names a command's operation.</summary>
    public const string TypeHeader = "bote-type";

    private readonly IReadOnlyList<Type> handlerTypes;
    private readonly Func<MessagePayload, InboundBinding> resolve;
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
        IReadOnlyList<Type> handlerTypes,
        bool hasReply,
        Func<MessagePayload, IServiceProvider, CancellationToken, Task<MessagePayload?>> dispatch,
        Func<MessagePayload, InboundBinding>? resolve = null
    )
    {
        ServiceId = serviceId;
        OperationId = operationId;
        Address = address;
        HasReply = hasReply;
        this.handlerTypes = handlerTypes;
        this.dispatch = dispatch;
        this.resolve = resolve ?? (_ => this);
    }

    public string ServiceId { get; }
    public string OperationId { get; }
    public string Address { get; }

    /// <summary>
    /// Whether the handler answers, and the transport publishes the reply before settlement. For
    /// commands sharing an address, whether any of them answers; <see cref="Resolve"/> tells.
    /// </summary>
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
            [typeof(THandler)],
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
            [typeof(THandler)],
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

    /// <summary>
    /// The commands that share one address, by operation name. Each delivery goes to the command
    /// its <see cref="TypeHeader"/> names, or to the only command when it names none.
    /// </summary>
    public static InboundBinding Commands(IReadOnlyDictionary<string, InboundBinding> byType)
    {
        ArgumentNullException.ThrowIfNull(byType);
        if (byType.Count == 0)
            throw new ArgumentException("At least one command is required.", nameof(byType));
        var bindings = byType.Values.ToArray();
        var first = bindings[0];
        if (bindings.Any(b => b.Address != first.Address))
            throw new ArgumentException("Commands must share one address.", nameof(byType));
        InboundBinding Resolve(MessagePayload payload)
        {
            if (
                payload.Headers is { } headers
                && headers.TryGetValue(TypeHeader, out var typeBytes)
            )
            {
                var type = System.Text.Encoding.UTF8.GetString(typeBytes);
                return byType.TryGetValue(type, out var binding)
                    ? binding
                    : throw new UnknownMessageTypeException(first.Address, type);
            }
            return bindings.Length == 1
                ? bindings[0]
                : throw new UnknownMessageTypeException(first.Address, type: null);
        }
        return new(
            first.ServiceId,
            string.Join(", ", bindings.Select(b => b.OperationId)),
            first.Address,
            [.. bindings.SelectMany(b => b.handlerTypes)],
            bindings.Any(b => b.HasReply),
            (payload, services, ct) => Resolve(payload).DispatchAsync(payload, services, ct),
            Resolve
        );
    }

    /// <summary>The binding that handles <paramref name="payload"/>: itself, or one of the commands it groups.</summary>
    public InboundBinding Resolve(MessagePayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return resolve(payload);
    }

    /// <summary>Checks that the handlers are registered, without constructing them in the root container.</summary>
    public void Validate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var isService = services.GetRequiredService<IServiceProviderIsService>();
        foreach (var handlerType in handlerTypes)
            if (!isService.IsService(handlerType))
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
