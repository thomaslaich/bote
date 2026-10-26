using Microsoft.Extensions.DependencyInjection;

namespace Bote.Tests;

public sealed class InboundBindingTests
{
    [Fact]
    public async Task CommandsDispatchOnTheTypeHeader()
    {
        var (commands, calls) = Commands();
        await using var services = Services(calls);

        await commands.DispatchAsync(Payload("Cancel"), services, CancellationToken.None);

        Assert.Equal(["cancel"], calls);
    }

    [Fact]
    public async Task OnlyCommandHandlesMessagesWithoutTheTypeHeader()
    {
        var calls = new List<string>();
        var commands = InboundBinding.Commands(
            new Dictionary<string, InboundBinding> { ["Submit"] = Binding<Submit>("submit") }
        );
        await using var services = Services(calls);

        await commands.DispatchAsync(new MessagePayload([]), services, CancellationToken.None);

        Assert.Equal(["submit"], calls);
    }

    [Fact]
    public async Task UnknownOrMissingTypeFailsBeforeAnyHandler()
    {
        var (commands, calls) = Commands();
        await using var services = Services(calls);

        await Assert.ThrowsAsync<UnknownMessageTypeException>(() =>
            commands.DispatchAsync(Payload("Archive"), services, CancellationToken.None)
        );
        await Assert.ThrowsAsync<UnknownMessageTypeException>(() =>
            commands.DispatchAsync(new MessagePayload([]), services, CancellationToken.None)
        );
        Assert.Empty(calls);
    }

    [Fact]
    public void ResolveReturnsTheNamedCommand()
    {
        var (commands, _) = Commands();

        Assert.Equal("test#Cancel", commands.Resolve(Payload("Cancel")).OperationId);
    }

    [Fact]
    public async Task ValidateRequiresEveryCommandsHandler()
    {
        var (commands, calls) = Commands();
        await using var services = new ServiceCollection()
            .AddSingleton(new Handler<Submit>(calls))
            .BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => commands.Validate(services));
        Assert.Contains("Handler`1", exception.Message, StringComparison.Ordinal);
    }

    private static (InboundBinding Commands, List<string> Calls) Commands() =>
        (
            InboundBinding.Commands(
                new Dictionary<string, InboundBinding>
                {
                    ["Submit"] = Binding<Submit>("submit"),
                    ["Cancel"] = Binding<Cancel>("cancel"),
                }
            ),
            []
        );

    private static InboundBinding Binding<T>(string name) =>
        InboundBinding.Create<string, Handler<T>>(
            "test#Service",
            "test#" + typeof(T).Name,
            "commands",
            _ => name,
            (handler, value, ct) => handler.HandleAsync(value)
        );

    private static MessagePayload Payload(string type) =>
        new(
            [],
            Headers: new Dictionary<string, byte[]>
            {
                [InboundBinding.TypeHeader] = System.Text.Encoding.UTF8.GetBytes(type),
            }
        );

    private static ServiceProvider Services(List<string> calls) =>
        new ServiceCollection()
            .AddSingleton(new Handler<Submit>(calls))
            .AddSingleton(new Handler<Cancel>(calls))
            .BuildServiceProvider();

    private sealed class Submit;

    private sealed class Cancel;

    private sealed class Handler<T>(List<string> calls)
    {
        public Task HandleAsync(string value)
        {
            calls.Add(value);
            return Task.CompletedTask;
        }
    }
}
