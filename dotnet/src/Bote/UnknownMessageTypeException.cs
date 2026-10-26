namespace Bote;

/// <summary>A delivery names a command its consumer does not know, or none where several share the address.</summary>
public sealed class UnknownMessageTypeException : Exception
{
    public UnknownMessageTypeException(string address, string? type)
        : base(
            type is null
                ? $"A message on '{address}' names no command, but several commands share the address."
                : $"A message on '{address}' names the unknown command '{type}'."
        )
    {
        Address = address;
        Type = type;
    }

    public UnknownMessageTypeException() { }

    public UnknownMessageTypeException(string message)
        : base(message) { }

    public UnknownMessageTypeException(string message, Exception innerException)
        : base(message, innerException) { }

    public string? Address { get; }
    public string? Type { get; }
}
