using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

// ── Output envelope ───────────────────────────────────────────────────────────
// Carries the original typed payload plus a human-readable summary and a
// processing timestamp. Forwarded as-is to the downstream HTTP endpoint.
public sealed record ProcessedMessage<T> where T : BaseData, new()
{
    public required DataTypeEnum DataType { get; init; }
    public required T Data { get; init; }
    public required string Summary { get; init; }
    public required DateTimeOffset ProcessedAtUtc { get; init; }

    // ── Factory ───────────────────────────────────────────────────────────
    // Centralises object creation so processors only supply the summary string.
    // ProcessedAtUtc is always stamped here to guarantee UTC consistency.
    public static ProcessedMessage<T> Create(SystemMessage<T> message, string summary)
        => new()
        {
            DataType       = message.DataType,
            Data           = message.Data,
            Summary        = summary,
            ProcessedAtUtc = DateTimeOffset.UtcNow,
        };
}