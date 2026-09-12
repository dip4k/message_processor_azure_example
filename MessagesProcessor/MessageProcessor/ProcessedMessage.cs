using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public sealed record ProcessedMessage<T> where T : BaseData
{
    public required DataTypeEnum DataType { get; init; }

    public required T Data { get; init; }

    public required string Summary { get; init; }

    public required DateTimeOffset ProcessedAtUtc { get; init; }
}