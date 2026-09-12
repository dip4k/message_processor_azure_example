using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl;

public sealed class OrderConfirmationProcessor : MessageProcessorBase<OrderConfirmationData>
{
    protected override Task<ProcessedMessage<OrderConfirmationData>> ProcessCoreAsync(
        SystemMessage<OrderConfirmationData> systemMessage,
        CancellationToken cancellationToken)
    {
        var data = systemMessage.Data;
        var summary = $"Confirmation {data.ConfirmationNumber!.Value} for order {data.OrderId!.Value} is {data.ConfirmationStatus} as of {data.ConfirmationDateUtc:O}.";

        return Task.FromResult(new ProcessedMessage<OrderConfirmationData>
        {
            DataType = systemMessage.DataType,
            Data = data,
            Summary = summary,
            ProcessedAtUtc = DateTimeOffset.UtcNow
        });
    }
}
