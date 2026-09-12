using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl;

public sealed class OrderInvoiceProcessor : MessageProcessorBase<OrderInvoiceData>
{
    protected override Task<ProcessedMessage<OrderInvoiceData>> ProcessCoreAsync(
        SystemMessage<OrderInvoiceData> systemMessage,
        CancellationToken cancellationToken)
    {
        var data = systemMessage.Data;
        var summary = $"Invoice {data.InvoiceNumber!.Value} for order {data.OrderId!.Value} is {data.InvoiceStatus} with amount {data.Amount!.Value:0.00}.";

        return Task.FromResult(new ProcessedMessage<OrderInvoiceData>
        {
            DataType = systemMessage.DataType,
            Data = data,
            Summary = summary,
            ProcessedAtUtc = DateTimeOffset.UtcNow
        });
    }
}
