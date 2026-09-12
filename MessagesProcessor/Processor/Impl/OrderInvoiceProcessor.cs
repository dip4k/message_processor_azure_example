using MessagesProcessor.Domain;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl;

// ── Processor: OrderInvoice ───────────────────────────────────────────────────
// Handles invoice events. Money is wrapped in a value object so currency and
// formatting rules stay in the domain layer, not in format strings.
public sealed class OrderInvoiceProcessor : MessageProcessorBase<OrderInvoiceData>
{
    // ── Core business logic ───────────────────────────────────────────────
    protected override Task<ProcessedMessage<OrderInvoiceData>> ProcessCoreAsync(
        SystemMessage<OrderInvoiceData> systemMessage,
        CancellationToken cancellationToken)
    {
        var data    = systemMessage.Data;
        var orderId = OrderId.From(data.OrderId!.Value);
        var amount  = Money.From(data.Amount!.Value, data.Currency!);
        var summary = $"Invoice #{data.InvoiceNumber!.Value} for order {orderId} " +
                      $"is {data.InvoiceStatus} — {amount}.";

        return Task.FromResult(ProcessedMessage<OrderInvoiceData>.Create(systemMessage, summary));
    }
}
