using MessagesProcessor.Domain;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl;

// ── Processor: OrderConfirmation ──────────────────────────────────────────────
// Handles the confirmed state of an order. Validates confirmation details through
// the domain value objects before building the summary.
public sealed class OrderConfirmationProcessor : MessageProcessorBase<OrderConfirmationData>
{
    // ── Core business logic ───────────────────────────────────────────────
    // All field access is through domain value objects — prevents primitive obsession
    // and keeps the invariant (positive OrderId) enforced at construction.
    protected override Task<ProcessedMessage<OrderConfirmationData>> ProcessCoreAsync(
        SystemMessage<OrderConfirmationData> systemMessage,
        CancellationToken cancellationToken)
    {
        var data    = systemMessage.Data;
        var orderId = OrderId.From(data.OrderId!.Value);
        var summary = $"Confirmation #{data.ConfirmationNumber!.Value} for order {orderId} " +
                      $"is {data.ConfirmationStatus} as of {data.ConfirmationDateUtc:O}.";

        return Task.FromResult(ProcessedMessage<OrderConfirmationData>.Create(systemMessage, summary));
    }
}
