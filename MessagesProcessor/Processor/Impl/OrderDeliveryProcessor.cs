using MessagesProcessor.Domain;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl;

// ── Processor: OrderDelivery ──────────────────────────────────────────────────
// Handles shipment tracking updates. TrackingNumber is wrapped in a value object
// so the zero-padded display format is owned by the domain, not the summary string.
public sealed class OrderDeliveryProcessor : MessageProcessorBase<OrderDeliveryData>
{
    // ── Core business logic ───────────────────────────────────────────────
    protected override Task<ProcessedMessage<OrderDeliveryData>> ProcessCoreAsync(
        SystemMessage<OrderDeliveryData> systemMessage,
        CancellationToken cancellationToken)
    {
        var data           = systemMessage.Data;
        var orderId        = OrderId.From(data.OrderId!.Value);
        var trackingNumber = TrackingNumber.From(data.DeliveryTrackingNumber!.Value);
        var summary        = $"Delivery #{trackingNumber} for order {orderId} " +
                             $"is {data.DeliveryStatus} to {data.DeliveryAddress}.";

        return Task.FromResult(ProcessedMessage<OrderDeliveryData>.Create(systemMessage, summary));
    }
}
