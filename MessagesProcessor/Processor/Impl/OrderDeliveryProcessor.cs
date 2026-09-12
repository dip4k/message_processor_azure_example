using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

namespace MessagesProcessor.Processor.Impl
{
    public sealed class OrderDeliveryProcessor : MessageProcessorBase<OrderDeliveryData>
    {
        protected override Task<ProcessedMessage<OrderDeliveryData>> ProcessCoreAsync(
            SystemMessage<OrderDeliveryData> systemMessage,
            CancellationToken cancellationToken)
        {
            var data = systemMessage.Data;
            var summary = $"Delivery {data.DeliveryTrackingNumber!.Value} for order {data.OrderId!.Value} is {data.DeliveryStatus} to {data.DeliveryAddress}.";

            return Task.FromResult(new ProcessedMessage<OrderDeliveryData>
            {
                DataType = systemMessage.DataType,
                Data = data,
                Summary = summary,
                ProcessedAtUtc = DateTimeOffset.UtcNow
            });
        }
    }
}
