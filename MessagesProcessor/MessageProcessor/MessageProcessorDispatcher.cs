using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public sealed class MessageProcessorDispatcher : IMessageProcessorDispatcher
{
    private readonly IProcessor<OrderConfirmationData> _orderConfirmationProcessor;
    private readonly IProcessor<OrderDeliveryData> _orderDeliveryProcessor;
    private readonly IProcessor<OrderInvoiceData> _orderInvoiceProcessor;

    public MessageProcessorDispatcher(
        IProcessor<OrderConfirmationData> orderConfirmationProcessor,
        IProcessor<OrderDeliveryData> orderDeliveryProcessor,
        IProcessor<OrderInvoiceData> orderInvoiceProcessor)
    {
        _orderConfirmationProcessor = orderConfirmationProcessor;
        _orderDeliveryProcessor = orderDeliveryProcessor;
        _orderInvoiceProcessor = orderInvoiceProcessor;
    }

    public Task<object> ProcessAsync(DataTypeEnum dataType, string messageBody, CancellationToken cancellationToken = default)
    {
        return dataType switch
        {
            DataTypeEnum.OrderConfirmation => _orderConfirmationProcessor.ProcessAsync(messageBody, cancellationToken).ContinueWith(task => (object)task.Result, cancellationToken),
            DataTypeEnum.OrderDelivery => _orderDeliveryProcessor.ProcessAsync(messageBody, cancellationToken).ContinueWith(task => (object)task.Result, cancellationToken),
            DataTypeEnum.OrderInvoice => _orderInvoiceProcessor.ProcessAsync(messageBody, cancellationToken).ContinueWith(task => (object)task.Result, cancellationToken),
            _ => throw new InvalidMessageException($"Unsupported data type '{dataType}'.")
        };
    }
}