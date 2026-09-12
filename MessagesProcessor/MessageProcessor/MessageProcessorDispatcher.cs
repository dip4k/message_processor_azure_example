using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

// ── Dispatcher ────────────────────────────────────────────────────────────────
// Routes a DataTypeEnum to the correct IProcessor<T> and returns the result as
// object so the caller (MessageProcessorFunction) stays type-agnostic.
// Adding a new message type = one new case here + one new DI registration.
public sealed class MessageProcessorDispatcher : IMessageProcessorDispatcher
{
    private readonly IProcessor<OrderConfirmationData> _confirmationProcessor;
    private readonly IProcessor<OrderDeliveryData> _deliveryProcessor;
    private readonly IProcessor<OrderInvoiceData> _invoiceProcessor;

    public MessageProcessorDispatcher(
        IProcessor<OrderConfirmationData> confirmationProcessor,
        IProcessor<OrderDeliveryData> deliveryProcessor,
        IProcessor<OrderInvoiceData> invoiceProcessor)
    {
        _confirmationProcessor = confirmationProcessor;
        _deliveryProcessor     = deliveryProcessor;
        _invoiceProcessor      = invoiceProcessor;
    }

    // ── Routing ───────────────────────────────────────────────────────────
    // Each arm awaits the typed processor and boxes the result.
    // Using async helpers avoids ContinueWith, which swallows exceptions on
    // faulted tasks when the continuation runs on a different scheduler.
    public Task<object> ProcessAsync(DataTypeEnum dataType, string messageBody, CancellationToken cancellationToken = default)
        => dataType switch
        {
            DataTypeEnum.OrderConfirmation => BoxAsync(_confirmationProcessor.ProcessAsync(messageBody, cancellationToken)),
            DataTypeEnum.OrderDelivery     => BoxAsync(_deliveryProcessor.ProcessAsync(messageBody, cancellationToken)),
            DataTypeEnum.OrderInvoice      => BoxAsync(_invoiceProcessor.ProcessAsync(messageBody, cancellationToken)),
            _                              => throw new InvalidMessageException($"Unsupported data type '{dataType}'.")
        };

    // ── Boxing helper ─────────────────────────────────────────────────────
    // Awaits any Task<T> and returns the result as object.
    // Exceptions propagate naturally through the await — no swallowing.
    private static async Task<object> BoxAsync<T>(Task<T> task) => (await task)!;
}