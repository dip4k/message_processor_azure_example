using Azure.Messaging.ServiceBus;

using MessagesProcessor.Configuration;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessagesProcessor;

// ── Entry point — thin Azure Function trigger ────────────────────────────────
// Responsibility: receive raw bytes, orchestrate the pipeline, handle
// infrastructure-level errors (dead-letter vs bubble-up for retry).
// It must NOT contain business logic — all rules live in the domain layer.
//
// Pipeline flow:
//   1. ResolveDataType   → peek at discriminator (no full parse yet)
//   2. GetEndpointUrl    → config lookup; throws on missing key (ops alert)
//   3. ProcessAsync      → typed deserialize + validate + process (dispatcher)
//   4. ForwardAsync      → HTTP POST to downstream endpoint
//
// Error routing:
//   InvalidMessageException      → dead-letter (bad message, never retry)
//   HttpRequestException         → bubble up → Service Bus retries
//   InvalidOperationException    → bubble up → alert ops (config bug)
public class MessageProcessorFunction
{
    private readonly ILogger<MessageProcessorFunction> _logger;
    private readonly IMessageTypeResolver _typeResolver;
    private readonly IMessageProcessorDispatcher _dispatcher;
    private readonly IMessageForwarder _forwarder;
    private readonly IOptions<MessageProcessorOptions> _options;

    public MessageProcessorFunction(
        ILogger<MessageProcessorFunction> logger,
        IMessageTypeResolver typeResolver,
        IMessageProcessorDispatcher dispatcher,
        IMessageForwarder forwarder,
        IOptions<MessageProcessorOptions> options)
    {
        _logger      = logger;
        _typeResolver = typeResolver;
        _dispatcher  = dispatcher;
        _forwarder   = forwarder;
        _options     = options;
    }

    [Function(nameof(MessageProcessorFunction))]
    public async Task Run(
        [ServiceBusTrigger("mytopic", "mysubscription", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Received message {MessageId} (ContentType: {ContentType})",
            message.MessageId, message.ContentType);

        var payload = message.Body.ToString();

        try
        {
            // ── Step 1: Resolve type from discriminator ───────────────────
            var dataType = _typeResolver.ResolveDataType(payload);
            _logger.LogInformation("Resolved {DataType} for message {MessageId}", dataType, message.MessageId);

            // ── Step 2: Look up the downstream endpoint ───────────────────
            var endpointUrl = GetEndpointUrl(dataType);

            // ── Step 3: Deserialize, validate, process ────────────────────
            var result = await _dispatcher.ProcessAsync(dataType, payload, cancellationToken);

            // ── Step 4: Forward to downstream system ──────────────────────
            _logger.LogInformation("Forwarding {DataType} {MessageId} → {EndpointUrl}",
                dataType, message.MessageId, endpointUrl);

            await _forwarder.ForwardAsync(endpointUrl, result, cancellationToken);

            _logger.LogInformation("Message {MessageId} processed and forwarded successfully.", message.MessageId);
        }
        catch (InvalidMessageException ex)
        {
            // Structural problem — retrying will never help; dead-letter immediately
            _logger.LogWarning(ex, "Dead-lettering invalid message {MessageId}.", message.MessageId);
            await messageActions.DeadLetterMessageAsync(
                message,
                deadLetterReason: "Invalid message",
                deadLetterErrorDescription: ex.Message,
                cancellationToken: cancellationToken);
        }
        // All other exceptions bubble up → Service Bus applies retry + eventual DLQ
    }

    // ── Config helper ─────────────────────────────────────────────────────────
    // Throws InvalidOperationException (not InvalidMessageException) because a
    // missing URL is a deployment/config bug, not a bad message.
    private string GetEndpointUrl(DataTypeEnum dataType)
    {
        var key = dataType.ToString();

        if (_options.Value.EndpointUrls.TryGetValue(key, out var url) && !string.IsNullOrWhiteSpace(url))
            return url;

        throw new InvalidOperationException($"No endpoint URL configured for '{key}'.");
    }
}
