using Azure.Messaging.ServiceBus;

using MessagesProcessor.Configuration;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;

namespace MessagesProcessor;

public class MessageProcessorFunction
{
    private readonly ILogger<MessageProcessorFunction> _logger;
    private readonly IMessageTypeResolver _messageTypeResolver;
    private readonly IMessageProcessorDispatcher _messageProcessorDispatcher;
    private readonly IMessageForwarder _messageForwarder;
    private readonly IOptions<MessageProcessorOptions> _options;

    public MessageProcessorFunction(
        ILogger<MessageProcessorFunction> logger,
        IMessageTypeResolver messageTypeResolver,
        IMessageProcessorDispatcher messageProcessorDispatcher,
        IMessageForwarder messageForwarder,
        IOptions<MessageProcessorOptions> options)
    {
        _logger = logger;
        _messageTypeResolver = messageTypeResolver;
        _messageProcessorDispatcher = messageProcessorDispatcher;
        _messageForwarder = messageForwarder;
        _options = options;
    }

    [Function(nameof(MessageProcessorFunction))]
    public async Task Run(
        [ServiceBusTrigger("mytopic", "mysubscription", Connection = "ServiceBusConnection")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Message ID: {MessageId}", message.MessageId);
        _logger.LogInformation("Message Content-Type: {ContentType}", message.ContentType);

        var payloadString = message.Body.ToString();
        _logger.LogDebug("Received message body: {Body}", payloadString);

        try
        {
            var dataType = _messageTypeResolver.ResolveDataType(payloadString);
            _logger.LogInformation("Resolved data type {DataType} for message {MessageId}", dataType, message.MessageId);

            var endpointUrl = GetEndpointUrl(dataType);
            var processedMessage = await _messageProcessorDispatcher.ProcessAsync(dataType, payloadString, cancellationToken);

            _logger.LogInformation(
                "Forwarding processed {DataType} message {MessageId} to {EndpointUrl}",
                dataType,
                message.MessageId,
                endpointUrl);

            await _messageForwarder.ForwardAsync(endpointUrl, processedMessage, cancellationToken);

            _logger.LogInformation("Message {MessageId} processed and forwarded successfully.", message.MessageId);
        }
        catch (InvalidMessageException ex)
        {
            _logger.LogWarning(ex, "Rejecting invalid message {MessageId}.", message.MessageId);
            await messageActions.DeadLetterMessageAsync(
                message,
                deadLetterReason: "Invalid message",
                deadLetterErrorDescription: ex.Message,
                cancellationToken: cancellationToken);
        }
    }

    private string GetEndpointUrl(DataTypeEnum dataType)
    {
        var endpointUrls = _options.Value.EndpointUrls;
        var endpointKey = dataType.ToString();

        if (endpointUrls.TryGetValue(endpointKey, out var endpointUrl) && !string.IsNullOrWhiteSpace(endpointUrl))
        {
            return endpointUrl;
        }

        throw new InvalidOperationException($"No endpoint URL configured for '{endpointKey}'.");
    }
}
