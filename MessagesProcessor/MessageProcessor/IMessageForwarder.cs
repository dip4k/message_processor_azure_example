namespace MessagesProcessor.MessageProcessor;

public interface IMessageForwarder
{
    Task ForwardAsync(string endpointUrl, object payload, CancellationToken cancellationToken = default);
}