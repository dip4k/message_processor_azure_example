using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public interface IProcessor<T> where T : BaseData, new()
{
    Task<ProcessedMessage<T>> ProcessAsync(string messageBody, CancellationToken cancellationToken = default);
}
