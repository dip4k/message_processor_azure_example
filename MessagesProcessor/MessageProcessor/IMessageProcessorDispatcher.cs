using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public interface IMessageProcessorDispatcher
{
    Task<object> ProcessAsync(DataTypeEnum dataType, string messageBody, CancellationToken cancellationToken = default);
}