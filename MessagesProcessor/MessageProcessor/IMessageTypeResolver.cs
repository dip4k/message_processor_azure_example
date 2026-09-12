using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public interface IMessageTypeResolver
{
    DataTypeEnum ResolveDataType(string messageBody);
}