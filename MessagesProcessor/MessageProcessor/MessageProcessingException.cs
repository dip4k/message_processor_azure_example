namespace MessagesProcessor.MessageProcessor;

public class MessageProcessingException : Exception
{
    public MessageProcessingException(string message)
        : base(message)
    {
    }

    public MessageProcessingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

public sealed class InvalidMessageException : MessageProcessingException
{
    public InvalidMessageException(string message)
        : base(message)
    {
    }

    public InvalidMessageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}