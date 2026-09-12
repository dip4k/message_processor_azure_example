namespace MessagesProcessor.MessageProcessor;

// ── Exception hierarchy ──────────────────────────────────────────────────────────────
// MessageProcessingException  → base; catch this to handle all pipeline failures
//   InvalidMessageException    → bad payload — dead-letter immediately, never retry
//
// HttpRequestException (built-in) → downstream failure — let Service Bus retry
// InvalidOperationException (built-in) → config error — alert ops, do not retry

// Base for all pipeline-controlled failures; extends Exception so callers can
// catch at different granularities (specific vs general).
public class MessageProcessingException : Exception
{
    public MessageProcessingException(string message) : base(message) { }
    public MessageProcessingException(string message, Exception innerException) : base(message, innerException) { }
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