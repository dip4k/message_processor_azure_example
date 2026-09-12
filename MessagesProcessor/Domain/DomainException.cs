namespace MessagesProcessor.Domain;

// Signals a violated domain invariant — not a bug, not an infra failure.
// Callers map this to a dead-letter decision, not a retry.
public sealed class DomainException : Exception
{
    public DomainException(string message) : base(message) { }
    public DomainException(string message, Exception inner) : base(message, inner) { }
}
