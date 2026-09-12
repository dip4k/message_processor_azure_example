namespace MessagesProcessor.Domain;

// Value Object — wraps a raw tracking number long with a zero-pad display format.
public readonly record struct TrackingNumber
{
    public long Value { get; }

    public TrackingNumber(long value)
    {
        if (value <= 0)
            throw new DomainException($"Tracking number must be positive, got {value}.");
        Value = value;
    }

    public static TrackingNumber From(long value) => new(value);

    // Zero-padded to 12 digits for carrier API compatibility
    public override string ToString() => Value.ToString("D12");
}
