namespace MessagesProcessor.Domain;

// Value Object — identity by value, self-validating, no invalid instance possible.
public readonly record struct OrderId
{
    public long Value { get; }

    public OrderId(long value)
    {
        if (value <= 0)
            throw new DomainException($"OrderId must be a positive integer, got {value}.");
        Value = value;
    }

    // Named factory for readability at call sites: OrderId.From(data.OrderId!.Value)
    public static OrderId From(long value) => new(value);

    // Transparent to callers that still expect a plain long
    public static implicit operator long(OrderId id) => id.Value;

    public override string ToString() => Value.ToString();
}
