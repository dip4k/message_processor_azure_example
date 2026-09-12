namespace MessagesProcessor.Domain;

// Value Object — amount + currency are always kept together; currency is normalised to upper-case.
public readonly record struct Money
{
    public decimal Amount { get; }
    public string Currency { get; }

    public Money(decimal amount, string currency)
    {
        if (amount < 0)
            throw new DomainException($"Amount cannot be negative: {amount}.");
        if (string.IsNullOrWhiteSpace(currency) || currency.Length != 3)
            throw new DomainException($"Currency must be a 3-letter ISO code, got '{currency}'.");

        Amount   = amount;
        Currency = currency.ToUpperInvariant();
    }

    public static Money From(decimal amount, string currency) => new(amount, currency);

    public override string ToString() => $"{Amount:F2} {Currency}";
}
