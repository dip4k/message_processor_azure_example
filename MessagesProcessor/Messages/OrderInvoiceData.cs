using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Messages;

public class OrderInvoiceData : BaseData
{
    [Required]
    public decimal? Amount { get; set; }

    // 3-letter ISO 4217 currency code (e.g. "INR", "USD")
    [Required, StringLength(3, MinimumLength = 3)]
    public string? Currency { get; set; }

    [Required]
    public DateTime? InvoiceDateUtc { get; set; }

    [Required]
    public long? InvoiceNumber { get; set; }

    [Required]
    public InvoiceStatusEnum? InvoiceStatus { get; set; }
}

public enum InvoiceStatusEnum
{
    Pending,
    Paid,
    Overdue
}
