using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Messages;

public class OrderInvoiceData : BaseData
{
    [Required]
    public decimal? Amount { get; set; }

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
