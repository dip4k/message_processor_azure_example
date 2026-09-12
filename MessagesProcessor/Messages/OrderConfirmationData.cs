using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Messages;

public class OrderConfirmationData : BaseData
{
    [Required]
    public long? ConfirmationNumber { get; set; }

    [Required]
    public DateTime? ConfirmationDateUtc { get; set; }

    [Required]
    public ConfirmationStatusEnum? ConfirmationStatus { get; set; }
}

public enum ConfirmationStatusEnum
{
    Pending,
    Confirmed,
    Rejected
}
