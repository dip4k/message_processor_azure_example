using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Messages;

public class OrderDeliveryData : BaseData
{
    [Required]
    public DeliveryStatusEnum? DeliveryStatus { get; set; }

    [Required]
    public DateTime? DeliveryDateUtc { get; set; }

    [Required, MinLength(1)]
    public string? DeliveryAddress { get; set; }

    [Required]
    public long? DeliveryTrackingNumber { get; set; }

}

public enum DeliveryStatusEnum
{
    Pending,
    Shipped,
    Delivered,
    Returned
}
