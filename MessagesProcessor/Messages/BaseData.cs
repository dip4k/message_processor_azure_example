using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Messages;

public abstract class BaseData
{
    [Required]
    public long? OrderId { get; set; }

    [Required]
    public DateTime? CreateAtUtc { get; set; }

    [Required, MinLength(1)]
    public string? CreatedBy { get; set; }

    [Required]
    public Guid? MessageCorelationId { get; set; }
}
