using System.ComponentModel.DataAnnotations;

namespace MessagesProcessor.Configuration;

public class MessageProcessorOptions
{
    public const string SectionName = "MessageProcessor";

    // At least one endpoint must be configured; catches misconfigured deployments at startup
    [Required, MinLength(1)]
    public Dictionary<string, string> EndpointUrls { get; set; } = new();
}
