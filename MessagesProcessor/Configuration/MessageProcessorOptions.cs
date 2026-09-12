namespace MessagesProcessor.Configuration;

public class MessageProcessorOptions
{
    public const string SectionName = "MessageProcessor";

    public Dictionary<string, string> EndpointUrls { get; set; } = new();
}
