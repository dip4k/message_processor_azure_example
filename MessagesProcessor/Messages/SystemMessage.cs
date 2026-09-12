namespace MessagesProcessor.Messages;

public class SystemMessage<T> where T : BaseData, new()
{
    public required DataTypeEnum DataType { get; set; }

    public required T Data { get; set; }
}
