using System.Text.Json;

using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public sealed class JsonMessageTypeResolver : IMessageTypeResolver
{
    public DataTypeEnum ResolveDataType(string messageBody)
    {
        if (string.IsNullOrWhiteSpace(messageBody))
        {
            throw new InvalidMessageException("Message body is empty.");
        }

        try
        {
            using var document = JsonDocument.Parse(messageBody);

            if (!document.RootElement.TryGetProperty("dataType", out var dataTypeElement) ||
                dataTypeElement.ValueKind != JsonValueKind.String)
            {
                throw new InvalidMessageException("Message body does not contain a string dataType property.");
            }

            var dataTypeName = dataTypeElement.GetString();

            if (!Enum.TryParse<DataTypeEnum>(dataTypeName, ignoreCase: true, out var dataType) ||
                dataType == DataTypeEnum.Unknown)
            {
                throw new InvalidMessageException($"Unsupported dataType '{dataTypeName}'.");
            }

            return dataType;
        }
        catch (JsonException ex)
        {
            throw new InvalidMessageException("Message body is not valid JSON.", ex);
        }
    }
}