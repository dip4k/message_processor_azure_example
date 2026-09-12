using System.Text.Json;

using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

// ── Type resolver ──────────────────────────────────────────────────────────────
// Two-pass approach: first peek at the discriminator with a lightweight JsonDocument
// (no full-graph allocation), then let the processor do the full typed deserialization.
// This is the only class that may read the raw JSON before a type is known.
public sealed class JsonMessageTypeResolver : IMessageTypeResolver
{
    public DataTypeEnum ResolveDataType(string messageBody)
    {
        if (string.IsNullOrWhiteSpace(messageBody))
            throw new InvalidMessageException("Message body is empty.");

        try
        {
            // ── First pass: read only the discriminator field ────────────────────
            using var document = JsonDocument.Parse(messageBody);

            if (!document.RootElement.TryGetProperty("dataType", out var dataTypeElement) ||
                dataTypeElement.ValueKind != JsonValueKind.String)
            {
                throw new InvalidMessageException("Message body does not contain a string 'dataType' property.");
            }

            var dataTypeName = dataTypeElement.GetString();

            // Unknown guards against accidentally mapping to the sentinel enum value
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