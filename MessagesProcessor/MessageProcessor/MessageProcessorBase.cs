using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;

using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

public abstract class MessageProcessorBase<T> : IProcessor<T> where T : BaseData, new()
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = CreateJsonSerializerOptions();

    public async Task<ProcessedMessage<T>> ProcessAsync(string messageBody, CancellationToken cancellationToken = default)
    {
        var systemMessage = Deserialize(messageBody);
        Validate(systemMessage);
        return await ProcessCoreAsync(systemMessage, cancellationToken);
    }

    protected abstract Task<ProcessedMessage<T>> ProcessCoreAsync(SystemMessage<T> systemMessage, CancellationToken cancellationToken);

    private static SystemMessage<T> Deserialize(string messageBody)
    {
        try
        {
            return JsonSerializer.Deserialize<SystemMessage<T>>(messageBody, JsonSerializerOptions)
                   ?? throw new InvalidMessageException($"Message body could not be deserialized into {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            throw new InvalidMessageException($"Message body is not valid JSON for {typeof(T).Name}.", ex);
        }
    }

    private static void Validate(SystemMessage<T> systemMessage)
    {
        if (systemMessage.Data is null)
        {
            throw new InvalidMessageException("Message payload is missing.");
        }

        var validationResults = new List<ValidationResult>();
        var validationContext = new ValidationContext(systemMessage.Data);

        if (!Validator.TryValidateObject(systemMessage.Data, validationContext, validationResults, validateAllProperties: true))
        {
            var errors = string.Join("; ", validationResults.Select(result => result.ErrorMessage).Where(error => !string.IsNullOrWhiteSpace(error)));
            throw new InvalidMessageException($"Message payload is invalid: {errors}");
        }
    }

    private static JsonSerializerOptions CreateJsonSerializerOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}