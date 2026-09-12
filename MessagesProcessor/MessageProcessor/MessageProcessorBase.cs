using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;

using MessagesProcessor.Messages;

namespace MessagesProcessor.MessageProcessor;

// ── Template Method base ────────────────────────────────────────────────────
// Fixed algorithm: Deserialize → Validate → ProcessCoreAsync (abstract).
// Subclasses only override step 3; parse/validate invariants are never duplicated.
public abstract class MessageProcessorBase<T> : IProcessor<T> where T : BaseData, new()
{
    // Shared options instance — JsonSerializerOptions is thread-safe once built
    private static readonly JsonSerializerOptions SerializerOptions = BuildSerializerOptions();

    // ── Entry point (fixed algorithm) ─────────────────────────────────────
    public async Task<ProcessedMessage<T>> ProcessAsync(string messageBody, CancellationToken cancellationToken = default)
    {
        var systemMessage = Deserialize(messageBody);
        Validate(systemMessage);
        return await ProcessCoreAsync(systemMessage, cancellationToken);
    }

    // ── Extension point ─────────────────────────────────────────────────────
    // Subclasses implement only the type-specific business logic.
    protected abstract Task<ProcessedMessage<T>> ProcessCoreAsync(
        SystemMessage<T> systemMessage, CancellationToken cancellationToken);

    // ── Step 1: Deserialize ─────────────────────────────────────────────────
    // JsonException is caught and re-thrown as InvalidMessageException so the
    // caller sees one exception type for all "bad message" scenarios.
    private static SystemMessage<T> Deserialize(string messageBody)
    {
        try
        {
            return JsonSerializer.Deserialize<SystemMessage<T>>(messageBody, SerializerOptions)
                   ?? throw new InvalidMessageException($"Deserialization returned null for {typeof(T).Name}.");
        }
        catch (JsonException ex)
        {
            throw new InvalidMessageException($"Message body is not valid JSON for {typeof(T).Name}.", ex);
        }
    }

    // ── Step 2: Validate ───────────────────────────────────────────────────
    // DataAnnotations on each payload class declare the rules declaratively.
    // This keeps validation co-located with the data shape, not scattered in processors.
    private static void Validate(SystemMessage<T> systemMessage)
    {
        if (systemMessage.Data is null)
            throw new InvalidMessageException("Message payload is missing.");

        var results = new List<ValidationResult>();
        var context = new ValidationContext(systemMessage.Data);

        if (!Validator.TryValidateObject(systemMessage.Data, context, results, validateAllProperties: true))
        {
            var errors = string.Join("; ", results
                .Select(r => r.ErrorMessage)
                .Where(e => !string.IsNullOrWhiteSpace(e)));
            throw new InvalidMessageException($"Message payload is invalid: {errors}");
        }
    }

    // Case-insensitive + enum-from-string — tolerates both "OrderConfirmation" and "orderconfirmation"
    private static JsonSerializerOptions BuildSerializerOptions()
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}