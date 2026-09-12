using System.Net.Http.Json;

namespace MessagesProcessor.MessageProcessor;

// ── Infrastructure adapter: HTTP forwarding ─────────────────────────────────────
// Hides HttpClient behind IMessageForwarder so processors never import HttpClient.
// IHttpClientFactory manages socket pooling and DNS TTL — never use new HttpClient().
public sealed class HttpMessageForwarder : IMessageForwarder
{
    private readonly IHttpClientFactory _httpClientFactory;

    public HttpMessageForwarder(IHttpClientFactory httpClientFactory)
        => _httpClientFactory = httpClientFactory;

    public async Task ForwardAsync(string endpointUrl, object payload, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(endpointUrl))
            throw new InvalidOperationException("A downstream endpoint URL must be configured.");

        // Named client keeps HttpClientFactory's handler pooling scoped to this forwarder
        var client = _httpClientFactory.CreateClient(nameof(HttpMessageForwarder));
        using var response = await client.PostAsJsonAsync(endpointUrl, payload, cancellationToken);

        // Non-2xx: throw so Service Bus retries the message (transient downstream failure)
        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Downstream '{endpointUrl}' returned {(int)response.StatusCode} ({response.ReasonPhrase}). {responseBody}");
        }
    }
}