using System.Net.Http.Json;

namespace MessagesProcessor.MessageProcessor;

public sealed class HttpMessageForwarder : IMessageForwarder
{
    private readonly IHttpClientFactory _httpClientFactory;

    public HttpMessageForwarder(IHttpClientFactory httpClientFactory)
    {
        _httpClientFactory = httpClientFactory;
    }

    public async Task ForwardAsync(string endpointUrl, object payload, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(endpointUrl))
        {
            throw new InvalidOperationException("A downstream endpoint URL must be configured.");
        }

        var client = _httpClientFactory.CreateClient(nameof(HttpMessageForwarder));
        using var response = await client.PostAsJsonAsync(endpointUrl, payload, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Downstream endpoint '{endpointUrl}' returned {(int)response.StatusCode} ({response.ReasonPhrase}). {responseBody}");
        }
    }
}