using MessagesProcessor.Configuration;
using MessagesProcessor.MessageProcessor;
using MessagesProcessor.Messages;
using MessagesProcessor.Processor.Impl;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

// ── Observability ─────────────────────────────────────────────────────────────
builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

// ── Configuration ─────────────────────────────────────────────────────────────
// Strongly-typed options bound from the "MessageProcessor" section in local.settings.json.
// ValidateDataAnnotations + ValidateOnStart cause the host to fail at boot (not on first message)
// when required endpoint URLs are missing — fail fast, not silently at runtime.
builder.Services
    .AddOptions<MessageProcessorOptions>()
    .BindConfiguration(MessageProcessorOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();

// ── Pipeline infrastructure ───────────────────────────────────────────────────
builder.Services.AddSingleton<IMessageTypeResolver, JsonMessageTypeResolver>();
builder.Services.AddSingleton<IMessageProcessorDispatcher, MessageProcessorDispatcher>();
builder.Services.AddSingleton<IMessageForwarder, HttpMessageForwarder>();

// Named client keeps socket pool scoped to the forwarder; avoids new HttpClient() anti-pattern
builder.Services.AddHttpClient(nameof(HttpMessageForwarder));

// ── Processors (one per message type) ────────────────────────────────────────
// To add a new type: register IProcessor<NewData> here + add a case to MessageProcessorDispatcher.
builder.Services.AddSingleton<IProcessor<OrderConfirmationData>, OrderConfirmationProcessor>();
builder.Services.AddSingleton<IProcessor<OrderDeliveryData>, OrderDeliveryProcessor>();
builder.Services.AddSingleton<IProcessor<OrderInvoiceData>, OrderInvoiceProcessor>();

builder.Build().Run();
