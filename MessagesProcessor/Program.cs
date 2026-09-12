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

builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

builder.Services
    .AddOptions<MessageProcessorOptions>()
    .BindConfiguration(MessageProcessorOptions.SectionName);

builder.Services.AddSingleton<IMessageTypeResolver, JsonMessageTypeResolver>();
builder.Services.AddSingleton<IMessageProcessorDispatcher, MessageProcessorDispatcher>();
builder.Services.AddSingleton<IMessageForwarder, HttpMessageForwarder>();
builder.Services.AddSingleton<IProcessor<OrderConfirmationData>, OrderConfirmationProcessor>();
builder.Services.AddSingleton<IProcessor<OrderDeliveryData>, OrderDeliveryProcessor>();
builder.Services.AddSingleton<IProcessor<OrderInvoiceData>, OrderInvoiceProcessor>();
builder.Services.AddHttpClient(nameof(HttpMessageForwarder));

builder.Build().Run();
