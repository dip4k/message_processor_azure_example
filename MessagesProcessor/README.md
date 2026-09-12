# Messages Processor — Coding Challenge

A small, open-ended challenge. We care more about **how** you approach the problem — how you
structure the code, model the domain, and keep it testable — than about how many features you add.
Treat it like production code. If something is ambiguous, make a reasonable decision and note your
assumption.

---

## The scenario

This is an **Azure Functions (.NET 8, isolated worker)** app triggered by an **Azure Service Bus**
subscription. Upstream systems publish JSON messages onto the topic; each message is an envelope
that wraps a typed payload:

```jsonc
{
  "dataType": "OrderConfirmation",   // discriminator — which payload type this is
  "data": {                          // the payload; shape depends on dataType
    // ...fields specific to the payload type...
  }
}
```

This maps to the generic envelope `SystemMessage<T>` in
[`Messages/SystemMessage.cs`](MessagesProcessor/Messages/SystemMessage.cs), where `T` is one of the
`BaseData` subclasses in the [`Messages/`](MessagesProcessor/Messages) folder:

| `dataType`         | Payload type             |
| ------------------ | ------------------------ |
| `OrderConfirmation`| `OrderConfirmationData`  |
| `OrderDelivery`    | `OrderDeliveryData`      |
| `OrderInvoice`     | `OrderInvoiceData`       |

The payload classes are intentionally **empty** — populating them with sensible fields is part
of the task.

---

## Your task

Implement the message-processing pipeline in
[`MessageProcessorFunction.cs`](MessagesProcessor/MessageProcessorFunction.cs):

1. **Parse** the raw message body into the correct `SystemMessage<T>` based on `dataType`.
2. **Validate** the payload — reject messages that are missing required fields.
3. **Process** the message. Each type may be handled differently; try to make adding a new
   message type later easy.
4. **Forward** the result via an **HTTP `POST`**. The target URL depends on the message type —
   see `MessageProcessor:EndpointUrls` in
   [`local.settings.json`](MessagesProcessor/local.settings.json) and
   [`Configuration/MessageProcessorOptions.cs`](MessagesProcessor/Configuration/MessageProcessorOptions.cs).

Add logging and sensible error handling along the way.

### Notes

- Keep the business logic **testable** — avoid welding it directly to `ServiceBusReceivedMessage`
  or a concrete `HttpClient`; depend on abstractions you can fake/mock. You don't have to write
  tests, but the code should be easy to test.
- You don't need a real Service Bus or downstream endpoint — reason about behaviour and stub the
  downstream call if you like.
- You're free to add classes, folders, and NuGet packages, and to rename or restructure anything.

---

## Getting started

### Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)

### Build

```bash
dotnet build
```

The template builds as-is; `Run` throws `NotImplementedException` until you implement it.

### Run locally

Fill in the placeholders in [`local.settings.json`](MessagesProcessor/local.settings.json)
(`ServiceBusConnection` and the endpoint URLs), then:

```bash
cd MessagesProcessor
func start
```
