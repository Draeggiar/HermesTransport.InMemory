# HermesTransport.InMemory [![Build status](https://github.com/Draeggiar/HermesTransport.InMemory/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/Draeggiar/HermesTransport.InMemory/actions)

An in-memory message broker implementation for the HermesTransport framework, providing fast, reliable messaging for testing and lightweight applications.

[![HermesTransport](https://raw.githubusercontent.com/Draeggiar/HermesTransport/master/assets/icon.png)](https://github.com/Draeggiar/HermesTransport.InMemory)

## 🚀 Features

- **In-Memory Message Broker**: Fast, thread-safe message processing using `System.Threading.Channels`
- **HermesTransport Integration**: Implements all HermesTransport abstractions (`IMessageBroker`, `IMessagePublisher`, `IMessageSubscriber`, etc.)
- **Flexible Dispatch Modes**: 
  - **Synchronous**: Sequential message processing (MaxConcurrency = 1)
  - **Asynchronous**: Parallel message processing with configurable concurrency
- **Fan-out Delivery**: Multiple subscribers receive the same message
- **Message Types**: Support for `IMessage`, `IEvent`, and `ICommand`
- **Lifecycle Management**: Proper subscription start/stop with cancellation support
- **Thread-Safe**: Concurrent publishers and subscribers without data races

## 📦 Installation

```bash
# Add the HermesTransport.InMemory package to your project
dotnet add package HermesTransport.InMemory

# Add the core HermesTransport package (if not already referenced)
dotnet add package HermesTransport
```

## 🎯 Quick Start

### Basic Setup

```csharp
using HermesTransport;
using HermesTransport.InMemory;

// Create and connect the broker
var broker = new InMemoryMessageBroker();
await broker.ConnectAsync();

// Get publisher and subscriber
var publisher = broker.GetPublisher();
var subscriber = broker.GetSubscriber();
```

### Define Messages

```csharp
public class OrderCreated : IEvent
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    
    // Required IEvent properties
    public string Source { get; set; } = "order-service";
    public string Version { get; set; } = "1.0";
    
    // Required IMessage properties
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(OrderCreated);
    public string CorrelationId { get; set; } = string.Empty;
}
```

### Create Message Handlers

```csharp
public class OrderEventHandler : IEventHandler<OrderCreated>
{
    public async Task HandleAsync(OrderCreated orderEvent, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"Processing order: {orderEvent.OrderId}");
        // Your business logic here
        await Task.CompletedTask;
    }
}
```

### Subscribe and Publish

```csharp
// Create handler and subscription
var handler = new OrderEventHandler();
var subscription = subscriber.Subscribe(handler);
await subscription.StartAsync();

// Publish an event
var orderEvent = new OrderCreated 
{ 
    OrderId = "ORD-001", 
    Amount = 99.99m 
};

await publisher.PublishAsync(orderEvent);

// Cleanup
await subscription.StopAsync();
await broker.DisconnectAsync();
```

## ⚙️ Configuration

### Synchronous Dispatch (Sequential Processing)

```csharp
var options = new SubscriptionOptions().WithSynchronousDispatch();
var subscription = subscriber.Subscribe(handler, options);
```

### Asynchronous Dispatch (Parallel Processing)

```csharp
// Use default processor count for max concurrency
var options = new SubscriptionOptions().WithAsynchronousDispatch();

// Or specify custom concurrency
var options = new SubscriptionOptions().WithAsynchronousDispatch(maxConcurrency: 4);

var subscription = subscriber.Subscribe(handler, options);
```

### Broker-wide Default Configuration

```csharp
var brokerOptions = new InMemoryBrokerOptions
{
    DefaultDispatchMode = DispatchMode.Asynchronous,
    DefaultMaxConcurrency = 8
};

var broker = new InMemoryMessageBroker(brokerOptions);
```

## 📋 Advanced Usage

### Multiple Subscribers (Fan-out)

```csharp
// Multiple handlers can subscribe to the same message type
var orderHandler = new OrderEventHandler();
var inventoryHandler = new InventoryEventHandler();

var orderSubscription = subscriber.Subscribe(orderHandler);
var inventorySubscription = subscriber.Subscribe(inventoryHandler);

await orderSubscription.StartAsync();
await inventorySubscription.StartAsync();

// Both handlers will receive the same event
await eventPublisher.PublishEventAsync(orderEvent);
```

### Topic-based Subscriptions

```csharp
// Subscribe to specific topics
var subscription = subscriber.Subscribe("orders.created", handler);

// Publish to specific topics  
await publisher.PublishAsync("orders.created", orderEvent);
```

### Commands vs Events

```csharp
// Events (one-to-many, fan-out)
var eventPublisher = broker.GetEventPublisher();
await eventPublisher.PublishEventAsync(orderEvent);

// Commands (typically one-to-one)
var commandSender = broker.GetCommandSender();
await commandSender.SendCommandAsync(processPaymentCommand);
```

## 🏗️ Architecture

### Core Components

- **`InMemoryMessageBroker`**: Main broker implementing `IMessageBroker`
- **`InMemoryMessagePublisher`**: Publishes messages to topics
- **`InMemoryMessageSubscriber`**: Creates and manages subscriptions
- **`InMemorySubscription<T>`**: Individual subscription with configurable dispatch
- **`InMemoryBrokerOptions`**: Configuration options
- **`SubscriptionOptionsExtensions`**: Fluent configuration API

### Message Flow

```
Publisher → Topic → Fan-out → [Subscription1, Subscription2, ...] → Handlers
```

Each subscription receives its own copy of the message, enabling true fan-out delivery.

## 🏗️ Using with IHostBuilder

HermesTransport.InMemory can be registered with the .NET Generic Host for dependency injection and configuration. This is the recommended approach for ASP.NET Core, Worker Services, and other modern .NET applications.

```csharp
using HermesTransport;
using HermesTransport.InMemory;
using HermesTransport.InMemory.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddHermesTransport(options =>
        {
            options.AddInMemoryBroker(inMemory =>
            {
                // Optional: configure in-memory broker options
                inMemory.MaxConcurrency = 4;
                // Register as command, event, or message broker as needed
                inMemory.UseForCommands();
                inMemory.UseForEvents();
            });
        });
        // Register your handlers, etc.
        services.AddSingleton<IEventHandler<OrderCreated>, OrderEventHandler>();
    })
    .Build();

await host.RunAsync();
```

- `AddHermesTransport` is an extension method from the core HermesTransport package.
- `AddInMemoryBroker` registers the in-memory broker and allows further configuration.
- Use `UseForCommands()`, `UseForEvents()`, or `UseForMessages()` to specify which message types the broker should handle.

## 📝 Logging and Exception Handling

HermesTransport.InMemory integrates with Microsoft.Extensions.Logging for comprehensive logging:

### Logging Features

- **Debug logs**: Message publishing and handling operations
- **Warning logs**: Cancelled operations  
- **Error logs**: Failed operations and exceptions
- **Information logs**: Hosted service lifecycle events

### Exception Handling

- **Null validation**: Commands and events are validated for null values
- **Cancellation support**: Proper handling of `CancellationToken`
- **Error isolation**: Exceptions in one subscription don't affect others
- **Graceful degradation**: Failed message deliveries are logged but don't stop the broker

### Configuration Example

```csharp
var hostBuilder = Host.CreateDefaultBuilder()
    .ConfigureLogging(logging =>
    {
        logging.AddConsole();
        logging.SetMinimumLevel(LogLevel.Debug);
    })
    .ConfigureServices((context, services) =>
    {
        services.AddHermesTransportInMemory();
    });
```

## 🧪 Testing

The library includes comprehensive tests covering:

- Basic broker operations (connect, disconnect, topics)
- Message publishing and subscription
- Synchronous vs asynchronous dispatch
- Multiple subscribers
- Error handling and cleanup
- Configuration options

Run tests:

```bash
dotnet test
```

## 🎪 Examples

See the `/examples` directory for complete working examples:

- **BasicUsageExample**: Demonstrates core features, sync/async dispatch, and multiple subscribers
- **ServiceCollectionExample**: Shows dependency injection integration and hosted service usage

Run the example:

```bash
cd examples/BasicUsageExample
dotnet run
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Related

- [HermesTransport](https://www.nuget.org/packages/HermesTransport/) - Core messaging abstractions
