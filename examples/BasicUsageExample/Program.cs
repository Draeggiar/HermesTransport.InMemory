using HermesTransport;
using HermesTransport.InMemory;
using HermesTransport.InMemory.Configuration;
using HermesTransport.Messaging;
using HermesTransport.Subscriptions;

Console.WriteLine("🚀 HermesTransport.InMemory - Basic Usage Example");
Console.WriteLine("================================================");

// Create broker with default synchronous dispatch
var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
await broker.ConnectAsync();

// Get components
var eventPublisher = broker.GetEventPublisher();
var commandSender = broker.GetCommandSender();
var subscriber = broker.GetSubscriber();

// Setup handlers with different dispatch modes
var orderHandler = new OrderEventHandler();
var inventoryHandler = new InventoryEventHandler();
var paymentHandler = new PaymentCommandHandler();

// Subscribe with synchronous dispatch (sequential processing)
Console.WriteLine("\n🔧 Setting up synchronous subscriptions...");
var syncOptions = new SubscriptionOptions().WithSynchronousDispatch();
var orderSubscription = subscriber.Subscribe(orderHandler, syncOptions);
var inventorySubscription = subscriber.Subscribe(inventoryHandler, syncOptions);
var paymentSubscription = subscriber.Subscribe(paymentHandler, syncOptions);

// Start subscriptions
await orderSubscription.StartAsync();
await inventorySubscription.StartAsync();
await paymentSubscription.StartAsync();

Console.WriteLine("✅ Subscriptions started");

// Demo 1: Event publishing with multiple subscribers
Console.WriteLine("\n📡 Demo 1: Publishing events (multiple subscribers receive same event)");
var orderEvent = new OrderCreated
{
    OrderId = "ORD-001",
    Amount = 99.99m,
    CustomerName = "John Doe"
};

await eventPublisher.PublishEventAsync(orderEvent);
await Task.Delay(500); // Allow processing time

// Demo 2: Command sending
Console.WriteLine("\n📡 Demo 2: Sending commands");
var paymentCommand = new ProcessPayment
{
    OrderId = "ORD-001",
    Amount = 99.99m
};

await commandSender.SendCommandAsync(paymentCommand);
await Task.Delay(500); // Allow processing time

// Stop synchronous subscriptions
await orderSubscription.StopAsync();
await inventorySubscription.StopAsync();
await paymentSubscription.StopAsync();

// Demo 3: Asynchronous dispatch
Console.WriteLine("\n🔧 Setting up asynchronous subscriptions...");
var asyncOptions = new SubscriptionOptions().WithAsynchronousDispatch(maxConcurrency: 3);
var asyncOrderSubscription = subscriber.Subscribe(orderHandler, asyncOptions);
var asyncInventorySubscription = subscriber.Subscribe(inventoryHandler, asyncOptions);

await asyncOrderSubscription.StartAsync();
await asyncInventorySubscription.StartAsync();

Console.WriteLine("\n📡 Demo 3: Asynchronous processing (parallel)");
var startTime = DateTime.UtcNow;

// Publish multiple events quickly
for (int i = 1; i <= 3; i++)
{
    var evt = new OrderCreated
    {
        OrderId = $"ORD-{i:D3}",
        Amount = 50.00m * i,
        CustomerName = $"Customer {i}"
    };
    await eventPublisher.PublishEventAsync(evt);
}

await Task.Delay(300); // Allow parallel processing
var endTime = DateTime.UtcNow;

Console.WriteLine($"⏱️  Parallel processing completed in {(endTime - startTime).TotalMilliseconds:F0}ms");

// Cleanup
await asyncOrderSubscription.StopAsync();
await asyncInventorySubscription.StopAsync();
await broker.DisconnectAsync();

Console.WriteLine("\n✅ Example completed successfully!");
Console.WriteLine("\n💡 Key Features Demonstrated:");
Console.WriteLine("   • Event publishing with multiple subscribers");
Console.WriteLine("   • Command sending");
Console.WriteLine("   • Synchronous (sequential) message processing");
Console.WriteLine("   • Asynchronous (parallel) message processing");
Console.WriteLine("   • Proper subscription lifecycle management");

// Sample message types
public class OrderCreated : IEvent
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    
    // Required IEvent properties
    public string Source { get; set; } = "order-service";
    public string Version { get; set; } = "1.0";
    
    // Required IMessage properties
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(OrderCreated);
    public string CorrelationId { get; set; } = string.Empty;
}

public class ProcessPayment : ICommand
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    
    // Required ICommand properties
    public string Target { get; set; } = "payment-service";
    public string Action { get; set; } = "process";
    
    // Required IMessage properties
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(ProcessPayment);
    public string CorrelationId { get; set; } = string.Empty;
}

// Sample handlers
public class OrderEventHandler : IEventHandler<OrderCreated>
{
    public async Task HandleAsync(OrderCreated orderEvent, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"📦 Order event received: {orderEvent.OrderId} for ${orderEvent.Amount} from {orderEvent.CustomerName}");
        await Task.Delay(100, cancellationToken); // Simulate processing
    }
}

public class PaymentCommandHandler : ICommandHandler<ProcessPayment>
{
    public async Task HandleAsync(ProcessPayment command, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"💳 Processing payment: {command.OrderId} for ${command.Amount}");
        await Task.Delay(200, cancellationToken); // Simulate payment processing
    }
}

public class InventoryEventHandler : IEventHandler<OrderCreated>
{
    public async Task HandleAsync(OrderCreated orderEvent, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"📋 Inventory update for order: {orderEvent.OrderId}");
        await Task.Delay(50, cancellationToken); // Simulate inventory update
    }
}
