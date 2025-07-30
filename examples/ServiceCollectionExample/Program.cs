using HermesTransport;
using HermesTransport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Example using service collection extensions
var hostBuilder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // Option 1: Basic registration
        services.AddHermesTransportInMemory(options =>
        {
            options.DefaultDispatchMode = DispatchMode.Asynchronous;
            options.DefaultMaxConcurrency = 4;
        });

        // Option 2: With hosted service (uncomment to use)
        // services.AddHermesTransportInMemoryWithHostedService(options =>
        // {
        //     options.DefaultDispatchMode = DispatchMode.Asynchronous;
        //     options.DefaultMaxConcurrency = 4;
        // });

        // Register handlers
        services.AddScoped<OrderHandler>();
    });

var host = hostBuilder.Build();

// Get services from DI container
var eventPublisher = host.Services.GetRequiredService<IEventPublisher>();
var commandSender = host.Services.GetRequiredService<ICommandSender>();
var subscriber = host.Services.GetRequiredService<IMessageSubscriber>();
var broker = host.Services.GetRequiredService<IMessageBroker>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

// Connect the broker manually (if not using hosted service)
await broker.ConnectAsync();

// Setup subscriptions using DI
using var scope = host.Services.CreateScope();
var orderHandler = scope.ServiceProvider.GetRequiredService<OrderHandler>();

var eventSubscription = subscriber.Subscribe<OrderCreated>(orderHandler);
var commandSubscription = subscriber.Subscribe<ProcessOrder>(orderHandler);

await eventSubscription.StartAsync();
await commandSubscription.StartAsync();

logger.LogInformation("Service Collection Extensions Example - Publishing messages...");

// Publish messages
var orderId = Guid.NewGuid();
await eventPublisher.PublishEventAsync(new OrderCreated { OrderId = orderId, Amount = 99.99m });
await commandSender.SendCommandAsync(new ProcessOrder { OrderId = orderId });

// Wait a bit for processing
await Task.Delay(1000);

// Cleanup
await eventSubscription.StopAsync();
await commandSubscription.StopAsync();
await broker.DisconnectAsync();

logger.LogInformation("Service Collection Extensions Example completed!");

// Simple message types for demonstration
public class OrderCreated : IEvent
{
    public Guid OrderId { get; set; }
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

public class ProcessOrder : ICommand
{
    public Guid OrderId { get; set; }
    
    // Required ICommand properties
    public string Target { get; set; } = "order-service";
    public string Action { get; set; } = "process";
    
    // Required IMessage properties
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(ProcessOrder);
    public string CorrelationId { get; set; } = string.Empty;
}

// Example handler
public class OrderHandler : IEventHandler<OrderCreated>, ICommandHandler<ProcessOrder>
{
    private readonly ILogger<OrderHandler> _logger;

    public OrderHandler(ILogger<OrderHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Handling OrderCreated event for Order {OrderId} with amount {Amount}",
            message.OrderId, message.Amount);
        return Task.CompletedTask;
    }

    public Task HandleAsync(ProcessOrder message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Processing order command for Order {OrderId}", message.OrderId);
        return Task.CompletedTask;
    }
}