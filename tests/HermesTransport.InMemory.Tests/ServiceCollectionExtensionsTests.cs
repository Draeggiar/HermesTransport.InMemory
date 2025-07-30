using HermesTransport;
using HermesTransport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HermesTransport.InMemory.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddHermesTransportInMemory_RegistersAllRequiredServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddHermesTransportInMemory(options =>
        {
            options.DefaultDispatchMode = DispatchMode.Asynchronous;
            options.DefaultMaxConcurrency = 4;
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        Assert.NotNull(serviceProvider.GetService<InMemoryBrokerOptions>());
        Assert.NotNull(serviceProvider.GetService<InMemoryMessageBroker>());
        Assert.NotNull(serviceProvider.GetService<IMessageBroker>());
        Assert.NotNull(serviceProvider.GetService<IMessagePublisher>());
        Assert.NotNull(serviceProvider.GetService<IMessageSubscriber>());
        Assert.NotNull(serviceProvider.GetService<IEventPublisher>());
        Assert.NotNull(serviceProvider.GetService<ICommandSender>());

        // Verify singleton behavior
        var broker1 = serviceProvider.GetService<IMessageBroker>();
        var broker2 = serviceProvider.GetService<IMessageBroker>();
        Assert.Same(broker1, broker2);
    }

    [Fact]
    public void AddHermesTransportInMemoryWithHostedService_RegistersHostedService()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddHermesTransportInMemoryWithHostedService(options =>
        {
            options.DefaultDispatchMode = DispatchMode.Synchronous;
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var hostedServices = serviceProvider.GetServices<IHostedService>();
        Assert.Single(hostedServices); // Should have exactly one hosted service
        Assert.NotNull(hostedServices.First());
    }

    [Fact]
    public void ServiceCollectionExtensions_OptionsConfigurationApplied()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddHermesTransportInMemory(options =>
        {
            options.DefaultDispatchMode = DispatchMode.Asynchronous;
            options.DefaultMaxConcurrency = 8;
        });

        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<InMemoryBrokerOptions>();

        // Assert
        Assert.Equal(DispatchMode.Asynchronous, options.DefaultDispatchMode);
        Assert.Equal(8, options.DefaultMaxConcurrency);
    }

    [Fact]
    public async Task ServiceCollectionExtensions_IntegrationTest()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHermesTransportInMemory();
        services.AddScoped<TestEventHandler>();

        var serviceProvider = services.BuildServiceProvider();

        // Act
        var broker = serviceProvider.GetRequiredService<IMessageBroker>();
        var eventPublisher = serviceProvider.GetRequiredService<IEventPublisher>();
        var subscriber = serviceProvider.GetRequiredService<IMessageSubscriber>();

        await broker.ConnectAsync();

        using var scope = serviceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<TestEventHandler>();
        var subscription = subscriber.Subscribe<TestEvent>(handler);

        await subscription.StartAsync();

        // Act
        var testEvent = new TestEvent();
        await eventPublisher.PublishEventAsync(testEvent);

        await Task.Delay(100); // Allow processing

        // Assert
        Assert.True(handler.EventReceived);
        Assert.Equal(testEvent.MessageId, handler.ReceivedEvent?.MessageId);

        // Cleanup
        await subscription.StopAsync();
        await broker.DisconnectAsync();
    }

    public class TestEvent : IEvent
    {
        public string Source { get; set; } = "test";
        public string Version { get; set; } = "1.0";
        public string MessageId { get; set; } = Guid.NewGuid().ToString();
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
        public string MessageType { get; set; } = nameof(TestEvent);
        public string CorrelationId { get; set; } = string.Empty;
    }

    public class TestEventHandler : IEventHandler<TestEvent>
    {
        public bool EventReceived { get; private set; }
        public TestEvent? ReceivedEvent { get; private set; }

        public Task HandleAsync(TestEvent @event, CancellationToken cancellationToken = default)
        {
            EventReceived = true;
            ReceivedEvent = @event;
            return Task.CompletedTask;
        }
    }
}