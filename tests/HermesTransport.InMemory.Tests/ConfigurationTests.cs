using HermesTransport;
using HermesTransport.InMemory.Tests.TestMessages;
using Xunit;

namespace HermesTransport.InMemory.Tests;

public class ConfigurationTests
{
    [Fact]
    public void InMemoryBrokerOptions_Should_Have_Default_Values()
    {
        // Arrange & Act
        var options = new InMemoryBrokerOptions();
        
        // Assert
        Assert.Equal(DispatchMode.Synchronous, options.DefaultDispatchMode);
        Assert.Equal(Environment.ProcessorCount, options.DefaultMaxConcurrency);
    }

    [Fact]
    public async Task Broker_With_Async_Default_Should_Use_Async_Dispatch()
    {
        // Arrange
        var options = new InMemoryBrokerOptions
        {
            DefaultDispatchMode = DispatchMode.Asynchronous,
            DefaultMaxConcurrency = 2
        };
        
        var broker = new InMemoryMessageBroker(options);
        await broker.ConnectAsync();
        
        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestMessageHandler { DelayMs = 100, ExpectedCount = 2 };
        
        // Default subscription options should use async dispatch
        var subscription = subscriber.Subscribe(handler);
        await subscription.StartAsync();

        var messages = new[]
        {
            new TestMessage { Content = "Message 1" },
            new TestMessage { Content = "Message 2" }
        };

        var startTime = DateTime.UtcNow;

        // Act
        foreach (var message in messages)
        {
            await publisher.PublishAsync(message);
        }
        
        await handler.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        var endTime = DateTime.UtcNow;
        var totalTime = (endTime - startTime).TotalMilliseconds;
        
        Assert.Equal(2, handler.ReceivedMessages.Count);
        // With async processing, should take less time than sequential
        Assert.True(totalTime < 180, $"Expected less than 180ms for async processing, but took {totalTime}ms");
        
        // Cleanup
        await subscription.StopAsync();
        subscription.Dispose();
    }

    [Fact]
    public void SubscriptionOptions_WithSynchronousDispatch_Should_Set_MaxConcurrency_To_One()
    {
        // Arrange
        var options = new SubscriptionOptions();
        
        // Act
        options.WithSynchronousDispatch();
        
        // Assert
        Assert.Equal(1, options.MaxConcurrency);
    }

    [Fact]
    public void SubscriptionOptions_WithAsynchronousDispatch_Should_Set_MaxConcurrency()
    {
        // Arrange
        var options = new SubscriptionOptions();
        
        // Act
        options.WithAsynchronousDispatch(4);
        
        // Assert
        Assert.Equal(4, options.MaxConcurrency);
    }

    [Fact]
    public void SubscriptionOptions_WithAsynchronousDispatch_Default_Should_Use_ProcessorCount()
    {
        // Arrange
        var options = new SubscriptionOptions();
        
        // Act
        options.WithAsynchronousDispatch();
        
        // Assert
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrency);
    }

    [Fact]
    public void SubscriptionOptions_WithAsynchronousDispatch_Zero_Should_Use_ProcessorCount()
    {
        // Arrange
        var options = new SubscriptionOptions();
        
        // Act
        options.WithAsynchronousDispatch(0);
        
        // Assert
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrency);
    }

    [Theory]
    [InlineData(DispatchMode.Synchronous)]
    [InlineData(DispatchMode.Asynchronous)]
    public void DispatchMode_Should_Have_Expected_Values(DispatchMode mode)
    {
        // Assert - Just checking the enum values exist
        Assert.True(Enum.IsDefined(typeof(DispatchMode), mode));
    }
}