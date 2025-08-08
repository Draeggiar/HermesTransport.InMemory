using HermesTransport.InMemory.Configuration;
using HermesTransport.InMemory.Tests.TestMessages;
using HermesTransport.Subscriptions;

namespace HermesTransport.InMemory.Tests;

public class MessagePublishingAndSubscriptionTests
{
    [Fact]
    public async Task Asynchronous_Dispatch_Should_Process_Messages_In_Parallel()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestMessageHandler { DelayMs = 100, ExpectedCount = 3 };

        var options = new SubscriptionOptions().WithAsynchronousDispatch(3);
        var subscription = subscriber.Subscribe(handler, options);
        await subscription.StartAsync();

        var messages = new[]
        {
            new TestMessage { Content = "Message 1" },
            new TestMessage { Content = "Message 2" },
            new TestMessage { Content = "Message 3" }
        };

        var startTime = DateTime.UtcNow;

        // Act
        foreach (var message in messages) await publisher.PublishAsync(message);

        // Wait for all messages to be processed
        await handler.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Give async tasks a moment to complete
        await Task.Delay(50);

        // Assert
        var endTime = DateTime.UtcNow;
        var totalTime = (endTime - startTime).TotalMilliseconds;

        Assert.Equal(3, handler.ReceivedMessages.Count);
        Assert.True(totalTime < 250, $"Expected less than 250ms for parallel processing, but took {totalTime}ms");

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task Command_Sender_Should_Send_Commands()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var commandSender = broker.GetCommandSender();
        var subscriber = broker.GetSubscriber();
        var handler = new TestCommandHandler();

        var subscription = subscriber.Subscribe(handler);
        await subscription.StartAsync();

        var testCommand = new TestCommand { Payload = "Command payload" };

        // Act
        await commandSender.SendCommandAsync(testCommand);

        // Assert
        await handler.CommandReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(handler.ReceivedCommands);
        Assert.Equal("Command payload", handler.ReceivedCommands[0].Payload);

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task Event_Publisher_Should_Publish_Events()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var eventPublisher = broker.GetEventPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestEventHandler();

        var subscription = subscriber.Subscribe(handler);
        await subscription.StartAsync();

        var testEvent = new TestEvent { Data = "Event data" };

        // Act
        await eventPublisher.PublishEventAsync(testEvent);

        // Assert
        await handler.EventReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(handler.ReceivedEvents);
        Assert.Equal("Event data", handler.ReceivedEvents[0].Data);

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task Multiple_Subscribers_Should_Receive_Same_Message()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();

        var handler1 = new TestMessageHandler();
        var handler2 = new TestMessageHandler();

        var subscription1 = subscriber.Subscribe("shared-topic", handler1);
        var subscription2 = subscriber.Subscribe("shared-topic", handler2);

        await subscription1.StartAsync();
        await subscription2.StartAsync();

        var message = new TestMessage { Content = "Shared message" };

        // Act
        await publisher.PublishAsync("shared-topic", message);

        // Assert - Both handlers should receive the message
        await Task.WhenAll(
            handler1.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            handler2.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5))
        );

        Assert.Single(handler1.ReceivedMessages);
        Assert.Single(handler2.ReceivedMessages);
        Assert.Equal("Shared message", handler1.ReceivedMessages[0].Content);
        Assert.Equal("Shared message", handler2.ReceivedMessages[0].Content);

        // Cleanup
        await subscription1.StopAsync();
        await subscription2.StopAsync();
        await subscription1.DisposeAsync();
        await subscription2.DisposeAsync();
    }

    [Fact]
    public async Task Publisher_Should_Publish_Message_To_Specific_Topic()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestMessageHandler();

        var topicName = "custom-topic";
        var subscription = subscriber.Subscribe(topicName, handler);
        await subscription.StartAsync();

        var message = new TestMessage { Content = "Custom topic content" };

        // Act
        await publisher.PublishAsync(topicName, message);

        // Assert
        await handler.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(handler.ReceivedMessages);
        Assert.Equal("Custom topic content", handler.ReceivedMessages[0].Content);

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task Publisher_Should_Publish_Message_To_Subscriber()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestMessageHandler();

        var subscription = subscriber.Subscribe(handler);
        await subscription.StartAsync();

        var message = new TestMessage { Content = "Test content" };

        // Act
        await publisher.PublishAsync(message);

        // Assert
        await handler.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(handler.ReceivedMessages);
        Assert.Equal("Test content", handler.ReceivedMessages[0].Content);

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task Synchronous_Dispatch_Should_Process_Messages_Sequentially()
    {
        // Arrange
        var broker = new InMemoryMessageBroker(new InMemoryBrokerOptions());
        await broker.ConnectAsync();

        var publisher = broker.GetPublisher();
        var subscriber = broker.GetSubscriber();
        var handler = new TestMessageHandler { DelayMs = 100, ExpectedCount = 3 };

        var options = new SubscriptionOptions().WithSynchronousDispatch();
        var subscription = subscriber.Subscribe(handler, options);
        await subscription.StartAsync();

        var messages = new[]
        {
            new TestMessage { Content = "Message 1" },
            new TestMessage { Content = "Message 2" },
            new TestMessage { Content = "Message 3" }
        };

        var startTime = DateTime.UtcNow;

        // Act
        foreach (var message in messages) await publisher.PublishAsync(message);

        // Wait for all messages to be processed
        await handler.MessageReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        var endTime = DateTime.UtcNow;
        var totalTime = (endTime - startTime).TotalMilliseconds;

        Assert.Equal(3, handler.ReceivedMessages.Count);
        Assert.True(totalTime >= 300, $"Expected at least 300ms for sequential processing, but took {totalTime}ms");

        // Cleanup
        await subscription.StopAsync();
        await subscription.DisposeAsync();
    }
}