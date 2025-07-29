using HermesTransport.InMemory.Tests.TestMessages;
using Xunit;

namespace HermesTransport.InMemory.Tests;

public class InMemoryMessageBrokerTests
{
    [Fact]
    public async Task ConnectAsync_Should_Set_IsConnected_To_True()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        
        // Act
        await broker.ConnectAsync();
        
        // Assert
        Assert.True(broker.IsConnected);
    }

    [Fact]
    public async Task DisconnectAsync_Should_Set_IsConnected_To_False()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        await broker.ConnectAsync();
        
        // Act
        await broker.DisconnectAsync();
        
        // Assert
        Assert.False(broker.IsConnected);
    }

    [Fact]
    public async Task CreateTopicAsync_Should_Create_New_Topic()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        var topicName = "test-topic";
        
        // Act
        await broker.CreateTopicAsync(topicName);
        var topics = await broker.GetTopicsAsync();
        
        // Assert
        Assert.Contains(topicName, topics);
    }

    [Fact]
    public async Task DeleteTopicAsync_Should_Remove_Topic()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        var topicName = "test-topic";
        await broker.CreateTopicAsync(topicName);
        
        // Act
        await broker.DeleteTopicAsync(topicName);
        var topics = await broker.GetTopicsAsync();
        
        // Assert
        Assert.DoesNotContain(topicName, topics);
    }

    [Fact]
    public void GetPublisher_Should_Return_Message_Publisher()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        
        // Act
        var publisher = broker.GetPublisher();
        
        // Assert
        Assert.NotNull(publisher);
    }

    [Fact]
    public void GetSubscriber_Should_Return_Message_Subscriber()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        
        // Act
        var subscriber = broker.GetSubscriber();
        
        // Assert
        Assert.NotNull(subscriber);
    }

    [Fact]
    public void GetEventPublisher_Should_Return_Event_Publisher()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        
        // Act
        var eventPublisher = broker.GetEventPublisher();
        
        // Assert
        Assert.NotNull(eventPublisher);
    }

    [Fact]
    public void GetCommandSender_Should_Return_Command_Sender()
    {
        // Arrange
        var broker = new InMemoryMessageBroker();
        
        // Act
        var commandSender = broker.GetCommandSender();
        
        // Assert
        Assert.NotNull(commandSender);
    }
}