using HermesTransport;

namespace HermesTransport.InMemory.Tests.TestMessages;

public class TestMessage : MessageBase
{
    public string Content { get; set; } = string.Empty;
    
    public TestMessage()
    {
        MessageId = Guid.NewGuid().ToString();
        Timestamp = DateTimeOffset.UtcNow;
        MessageType = GetType().Name;
    }
}

public class TestEvent : IEvent
{
    public string Data { get; set; } = string.Empty;
    public string Source { get; set; } = "test-source";
    public string Version { get; set; } = "1.0";
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(TestEvent);
    public string CorrelationId { get; set; } = string.Empty;
}

public class TestCommand : ICommand
{
    public string Payload { get; set; } = string.Empty;
    public string Target { get; set; } = "test-target";
    public string Action { get; set; } = "test-action";
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string MessageType { get; set; } = nameof(TestCommand);
    public string CorrelationId { get; set; } = string.Empty;
}

public class TestMessageHandler : IMessageHandler<TestMessage>
{
    public List<TestMessage> ReceivedMessages { get; } = new();
    public TaskCompletionSource<bool> MessageReceived { get; set; } = new();
    public int DelayMs { get; set; } = 0;
    public int ExpectedCount { get; set; } = 1;
    private int _receivedCount = 0;

    public async Task HandleAsync(TestMessage message, CancellationToken cancellationToken = default)
    {
        if (DelayMs > 0)
            await Task.Delay(DelayMs, cancellationToken);
            
        ReceivedMessages.Add(message);
        var newCount = Interlocked.Increment(ref _receivedCount);
        
        if (newCount >= ExpectedCount)
        {
            MessageReceived.TrySetResult(true);
        }
    }
}

public class TestEventHandler : IEventHandler<TestEvent>
{
    public List<TestEvent> ReceivedEvents { get; } = new();
    public TaskCompletionSource<bool> EventReceived { get; set; } = new();
    
    public async Task HandleAsync(TestEvent @event, CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        ReceivedEvents.Add(@event);
        EventReceived.TrySetResult(true);
    }
}

public class TestCommandHandler : ICommandHandler<TestCommand>
{
    public List<TestCommand> ReceivedCommands { get; } = new();
    public TaskCompletionSource<bool> CommandReceived { get; set; } = new();
    
    public async Task HandleAsync(TestCommand command, CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        ReceivedCommands.Add(command);
        CommandReceived.TrySetResult(true);
    }
}