using HermesTransport;

namespace HermesTransport.InMemory;

internal class InMemoryEventPublisher : IEventPublisher
{
    private readonly InMemoryMessagePublisher _messagePublisher;

    public InMemoryEventPublisher(InMemoryMessageBroker broker)
    {
        _messagePublisher = new InMemoryMessagePublisher(broker);
    }

    public async Task PublishEventAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default) 
        where TEvent : IEvent
    {
        await _messagePublisher.PublishAsync(@event, cancellationToken);
    }
}