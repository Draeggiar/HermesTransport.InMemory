using HermesTransport;
using HermesTransport.Messaging;

namespace HermesTransport.InMemory;

internal class InMemoryMessagePublisher : IMessagePublisher
{
    private readonly InMemoryMessageBroker _broker;

    public InMemoryMessagePublisher(InMemoryMessageBroker broker)
    {
        _broker = broker;
    }

    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default) 
        where TMessage : IMessage
    {
        if (message is null)
            throw new ArgumentNullException(nameof(message));

        var topic = message.GetType().Name;
        await PublishAsync(topic, message, cancellationToken);
    }

    public async Task PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default) 
        where TMessage : IMessage
    {
        if (string.IsNullOrEmpty(topic))
            throw new ArgumentException("Topic cannot be null or empty", nameof(topic));
        
        if (message is null)
            throw new ArgumentNullException(nameof(message));

        await _broker.PublishToTopicAsync(topic, message);
    }
}