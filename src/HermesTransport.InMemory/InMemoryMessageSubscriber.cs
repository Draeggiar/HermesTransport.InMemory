using HermesTransport;

namespace HermesTransport.InMemory;

internal class InMemoryMessageSubscriber : IMessageSubscriber
{
    private readonly InMemoryMessageBroker _broker;

    public InMemoryMessageSubscriber(InMemoryMessageBroker broker)
    {
        _broker = broker;
    }

    public ISubscription Subscribe<TMessage>(IMessageHandler<TMessage> handler, SubscriptionOptions? options = null) 
        where TMessage : IMessage
    {
        var topic = typeof(TMessage).Name;
        return Subscribe(topic, handler, options);
    }

    public ISubscription Subscribe<TMessage>(string topic, IMessageHandler<TMessage> handler, SubscriptionOptions? options = null) 
        where TMessage : IMessage
    {
        if (string.IsNullOrEmpty(topic))
            throw new ArgumentException("Topic cannot be null or empty", nameof(topic));
        
        if (handler is null)
            throw new ArgumentNullException(nameof(handler));

        options ??= new SubscriptionOptions();
        
        // Apply default dispatch mode if MaxConcurrency is the default value
        if (options.MaxConcurrency == 1 && _broker.Options.DefaultDispatchMode == DispatchMode.Asynchronous)
        {
            options.MaxConcurrency = _broker.Options.DefaultMaxConcurrency;
        }
        
        return new InMemorySubscription<TMessage>(topic, _broker, handler, options);
    }
}