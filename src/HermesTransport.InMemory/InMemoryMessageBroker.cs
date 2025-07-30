using HermesTransport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace HermesTransport.InMemory;

public class InMemoryMessageBroker : IMessageBroker, IDisposable
{
    private readonly ConcurrentDictionary<string, List<IInternalSubscription>> _subscriptions = new();
    private readonly InMemoryMessagePublisher _publisher;
    private readonly InMemoryMessageSubscriber _subscriber;
    private readonly InMemoryEventPublisher _eventPublisher;
    private readonly InMemoryCommandSender _commandSender;
    private readonly InMemoryBrokerOptions _options;
    private readonly ConcurrentDictionary<string, bool> _topics = new();
    private readonly ILogger<InMemoryMessageBroker> _logger;
    private bool _isConnected = false;
    private bool _disposed = false;

    public bool IsConnected => _isConnected;
    public InMemoryBrokerOptions Options => _options;

    public InMemoryMessageBroker() : this(new InMemoryBrokerOptions())
    {
    }

    public InMemoryMessageBroker(InMemoryBrokerOptions options, ILoggerFactory? loggerFactory = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = loggerFactory?.CreateLogger<InMemoryMessageBroker>() ?? NullLogger<InMemoryMessageBroker>.Instance;
        _publisher = new InMemoryMessagePublisher(this);
        _subscriber = new InMemoryMessageSubscriber(this);
        _eventPublisher = new InMemoryEventPublisher(this);
        _commandSender = new InMemoryCommandSender(this, loggerFactory?.CreateLogger<InMemoryCommandSender>());
    }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _isConnected = true;
        return Task.CompletedTask;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        _isConnected = false;
        return Task.CompletedTask;
    }

    public Task CreateTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        _topics.TryAdd(topic, true);
        return Task.CompletedTask;
    }

    public Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        _topics.TryRemove(topic, out _);
        if (_subscriptions.TryRemove(topic, out var subscriptions))
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Complete();
            }
        }
        return Task.CompletedTask;
    }

    public Task<IEnumerable<string>> GetTopicsAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IEnumerable<string>>(_topics.Keys.ToList());
    }

    public IMessagePublisher GetPublisher() => _publisher;

    public IMessageSubscriber GetSubscriber() => _subscriber;

    public IEventPublisher GetEventPublisher() => _eventPublisher;

    public ICommandSender GetCommandSender() => _commandSender;

    internal async Task PublishToTopicAsync(string topic, IMessage message)
    {
        if (_subscriptions.TryGetValue(topic, out var subscriptions))
        {
            // Create a copy of the subscriptions list to avoid modification during iteration
            var subscriptionsCopy = subscriptions.ToList();
            
            foreach (var subscription in subscriptionsCopy)
            {
                try
                {
                    await subscription.DeliverMessageAsync(message);
                }
                catch (Exception ex)
                {
                    // Log error - in production, use proper logging
                    Console.WriteLine($"Error delivering message to subscription: {ex.Message}");
                }
            }
        }
    }

    internal void AddSubscription(string topic, IInternalSubscription subscription)
    {
        _subscriptions.AddOrUpdate(topic, 
            new List<IInternalSubscription> { subscription },
            (key, existing) => 
            {
                existing.Add(subscription);
                return existing;
            });
    }

    internal void RemoveSubscription(string topic, IInternalSubscription subscription)
    {
        if (_subscriptions.TryGetValue(topic, out var subscriptions))
        {
            subscriptions.Remove(subscription);
            if (subscriptions.Count == 0)
            {
                _subscriptions.TryRemove(topic, out _);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;

        foreach (var subscriptionList in _subscriptions.Values)
        {
            foreach (var subscription in subscriptionList)
            {
                subscription.Complete();
            }
        }
        _subscriptions.Clear();
        _topics.Clear();
        _disposed = true;
    }
}

internal interface IInternalSubscription
{
    Task DeliverMessageAsync(IMessage message);
    void Complete();
}