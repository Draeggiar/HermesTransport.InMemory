using System.Collections.Concurrent;
using HermesTransport.Brokers;
using HermesTransport.InMemory.Configuration;
using HermesTransport.Messaging;
using HermesTransport.Subscriptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HermesTransport.InMemory;

public sealed class InMemoryMessageBroker : IMessageBroker
{
    private readonly InMemoryCommandSender _commandSender;
    private readonly InMemoryEventPublisher _eventPublisher;
    private readonly ILogger<InMemoryMessageBroker> _logger;
    private readonly InMemoryMessagePublisher _publisher;
    private readonly InMemoryMessageSubscriber _subscriber;
    private readonly ConcurrentDictionary<string, List<ISubscription>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, bool> _topics = new();
    private bool _disposed;
    public InMemoryBrokerOptions Options { get; }

    public InMemoryMessageBroker(InMemoryBrokerOptions options, ILoggerFactory? loggerFactory = null)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = loggerFactory?.CreateLogger<InMemoryMessageBroker>() ?? NullLogger<InMemoryMessageBroker>.Instance;
        _publisher = new InMemoryMessagePublisher(this);
        _subscriber = new InMemoryMessageSubscriber(this, loggerFactory);
        _eventPublisher = new InMemoryEventPublisher(this);
        _commandSender = new InMemoryCommandSender(this, loggerFactory?.CreateLogger<InMemoryCommandSender>());
    }

    public bool IsConnected { get; private set; }

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        IsConnected = true;
        return Task.CompletedTask;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        IsConnected = false;
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
                _logger.LogDebug("Removed subscription for topic '{Topic}'", topic);
            }
        }

        return Task.CompletedTask;
    }

    public Task<IEnumerable<string>> GetTopicsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IEnumerable<string>>(_topics.Keys.ToList());

    public IMessagePublisher GetPublisher() => _publisher;

    public IMessageSubscriber GetSubscriber() => _subscriber;

    public IEventPublisher GetEventPublisher() => _eventPublisher;

    public ICommandSender GetCommandSender() => _commandSender;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        foreach (var subscriptionList in _subscriptions.Values)
        {
            foreach (var subscription in subscriptionList) await subscription.DisposeAsync();
        }

        _subscriptions.Clear();
        _topics.Clear();
        _disposed = true;
    }

    internal void AddSubscription(string topic, ISubscription subscription)
    {
        _subscriptions.AddOrUpdate(topic,
            [subscription],
            (key, existing) =>
            {
                existing.Add(subscription);
                return existing;
            });
    }

    internal async Task PublishToTopicAsync(string topic, IMessage message)
    {
        if (_subscriptions.TryGetValue(topic, out var subscriptions))
        {
            // Create a copy of the subscriptions list to avoid modification during iteration
            var subscriptionsCopy = subscriptions.ToList();

            foreach (var subscription in subscriptionsCopy)
                try
                {
                    await subscription.DeliverMessageAsync(message);
                    _logger.LogDebug("Delivered message to subscription for topic '{Topic}'", topic);
                }
                catch (Exception ex)
                {
                    // Log error using the logger
                    _logger.LogError(ex, "Error delivering message to subscription.");
                }
        }
    }

    internal void RemoveSubscription(string topic, ISubscription subscription)
    {
        if (_subscriptions.TryGetValue(topic, out var subscriptions))
        {
            subscriptions.Remove(subscription);
            if (subscriptions.Count == 0) _subscriptions.TryRemove(topic, out _);
        }
    }
}