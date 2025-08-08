using System.Threading.Channels;
using HermesTransport.Messaging;
using HermesTransport.Subscriptions;
using Microsoft.Extensions.Logging;

namespace HermesTransport.InMemory;

internal sealed class InMemorySubscription<TMessage> : ISubscription where TMessage : IMessage
{
    private readonly InMemoryMessageBroker _broker;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly IMessageHandler<TMessage> _handler;
    private readonly ILogger _logger;
    private readonly Channel<IMessage> _messageQueue;
    private readonly SubscriptionOptions _options;
    private readonly SemaphoreSlim _semaphore;
    private readonly string _topic;
    private bool _disposed;
    private Task? _processingTask;

    public InMemorySubscription(
        string topic,
        InMemoryMessageBroker broker,
        IMessageHandler<TMessage> handler,
        SubscriptionOptions options, ILogger logger)
    {
        _topic = topic;
        _broker = broker;
        _handler = handler;
        _options = options;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _semaphore = new SemaphoreSlim(_options.MaxConcurrency, _options.MaxConcurrency);
        IsActive = false;
        _messageQueue = Channel.CreateUnbounded<IMessage>(new UnboundedChannelOptions { SingleReader = true });
    }

    public async Task DeliverMessageAsync(IMessage message, CancellationToken cancellation = default)
    {
        if (!IsActive) return;

        // Only deliver messages of the correct type
        if (message is TMessage) await _messageQueue.Writer.WriteAsync(message, cancellation);
    }

    public void Complete()
    {
        _messageQueue.Writer.Complete();
    }

    public string SubscriptionId { get; } = Guid.NewGuid().ToString();
    public bool IsActive { get; private set; }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (IsActive) return Task.CompletedTask;

        IsActive = true;

        // Register with broker
        _broker.AddSubscription(_topic, this);

        // Start message processing task
        _processingTask = ProcessMessagesAsync(_cancellationTokenSource.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!IsActive) return;

        IsActive = false;

        // Unregister from broker
        _broker.RemoveSubscription(_topic, this);

        // Complete the message queue
        Complete();

        // Cancel processing
        await _cancellationTokenSource.CancelAsync();

        if (_processingTask != null)
        {
            try
            {
                await _processingTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when cancelling
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        await StopAsync();

        _cancellationTokenSource.Dispose();
        _semaphore.Dispose();
        _disposed = true;
    }

    private async Task ProcessMessage(IMessage message, CancellationToken cancellationToken)
    {
        // Cast to the expected type (we already filtered in DeliverMessageAsync)
        if (message is TMessage typedMessage)
        {
            // For concurrent processing, we don't wait for the task to complete
            // The semaphore handles the concurrency limit
            if (_options.MaxConcurrency == 1)
            {
                // For sequential processing, await each message
                await ProcessMessageAsync(typedMessage, cancellationToken);
            }
            else
            {
                // For concurrent processing, start the task but don't await it
                // The semaphore ensures we don't exceed the concurrency limit
                _ = ProcessMessageAsync(typedMessage, cancellationToken);
            }
        }
        else
        {
            _logger.LogWarning("Received message of type {MessageType} but expected {ExpectedType}", message.GetType().Name,
                typeof(TMessage).Name);
        }
    }

    private async Task ProcessMessageAsync(TMessage message, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            await _handler.HandleAsync(message, cancellationToken);
            _logger.LogDebug("Processed message of type {MessageType} with ID {MessageId}", typeof(TMessage).Name, message.MessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message of type {MessageType}", typeof(TMessage).Name);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _messageQueue.Reader.ReadAllAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested) break;

                await ProcessMessage(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when stopping
        }
    }
}