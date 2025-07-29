using HermesTransport;
using System.Threading.Channels;

namespace HermesTransport.InMemory;

internal class InMemorySubscription<TMessage> : ISubscription, IInternalSubscription, IDisposable 
    where TMessage : IMessage
{
    private readonly string _topic;
    private readonly InMemoryMessageBroker _broker;
    private readonly IMessageHandler<TMessage> _handler;
    private readonly SubscriptionOptions _options;
    private readonly Channel<IMessage> _messageQueue;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly SemaphoreSlim _semaphore;
    private Task? _processingTask;
    private bool _isActive = false;
    private bool _disposed = false;

    public string SubscriptionId { get; } = Guid.NewGuid().ToString();
    public bool IsActive => _isActive;

    public InMemorySubscription(
        string topic, 
        InMemoryMessageBroker broker,
        IMessageHandler<TMessage> handler, 
        SubscriptionOptions options)
    {
        _topic = topic;
        _broker = broker;
        _handler = handler;
        _options = options;
        _semaphore = new SemaphoreSlim(_options.MaxConcurrency, _options.MaxConcurrency);
        _messageQueue = Channel.CreateUnbounded<IMessage>();
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_isActive) return Task.CompletedTask;

        _isActive = true;
        
        // Register with broker
        _broker.AddSubscription(_topic, this);
        
        // Start message processing task
        _processingTask = ProcessMessagesAsync(_cancellationTokenSource.Token);
        
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_isActive) return;

        _isActive = false;
        
        // Unregister from broker
        _broker.RemoveSubscription(_topic, this);
        
        // Complete the message queue
        _messageQueue.Writer.Complete();
        
        // Cancel processing
        _cancellationTokenSource.Cancel();

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

    public async Task DeliverMessageAsync(IMessage message)
    {
        if (!_isActive) return;
        
        // Only deliver messages of the correct type
        if (message is TMessage)
        {
            await _messageQueue.Writer.WriteAsync(message);
        }
    }

    public void Complete()
    {
        _messageQueue.Writer.Complete();
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _messageQueue.Reader.ReadAllAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested) break;

                // Cast to the expected type (we already filtered in DeliverMessageAsync)
                if (message is TMessage typedMessage)
                {
                    try
                    {
                        if (_options.MaxConcurrency == 1)
                        {
                            // Synchronous processing
                            await ProcessMessageSynchronously(typedMessage, cancellationToken);
                        }
                        else
                        {
                            // Asynchronous parallel processing
                            _ = ProcessMessageAsynchronously(typedMessage, cancellationToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log error (in a real implementation, you'd use ILogger)
                        Console.WriteLine($"Error processing message {message.MessageId}: {ex.Message}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when stopping
        }
    }

    private async Task ProcessMessageSynchronously(TMessage message, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            await _handler.HandleAsync(message, cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task ProcessMessageAsynchronously(TMessage message, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        
        // Don't use fire-and-forget, await the task completion
        _ = Task.Run(async () =>
        {
            try
            {
                await _handler.HandleAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                // Log error (in a real implementation, you'd use ILogger)
                Console.WriteLine($"Error in async handler for message {message.MessageId}: {ex.Message}");
            }
            finally
            {
                _semaphore.Release();
            }
        }, cancellationToken);
    }

    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            StopAsync().Wait(TimeSpan.FromSeconds(2));
        }
        catch
        {
            // Ignore timeout during disposal
        }
        
        _cancellationTokenSource.Dispose();
        _semaphore.Dispose();
        _disposed = true;
    }
}