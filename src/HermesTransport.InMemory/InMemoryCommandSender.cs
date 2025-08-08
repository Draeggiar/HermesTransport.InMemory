using HermesTransport;
using HermesTransport.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HermesTransport.InMemory;

internal class InMemoryCommandSender : ICommandSender
{
    private readonly InMemoryMessagePublisher _messagePublisher;
    private readonly ILogger<InMemoryCommandSender> _logger;

    public InMemoryCommandSender(InMemoryMessageBroker broker, ILogger<InMemoryCommandSender>? logger = null)
    {
        _messagePublisher = new InMemoryMessagePublisher(broker);
        _logger = logger ?? NullLogger<InMemoryCommandSender>.Instance;
    }

    public async Task SendCommandAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default) 
        where TCommand : ICommand
    {
        if (Equals(command, default(TCommand)))
        {
            var argumentException = new ArgumentNullException(nameof(command), "Command cannot be null");
            _logger.LogError(argumentException, "Attempted to send null command of type {CommandType}", typeof(TCommand).Name);
            throw argumentException;
        }

        try
        {
            _logger.LogDebug("Sending command of type {CommandType}", typeof(TCommand).Name);
            await _messagePublisher.PublishAsync(command, cancellationToken);
            _logger.LogDebug("Successfully sent command of type {CommandType}", typeof(TCommand).Name);
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogWarning(ex, "Command sending was cancelled for command type {CommandType}", typeof(TCommand).Name);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send command of type {CommandType}", typeof(TCommand).Name);
            throw;
        }
    }
}