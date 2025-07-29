using HermesTransport;

namespace HermesTransport.InMemory;

internal class InMemoryCommandSender : ICommandSender
{
    private readonly InMemoryMessagePublisher _messagePublisher;

    public InMemoryCommandSender(InMemoryMessageBroker broker)
    {
        _messagePublisher = new InMemoryMessagePublisher(broker);
    }

    public async Task SendCommandAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default) 
        where TCommand : ICommand
    {
        await _messagePublisher.PublishAsync(command, cancellationToken);
    }
}