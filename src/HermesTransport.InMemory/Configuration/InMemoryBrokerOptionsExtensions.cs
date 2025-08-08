using Microsoft.Extensions.DependencyInjection;

namespace HermesTransport.InMemory.Configuration;

/// <summary>
///     Provides extension methods for configuring <see cref="InMemoryBrokerOptions" />
///     to use the in-memory message broker for commands, events, or messages.
/// </summary>
public static class InMemoryBrokerOptionsExtensions
{
    /// <summary>
    ///     Configures the <see cref="InMemoryBrokerOptions" /> to use the in-memory broker for command messages.
    /// </summary>
    /// <param name="options">The in-memory broker options to configure.</param>
    /// <returns>The same <see cref="InMemoryBrokerOptions" /> instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown if <see cref="InMemoryBrokerOptions.TransportOptions" /> is null.
    /// </exception>
    public static InMemoryBrokerOptions UseForCommands(this InMemoryBrokerOptions options)
    {
        if (options.TransportOptions == null)
            throw new InvalidOperationException("Add memory broker before configuring options.");

        options.TransportOptions.RegisterCommandBroker(provider => provider.GetRequiredService<InMemoryMessageBroker>());
        return options;
    }

    /// <summary>
    ///     Configures the <see cref="InMemoryBrokerOptions" /> to use the in-memory broker for event messages.
    /// </summary>
    /// <param name="options">The in-memory broker options to configure.</param>
    /// <returns>The same <see cref="InMemoryBrokerOptions" /> instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown if <see cref="InMemoryBrokerOptions.TransportOptions" /> is null.
    /// </exception>
    public static InMemoryBrokerOptions UseForEvents(this InMemoryBrokerOptions options)
    {
        if (options.TransportOptions == null)
            throw new InvalidOperationException("Add memory broker before configuring options.");

        options.TransportOptions.RegisterEventBroker(provider => provider.GetRequiredService<InMemoryMessageBroker>());
        return options;
    }

    /// <summary>
    ///     Configures the <see cref="InMemoryBrokerOptions" /> to use the in-memory broker for generic messages.
    /// </summary>
    /// <param name="options">The in-memory broker options to configure.</param>
    /// <returns>The same <see cref="InMemoryBrokerOptions" /> instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown if <see cref="InMemoryBrokerOptions.TransportOptions" /> is null.
    /// </exception>
    public static InMemoryBrokerOptions UseForMessages(this InMemoryBrokerOptions options)
    {
        if (options.TransportOptions == null)
            throw new InvalidOperationException("Add memory broker before configuring options.");

        options.TransportOptions.RegisterMessageBroker(provider => provider.GetRequiredService<InMemoryMessageBroker>());
        return options;
    }
}