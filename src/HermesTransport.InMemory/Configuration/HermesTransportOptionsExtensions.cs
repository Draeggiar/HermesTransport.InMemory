using HermesTransport.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace HermesTransport.InMemory.Configuration;

/// <summary>
///     Provides extension methods for configuring <see cref="HermesTransportOptions" />
///     to use the in-memory message broker.
/// </summary>
public static class HermesTransportOptionsExtensions
{
    /// <summary>
    ///     Adds and configures the in-memory message broker for the specified <see cref="HermesTransportOptions" />.
    ///     Registers <see cref="InMemoryBrokerOptions" /> and <see cref="InMemoryMessageBroker" /> as singletons
    ///     in the service collection.
    /// </summary>
    /// <param name="options">The transport options to extend.</param>
    /// <param name="configure">A delegate to configure the <see cref="InMemoryBrokerOptions" />.</param>
    /// <returns>The same <see cref="HermesTransportOptions" /> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="options" /> is <c>null</c>.
    /// </exception>
    public static HermesTransportOptions AddInMemoryBroker(this HermesTransportOptions options,
        Action<InMemoryBrokerOptions> configure)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));

        var inMemoryBrokerOptions = new InMemoryBrokerOptions { TransportOptions = options };
        configure(inMemoryBrokerOptions);

        options.Services.AddSingleton(inMemoryBrokerOptions);
        options.Services.AddSingleton<InMemoryMessageBroker>();

        return options;
    }
}