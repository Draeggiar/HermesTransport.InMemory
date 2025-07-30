using HermesTransport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HermesTransport.InMemory;

/// <summary>
/// Extension methods for configuring HermesTransport.InMemory services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds HermesTransport.InMemory services to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for the broker options.</param>
    /// <returns>The service collection for method chaining.</returns>
    public static IServiceCollection AddHermesTransportInMemory(
        this IServiceCollection services,
        Action<InMemoryBrokerOptions>? configure = null)
    {
        var options = new InMemoryBrokerOptions();
        configure?.Invoke(options);

        // Register the broker options
        services.TryAddSingleton(options);

        // Register the message broker as singleton
        services.TryAddSingleton<InMemoryMessageBroker>(serviceProvider =>
        {
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var brokerOptions = serviceProvider.GetRequiredService<InMemoryBrokerOptions>();
            return new InMemoryMessageBroker(brokerOptions, loggerFactory);
        });

        // Register HermesTransport abstractions
        services.TryAddSingleton<IMessageBroker>(serviceProvider => 
            serviceProvider.GetRequiredService<InMemoryMessageBroker>());
        
        services.TryAddSingleton<IMessagePublisher>(serviceProvider => 
            serviceProvider.GetRequiredService<InMemoryMessageBroker>().GetPublisher());
        
        services.TryAddSingleton<IMessageSubscriber>(serviceProvider => 
            serviceProvider.GetRequiredService<InMemoryMessageBroker>().GetSubscriber());
        
        services.TryAddSingleton<IEventPublisher>(serviceProvider => 
            serviceProvider.GetRequiredService<InMemoryMessageBroker>().GetEventPublisher());
        
        services.TryAddSingleton<ICommandSender>(serviceProvider => 
            serviceProvider.GetRequiredService<InMemoryMessageBroker>().GetCommandSender());

        return services;
    }

    /// <summary>
    /// Adds HermesTransport.InMemory services and a hosted service that automatically connects the broker.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration for the broker options.</param>
    /// <returns>The service collection for method chaining.</returns>
    public static IServiceCollection AddHermesTransportInMemoryWithHostedService(
        this IServiceCollection services,
        Action<InMemoryBrokerOptions>? configure = null)
    {
        services.AddHermesTransportInMemory(configure);
        services.AddHostedService<InMemoryMessageBrokerHostedService>();
        return services;
    }
}

/// <summary>
/// Hosted service that manages the lifecycle of the InMemory message broker.
/// </summary>
internal class InMemoryMessageBrokerHostedService : BackgroundService
{
    private readonly InMemoryMessageBroker _messageBroker;
    private readonly ILogger<InMemoryMessageBrokerHostedService> _logger;

    public InMemoryMessageBrokerHostedService(
        InMemoryMessageBroker messageBroker,
        ILogger<InMemoryMessageBrokerHostedService> logger)
    {
        _messageBroker = messageBroker ?? throw new ArgumentNullException(nameof(messageBroker));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting InMemory message broker hosted service");
        await _messageBroker.ConnectAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
        _logger.LogInformation("InMemory message broker hosted service started");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping InMemory message broker hosted service");
        await base.StopAsync(cancellationToken);
        await _messageBroker.DisconnectAsync(cancellationToken);
        _logger.LogInformation("InMemory message broker hosted service stopped");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The InMemory broker is event-driven and doesn't need continuous polling
        // This method can be used for health checks or maintenance tasks if needed
        _logger.LogDebug("InMemory message broker hosted service is running");
        
        // Keep the service alive while not cancelled
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }
    }
}