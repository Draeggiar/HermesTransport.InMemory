using HermesTransport.Subscriptions;

namespace HermesTransport.InMemory.Configuration;

/// <summary>
/// Extension methods for SubscriptionOptions to configure dispatch mode
/// </summary>
public static class SubscriptionOptionsExtensions
{
    /// <summary>
    /// Configure synchronous dispatch (MaxConcurrency = 1)
    /// </summary>
    public static SubscriptionOptions WithSynchronousDispatch(this SubscriptionOptions options)
    {
        options.MaxConcurrency = 1;
        return options;
    }

    /// <summary>
    /// Configure asynchronous dispatch with specified concurrency
    /// </summary>
    public static SubscriptionOptions WithAsynchronousDispatch(this SubscriptionOptions options, int maxConcurrency = 0)
    {
        options.MaxConcurrency = maxConcurrency <= 0 ? Environment.ProcessorCount : maxConcurrency;
        return options;
    }
}