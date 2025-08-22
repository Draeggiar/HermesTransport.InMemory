using HermesTransport.Configuration;

namespace HermesTransport.InMemory.Configuration;

/// <summary>
///     Configuration options for the in-memory message broker
/// </summary>
public class InMemoryBrokerOptions
{
    internal HermesTransportOptions? TransportOptions { get; set; }

    /// <summary>
    ///     Default dispatch mode for subscriptions when not explicitly specified
    /// </summary>
    public DispatchMode DefaultDispatchMode { get; set; } = DispatchMode.Synchronous;

    /// <summary>
    ///     Default maximum concurrency for asynchronous dispatch
    /// </summary>
    public int DefaultMaxConcurrency { get; set; } = Environment.ProcessorCount;
}

/// <summary>
///     Dispatch mode for message processing
/// </summary>
public enum DispatchMode
{
    /// <summary>
    ///     Messages are processed synchronously, one at a time
    /// </summary>
    Synchronous,

    /// <summary>
    ///     Messages are processed asynchronously with configured concurrency
    /// </summary>
    Asynchronous
}