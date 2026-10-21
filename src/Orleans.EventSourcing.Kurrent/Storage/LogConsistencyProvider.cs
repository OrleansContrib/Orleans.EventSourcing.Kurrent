using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal sealed class LogConsistencyProvider : ILogViewAdaptorFactory
{
    private readonly IEventConverterFactory eventSerializer;
    private readonly IKurrentClient client;
    private readonly KurrentStorageOptions options;
    private readonly KurrentMetrics metrics;

    internal LogConsistencyProvider(IEventConverterFactory eventSerializer, IKurrentClient client, KurrentStorageOptions options, KurrentMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(eventSerializer);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(metrics);

        this.eventSerializer = eventSerializer;
        this.client = client;
        this.options = options;
        this.metrics = metrics;
    }

    /// <inheritdoc />
    public ILogViewAdaptor<TLogView, TLogEntry> MakeLogViewAdaptor<TLogView, TLogEntry>(ILogViewAdaptorHost<TLogView, TLogEntry> hostGrain, TLogView initialState, string grainTypeName, IGrainStorage? grainStorage, ILogConsistencyProtocolServices services)
        where TLogView : class, new()
        where TLogEntry : class
    {
        ArgumentNullException.ThrowIfNull(services);
        return new KurrentLogViewAdapter<TLogView, TLogEntry>(hostGrain, client, eventSerializer.GetEventConverter<TLogEntry>(), services, options.StreamNameProvider, metrics, options.Retry);
    }

    /// <inheritdoc />
    public bool UsesStorageProvider => false;
}
