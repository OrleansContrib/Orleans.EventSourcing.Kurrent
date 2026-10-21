using System.Diagnostics.Metrics;

namespace Orleans.EventSourcing.Kurrent.Observability;

/// <summary>
///     Instruments for the Kurrent providers, created from the host's <see cref="IMeterFactory"/> so they are scoped to its service provider.
/// </summary>
internal sealed class KurrentMetrics
{
    public const string MeterName = "Orleans.EventSourcing.Kurrent";

    public KurrentMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);

        Meter = meterFactory.Create(MeterName);

        CatchupEventsProcessed = Meter.CreateCounter<int>("Catchup.Events", "count", "Number of events processed");
        CatchUpNotificationsProcessed = Meter.CreateCounter<int>("Catchup.Notifications", "count", "Number of notifications processed");
        CatchUpCheckpoints = Meter.CreateCounter<int>("Catchup.Checkpoints", "count", "Number of checkpoints reached");
        CatchUpLive = Meter.CreateGauge<byte>("Catchup.Live", "bool", "1=Live, 0=Catching-up");
        CatchupEventYieldLatency = Meter.CreateHistogram<long>("Catchup.Event.Processing", "ms", "Time taken yielding events");
        CatchupNotificationYieldLatency = Meter.CreateHistogram<long>("Catchup.Notification.Processing", "ms", "Time taken yielding notifications");
        CatchupCheckpointYieldLatency = Meter.CreateHistogram<long>("Catchup.Checkpoint.Processing", "ms", "Time taken yielding checkpoint");

        EventDeserializationLatency = Meter.CreateHistogram<long>("Event.Deserialization", "ms", "Time taken to deserialize");
        EventSerializationLatency = Meter.CreateHistogram<long>("Event.Serialization", "ms", "Time taken to serialize");

        StateDeserializationLatency = Meter.CreateHistogram<long>("State.Deserialization", "ms", "Time taken to deserialize");
        StateSerializationLatency = Meter.CreateHistogram<long>("State.Serialization", "ms", "Time taken to serialize");

        Retries = Meter.CreateCounter<int>("Retries", "count", "Number of Kurrent operations retried after a leader change");
    }

    /// <summary>
    ///     The meter the instruments belong to. It is owned and disposed by the <see cref="IMeterFactory"/> that created it.
    /// </summary>
    public Meter Meter { get; }

    public Counter<int> CatchupEventsProcessed { get; }
    public Counter<int> CatchUpNotificationsProcessed { get; }
    public Counter<int> CatchUpCheckpoints { get; }
    public Gauge<byte> CatchUpLive { get; }
    public Histogram<long> CatchupEventYieldLatency { get; }
    public Histogram<long> CatchupNotificationYieldLatency { get; }
    public Histogram<long> CatchupCheckpointYieldLatency { get; }

    public Histogram<long> EventDeserializationLatency { get; }
    public Histogram<long> EventSerializationLatency { get; }

    public Histogram<long> StateDeserializationLatency { get; }
    public Histogram<long> StateSerializationLatency { get; }

    public Counter<int> Retries { get; }
}
