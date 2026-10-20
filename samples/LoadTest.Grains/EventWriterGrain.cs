using Orleans.EventSourcing;

namespace LoadTest.Grains;

[GenerateSerializer]
[Alias("LoadTest.Grains.EventWritten")]
public sealed record EventWritten([property: Id(0)] byte[] Payload);

[GenerateSerializer]
[Alias("LoadTest.Grains.EventWriterState")]
public sealed record EventWriterState
{
    [Id(0)]
    public long PayloadBytes { get; set; }

    public void Apply(EventWritten written) => PayloadBytes += written.Payload.Length;
}

/// <summary>
/// A minimal event-sourced grain used to measure raw event write throughput through the Kurrent
/// log-consistency provider. Each <see cref="Append"/> call raises a batch of events and confirms
/// them as a single append to the grain's Kurrent stream.
/// </summary>
[Alias("LoadTest.Grains.IEventWriterGrain")]
public interface IEventWriterGrain : IGrainWithGuidKey
{
    /// <summary>Raises <paramref name="eventCount"/> events and confirms them; returns the confirmed version.</summary>
    [Alias("Append")]
    Task<int> Append(int eventCount, int payloadBytes);

    [Alias("GetConfirmedVersion")]
    Task<int> GetConfirmedVersion();
}

public sealed class EventWriterGrain : JournaledGrain<EventWriterState, EventWritten>, IEventWriterGrain
{
    public async Task<int> Append(int eventCount, int payloadBytes)
    {
        var events = new EventWritten[eventCount];
        for (var i = 0; i < events.Length; i++)
        {
            events[i] = new EventWritten(new byte[payloadBytes]);
        }

        RaiseEvents(events);
        await ConfirmEvents();
        return Version;
    }

    public Task<int> GetConfirmedVersion() => Task.FromResult(Version);
}
