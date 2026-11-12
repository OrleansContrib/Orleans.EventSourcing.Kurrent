using LoadTest.Grains;
using System.Collections.Concurrent;

namespace LoadTest.Client;

/// <summary>
/// Client-side record of every reminder tick, fed by <see cref="IReminderLoadGrain"/>s through a
/// one-way observer. Orleans schedules ticks at FirstTickTime + k * Period and never replays an
/// occurrence: if a delivery overruns, or a reminder's owning silo dies, the occurrences in between
/// are skipped and the schedule resumes at the next boundary. So each tick is matched to the latest
/// occurrence due when it arrived, lateness is measured against that occurrence, and occurrences
/// that never arrived show up as missed. Lateness compares two silo-clock times, so it is unaffected
/// by client/silo clock skew.
/// </summary>
internal sealed class TickCollector(DateTime runStartUtc, TimeSpan window) : IReminderTickObserver
{
    private readonly ConcurrentDictionary<string, ReminderState> reminders = new();
    private readonly ConcurrentDictionary<int, WindowStats> windows = new();
    private LatencyHistogram interval = new();
    private long totalTicks;
    private long duplicates;

    public LatencyHistogram Lateness { get; } = new();

    public long TotalTicks => Interlocked.Read(ref totalTicks);

    public long Duplicates => Interlocked.Read(ref duplicates);

    public IReadOnlyDictionary<string, ReminderState> Reminders => reminders;

    public IReadOnlyDictionary<int, WindowStats> Windows => windows;

    public TimeSpan Window => window;

    /// <summary>Returns lateness for ticks received since the previous call.</summary>
    public LatencyHistogram TakeInterval() => Interlocked.Exchange(ref interval, new LatencyHistogram());

    public Task OnTick(string key, DateTime firstTickUtc, TimeSpan period, DateTime receivedUtc)
    {
        var clientNow = DateTime.UtcNow;
        var state = reminders.GetOrAdd(key, _ => new ReminderState());
        double latenessMs;
        lock (state)
        {
            state.FirstTickUtc = firstTickUtc;
            state.Period = period;
            state.LastReceivedClientUtc = clientNow;

            var latestDue = receivedUtc <= firstTickUtc ? 0 : (long)((receivedUtc - firstTickUtc) / period);
            if (state.Indices.Count > 0 && latestDue <= state.Indices[^1])
            {
                // An occurrence already accounted for was delivered again (e.g. two silos briefly both owning it).
                Interlocked.Increment(ref duplicates);
                return Task.CompletedTask;
            }

            latenessMs = Math.Max(0, (receivedUtc - (firstTickUtc + period * latestDue)).TotalMilliseconds);
            state.Indices.Add(latestDue);
        }

        Interlocked.Increment(ref totalTicks);
        Lateness.RecordMs(latenessMs);
        Volatile.Read(ref interval).RecordMs(latenessMs);
        var stats = windows.GetOrAdd((int)((clientNow - runStartUtc) / window), _ => new WindowStats());
        Interlocked.Increment(ref stats.Ticks);
        stats.Lateness.RecordMs(latenessMs);
        return Task.CompletedTask;
    }

    internal sealed class ReminderState
    {
        public DateTime FirstTickUtc;
        public TimeSpan Period;
        public DateTime LastReceivedClientUtc;

        /// <summary>Delivered occurrence indices, ascending.</summary>
        public List<long> Indices { get; } = [];
    }

    internal sealed class WindowStats
    {
        public long Ticks;
        public LatencyHistogram Lateness { get; } = new();
    }
}
