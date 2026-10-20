namespace LoadTest.Grains;

[GenerateSerializer]
[Alias("LoadTest.Grains.ReminderTick")]
public sealed record ReminderTick(
    [property: Id(0)] long Index,
    [property: Id(1)] DateTime ReceivedUtc,
    [property: Id(2)] double LatenessMs);

[GenerateSerializer]
[Alias("LoadTest.Grains.ReminderLoadStats")]
public sealed record ReminderLoadStats(
    [property: Id(0)] DateTime? FirstTickUtc,
    [property: Id(1)] TimeSpan Period,
    [property: Id(2)] IReadOnlyList<ReminderTick> Ticks);

/// <summary>
/// A plain (non event-sourced) grain that owns a single reminder. Keeping the tick handler free of
/// storage writes isolates the cost of the Kurrent reminder table from the log-consistency provider:
/// registrations measure reminder-table write throughput, ticks measure delivery rate and lateness.
/// </summary>
[Alias("LoadTest.Grains.IReminderLoadGrain")]
public interface IReminderLoadGrain : IGrainWithStringKey
{
    [Alias("Register")]
    Task Register(TimeSpan dueTime, TimeSpan period);

    [Alias("Unregister")]
    Task Unregister();

    [Alias("GetStats")]
    Task<ReminderLoadStats> GetStats();
}

public sealed class ReminderLoadGrain : Grain, IReminderLoadGrain, IRemindable
{
    private const string ReminderName = "load";
    private readonly List<ReminderTick> ticks = [];
    private IGrainReminder? reminder;
    private DateTime? firstTickUtc;
    private TimeSpan period;

    public async Task Register(TimeSpan dueTime, TimeSpan period)
    {
        reminder = await this.RegisterOrUpdateReminder(ReminderName, dueTime, period);
    }

    public async Task Unregister()
    {
        if ((reminder ?? await this.GetReminder(ReminderName)) is { } existing)
        {
            await this.UnregisterReminder(existing);
            reminder = null;
        }

        DeactivateOnIdle();
    }

    public Task<ReminderLoadStats> GetStats() => Task.FromResult(new ReminderLoadStats(firstTickUtc, period, ticks.ToArray()));

    public Task ReceiveReminder(string reminderName, TickStatus status)
    {
        var now = DateTime.UtcNow;
        firstTickUtc = status.FirstTickTime.ToUniversalTime();
        period = status.Period;

        // Lateness is measured against the most recent scheduled tick: FirstTickTime + k * Period.
        var sinceFirst = now - firstTickUtc.Value;
        var index = period > TimeSpan.Zero && sinceFirst > TimeSpan.Zero ? (long)(sinceFirst / period) : 0;
        var scheduled = firstTickUtc.Value + period * index;
        // Ticks are kept in memory only: each tick is a grain call, so while the period is well below
        // the idle collection age the activation (and what it recorded) stays alive for the run.
        ticks.Add(new ReminderTick(index, now, Math.Max(0, (now - scheduled).TotalMilliseconds)));
        return Task.CompletedTask;
    }
}
