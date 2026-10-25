using Microsoft.Extensions.Logging;
using Orleans.Concurrency;

namespace LoadTest.Grains;

/// <summary>
/// Implemented by the load test client to receive every reminder tick as it happens, so tick
/// records live outside the cluster and survive grain deactivation and silo failure.
/// </summary>
[Alias("LoadTest.Grains.IReminderTickObserver")]
public interface IReminderTickObserver : IGrainObserver
{
    /// <param name="key">The reminder load grain's key.</param>
    /// <param name="firstTickUtc">The reminder's persisted start time; ticks are scheduled at firstTickUtc + k * period.</param>
    /// <param name="period">The reminder period.</param>
    /// <param name="receivedUtc">When the tick was delivered, by the silo clock (the same clock as <paramref name="firstTickUtc"/>).</param>
    [OneWay]
    [Alias("OnTick")]
    Task OnTick(string key, DateTime firstTickUtc, TimeSpan period, DateTime receivedUtc);
}

/// <summary>
/// A plain (non event-sourced) grain that owns a single reminder. Keeping the tick handler free of
/// storage writes isolates the cost of the Kurrent reminder table from the log-consistency provider:
/// registrations measure reminder-table write throughput, ticks measure delivery rate and lateness.
///
/// The observer to report ticks to is encoded in the reminder name, which is persisted in the
/// reminder table, so a grain reactivated on another silo after a failure still reports its ticks.
/// </summary>
[Alias("LoadTest.Grains.IReminderLoadGrain")]
public interface IReminderLoadGrain : IGrainWithStringKey
{
    [Alias("Register")]
    Task Register(TimeSpan dueTime, TimeSpan period, IReminderTickObserver observer);

    [Alias("Unregister")]
    Task Unregister();
}

public sealed class ReminderLoadGrain(ILogger<ReminderLoadGrain> logger) : Grain, IReminderLoadGrain, IRemindable
{
    private const string ReminderPrefix = "load|";
    private IReminderTickObserver? observer;

    public async Task Register(TimeSpan dueTime, TimeSpan period, IReminderTickObserver observer)
    {
        this.observer = observer;
        await this.RegisterOrUpdateReminder(ReminderPrefix + observer.GetGrainId(), dueTime, period);
    }

    public async Task Unregister()
    {
        foreach (var reminder in await this.GetReminders())
        {
            await this.UnregisterReminder(reminder);
        }

        DeactivateOnIdle();
    }

    public Task ReceiveReminder(string reminderName, TickStatus status)
    {
        var receivedUtc = DateTime.UtcNow;
        if (!reminderName.StartsWith(ReminderPrefix, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        observer ??= GrainFactory.GetGrain(GrainId.Parse(reminderName[ReminderPrefix.Length..])).AsReference<IReminderTickObserver>();
        try
        {
            // One-way: the tick is not held up by the client, and delivery loss shows up as a missed tick.
            _ = observer.OnTick(this.GetPrimaryKeyString(), status.FirstTickTime.ToUniversalTime(), status.Period, receivedUtc);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to report reminder tick for {Key}", this.GetPrimaryKeyString());
        }

        return Task.CompletedTask;
    }
}
