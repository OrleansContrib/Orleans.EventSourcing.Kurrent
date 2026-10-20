using System.Diagnostics;

namespace LoadTest.Grains;

/// <summary>
/// Fault injection for load tests: terminates the silo process hosting this activation without a
/// graceful shutdown, so the cluster has to detect the failure and recover (membership, reminder
/// ownership, grain re-activation) the same way it would after a crash.
/// </summary>
[Alias("LoadTest.Grains.ISiloKillerGrain")]
public interface ISiloKillerGrain : IGrainWithGuidKey
{
    /// <summary>Returns the address of the silo that will be killed, then kills it shortly afterwards.</summary>
    [Alias("Kill")]
    Task<string> Kill();
}

public sealed class SiloKillerGrain(ILocalSiloDetails localSilo) : Grain, ISiloKillerGrain
{
    public Task<string> Kill()
    {
        var silo = localSilo.SiloAddress.ToString();

        // Give the response time to leave the silo before the process disappears.
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250));
            Process.GetCurrentProcess().Kill();
        });

        return Task.FromResult(silo);
    }
}
