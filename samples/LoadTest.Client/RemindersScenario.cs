using LoadTest.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Load-tests the Kurrent reminder table using dedicated <see cref="IReminderLoadGrain"/>s.
///
/// 1. Preload (optional, <c>--preload</c>): registers a fixed number of reminders as fast as
///    <c>--preload-concurrency</c> allows. The minimum period is one minute, so meaningful tick
///    rates need many reminders (1,000 ticks/s = 60,000 reminders); this gets there without
///    waiting for a ramp.
/// 2. Registration ramp (skip with <c>--max-steps 0</c>): registers reminders at a stepped rate.
///    Every registration is an upsert on the single reminder table grain, so this finds the reminder
///    table's write ceiling.
/// 3. Observation: holds for <c>--observe</c> seconds while the reminders tick, optionally killing a
///    silo <c>--kill-silo-after</c> seconds in, then reports delivered ticks/sec, lateness and missed
///    ticks per reminder period, and any reminders that never fired or stopped firing.
/// 4. Cleanup: unregisters every reminder (skip with <c>--no-cleanup</c>) so they don't keep firing
///    in later runs.
/// </summary>
internal static class RemindersScenario
{
    public static async Task<int> RunAsync(IClusterClient client, CommandLine args, CancellationToken cancellationToken)
    {
        var period = TimeSpan.FromSeconds(args.GetInt("period", 60));
        var observe = TimeSpan.FromSeconds(args.GetInt("observe", (int)period.TotalSeconds * 3));
        var preload = args.GetInt("preload", 0);
        var preloadConcurrency = args.GetInt("preload-concurrency", 64);
        var killAfter = args.GetInt("kill-silo-after", 0);
        var cleanup = !args.Has("no-cleanup");
        var ramp = RampOptions.Parse(args, new RampOptions { StartRate = 20, StepRate = 20, StepSeconds = 30, MaxSteps = 10, MaxErrorRate = 0 });

        using var reminders = new ReminderWorkload(client, period);
        Console.WriteLine($"Reminders scenario: run={reminders.RunId} period={period.TotalSeconds:N0}s observe={observe.TotalSeconds:N0}s preload={preload:N0} "
                        + $"kill-silo-after={(killAfter > 0 ? $"{killAfter}s" : "off")} cleanup={cleanup}");

        // --- 1. Preload.
        if (preload > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"Phase 1: preloading {preload:N0} reminders at concurrency {preloadConcurrency}");
            await reminders.PreloadAsync(preload, preloadConcurrency, cancellationToken);
        }

        // --- 2. Registration ramp.
        if (ramp.MaxSteps > 0 && !cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine();
            Console.WriteLine("Phase 2: reminder registration ramp");
            await RampRunner.RunAsync("regs", ramp, async (sequence, _) =>
            {
                await reminders.RegisterAsync($"r{sequence}");
                return 1;
            }, cancellationToken);
        }

        Console.WriteLine($"Registered {reminders.Registered:N0} reminders ({reminders.Attempted - reminders.Registered:N0} failed); steady-state tick rate {reminders.Registered / period.TotalSeconds:N1}/s");

        // --- 3. Observe ticks.
        Console.WriteLine();
        Console.WriteLine($"Phase 3: observing reminder ticks for {observe.TotalSeconds:N0}s");
        var disruptions = new List<DateTime>();
        var killed = false;
        var observeWatch = Stopwatch.StartNew();
        var lastTicks = reminders.Collector.TotalTicks;
        reminders.Collector.TakeInterval();
        while (observeWatch.Elapsed < observe && !cancellationToken.IsCancellationRequested)
        {
            var wait = TimeSpan.FromSeconds(Math.Min(15, (observe - observeWatch.Elapsed).TotalSeconds));
            if (killAfter > 0 && !killed)
            {
                wait = TimeSpan.FromSeconds(Math.Max(0, Math.Min(wait.TotalSeconds, killAfter - observeWatch.Elapsed.TotalSeconds)));
            }

            try
            {
                await Task.Delay(wait, cancellationToken);
            }
            catch (OperationCanceledException)
            {
            }

            if (killAfter > 0 && !killed && observeWatch.Elapsed.TotalSeconds >= killAfter)
            {
                killed = true;
                if (await KillSiloAsync(client))
                {
                    disruptions.Add(DateTime.UtcNow);
                }
            }

            var ticks = reminders.Collector.TotalTicks;
            var lateness = reminders.Collector.TakeInterval();
            Console.WriteLine($"  t+{observeWatch.Elapsed.TotalSeconds,4:N0}s  ticks {ticks - lastTicks,7:N0} ({(ticks - lastTicks) / Math.Max(1, wait.TotalSeconds),7:N1}/s)  lateness p50 {lateness.PercentileMs(0.5),6:N0}ms p99 {lateness.PercentileMs(0.99),6:N0}ms max {lateness.MaxMs,6:N0}ms");
            lastTicks = ticks;
        }

        var passed = reminders.Report(DateTime.UtcNow, disruptions);

        // --- 4. Cleanup.
        if (cleanup)
        {
            await reminders.CleanupAsync();
        }
        else
        {
            Console.WriteLine($"Skipping cleanup: reminders for run {reminders.RunId} remain registered and will keep firing.");
        }

        return passed ? 0 : 1;
    }

    private static async Task<bool> KillSiloAsync(IClusterClient client)
    {
        var hosts = await client.GetGrain<IManagementGrain>(0).GetHosts(onlyActive: true);
        if (hosts.Count < 2)
        {
            Console.WriteLine($"  Not killing a silo: only {hosts.Count} active silo(s). Start at least two silos to test failover.");
            return false;
        }

        var silo = await client.GetGrain<ISiloKillerGrain>(Guid.NewGuid()).Kill();
        Console.WriteLine($"  KILLED silo {silo} ({hosts.Count - 1} remain)");
        return true;
    }
}

/// <summary>
/// A set of <see cref="IReminderLoadGrain"/> reminders for one run, with a client-side
/// <see cref="TickCollector"/> receiving their ticks, plus verification and cleanup.
/// </summary>
internal sealed class ReminderWorkload : IDisposable
{
    private readonly IClusterClient client;
    private readonly TimeSpan period;
    private readonly DateTime runStart = DateTime.UtcNow;
    private readonly IReminderTickObserver observer;
    private readonly ConcurrentBag<string> attempted = [];
    private readonly ConcurrentDictionary<string, DateTime> registered = new();

    public ReminderWorkload(IClusterClient client, TimeSpan period)
    {
        this.client = client;
        this.period = period;
        Collector = new TickCollector(runStart, period);
        observer = client.CreateObjectReference<IReminderTickObserver>(Collector);
    }

    public string RunId { get; } = Guid.NewGuid().ToString("N")[..8];

    public TickCollector Collector { get; }

    public int Attempted => attempted.Count;

    public int Registered => registered.Count;

    /// <summary>
    /// Registers a reminder on a new grain. The first tick is spread uniformly over (period, 2 x period]
    /// so ticks arrive evenly rather than in waves aligned to registration bursts.
    /// </summary>
    public async Task RegisterAsync(string name)
    {
        var key = $"{RunId}/{name}";
        attempted.Add(key);
        var dueTime = period + period * Random.Shared.NextDouble();
        await client.GetGrain<IReminderLoadGrain>(key).Register(dueTime, period, observer);
        registered[key] = DateTime.UtcNow + dueTime;
    }

    public async Task PreloadAsync(int count, int concurrency, CancellationToken cancellationToken)
    {
        var latency = new LatencyHistogram();
        var errors = 0;
        var watch = Stopwatch.StartNew();
        var progress = Stopwatch.StartNew();
        try
        {
            await Parallel.ForAsync(0, count, new ParallelOptions { MaxDegreeOfParallelism = concurrency, CancellationToken = cancellationToken }, async (i, _) =>
            {
                var opWatch = Stopwatch.StartNew();
                try
                {
                    await RegisterAsync($"p{i}");
                    latency.Record(opWatch.Elapsed);
                }
                catch
                {
                    Interlocked.Increment(ref errors);
                }

                if (progress.Elapsed > TimeSpan.FromSeconds(15))
                {
                    progress.Restart();
                    Console.WriteLine($"  {registered.Count:N0}/{count:N0} registered, {registered.Count / watch.Elapsed.TotalSeconds:N0}/s");
                }
            });
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("  Preload cancelled.");
        }

        Console.WriteLine($"Preloaded {registered.Count:N0} reminders in {watch.Elapsed.TotalSeconds:F1}s = {registered.Count / watch.Elapsed.TotalSeconds:N0}/s, "
                        + $"p50 {latency.PercentileMs(0.5):N0}ms p99 {latency.PercentileMs(0.99):N0}ms, {errors} error(s)");
    }

    /// <summary>
    /// Prints tick delivery per period window and verifies no reminder was lost. Reminders are
    /// best-effort per tick: occurrences can be dropped while a failed silo's reminders are reassigned,
    /// so when <paramref name="disruptions"/> is non-empty only lost reminders (never fired / stopped
    /// firing) fail verification.
    /// </summary>
    public bool Report(DateTime reportAt, IReadOnlyList<DateTime> disruptions)
    {
        // Occurrences scheduled within this grace window before the report may legitimately not have arrived yet.
        var cutoff = reportAt - TimeSpan.FromSeconds(10);
        long expected = 0, delivered = 0, neverFired = 0, stoppedFiring = 0;

        foreach (var (key, estimatedFirstDue) in registered)
        {
            if (!Collector.Reminders.TryGetValue(key, out var state))
            {
                if (cutoff >= estimatedFirstDue)
                {
                    neverFired++;
                    expected += (long)((cutoff - estimatedFirstDue) / period) + 1;
                }

                continue;
            }

            lock (state)
            {
                // Count expected and delivered occurrences up to the same cutoff.
                if (cutoff >= state.FirstTickUtc)
                {
                    var lastExpected = (long)((cutoff - state.FirstTickUtc) / period);
                    expected += lastExpected + 1;
                    delivered += state.Indices.Count(index => index <= lastExpected);
                }

                if (state.LastReceivedClientUtc < reportAt - period * 2)
                {
                    stoppedFiring++;
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine($"Reminder ticks per {period.TotalSeconds:N0}s window (lateness = received - scheduled occurrence):");
        Console.WriteLine($"{"window",13} {"active",8} {"ticks",8} {"ticks/s",8} {"p50",9} {"p99",9} {"max",9}");
        foreach (var (window, stats) in Collector.Windows.OrderBy(w => w.Key))
        {
            var windowStart = runStart + period * window;
            var windowEnd = windowStart + period;
            var active = registered.Values.Count(due => due <= windowEnd);
            var marker = (windowEnd > reportAt ? "*" : "") + (disruptions.Any(d => d >= windowStart && d < windowEnd) ? "D" : "");
            Console.WriteLine($"{$"{window * period.TotalSeconds:N0}-{(window + 1) * period.TotalSeconds:N0}s{marker}",13} {active,8:N0} {stats.Ticks,8:N0} {stats.Ticks / period.TotalSeconds,8:N1} {stats.Lateness.PercentileMs(0.5),7:N0}ms {stats.Lateness.PercentileMs(0.99),7:N0}ms {stats.Lateness.MaxMs,7:N0}ms");
        }

        Console.WriteLine("  * window extends past the end of observation, so its tick count is partial; D = disruption (silo or Kurrent node stopped) in this window");

        var missed = Math.Max(0, expected - delivered);
        var lateness = Collector.Lateness;
        Console.WriteLine();
        Console.WriteLine($"Ticks: expected {expected:N0}, delivered {delivered:N0}, missed {missed:N0} ({(expected == 0 ? 0 : (double)missed / expected):P2}), duplicates {Collector.Duplicates:N0}");
        Console.WriteLine($"Reminders: never fired {neverFired:N0}, stopped firing (nothing in the last 2 periods) {stoppedFiring:N0}");
        Console.WriteLine($"Lateness overall: p50 {lateness.PercentileMs(0.5):N0}ms p90 {lateness.PercentileMs(0.9):N0}ms p99 {lateness.PercentileMs(0.99):N0}ms max {lateness.MaxMs:N0}ms");

        var disrupted = disruptions.Count > 0;
        var passed = neverFired == 0 && stoppedFiring == 0 && (disrupted || missed == 0);
        Console.WriteLine(passed
            ? "REMINDER VERIFICATION PASSED"
            : $"REMINDER VERIFICATION FAILED{(disrupted ? "" : " (missed ticks without a disruption)")}");
        return passed;
    }

    public async Task CleanupAsync()
    {
        var keys = attempted.ToArray();
        Console.WriteLine();
        Console.WriteLine($"Unregistering {keys.Length:N0} reminders");
        var watch = Stopwatch.StartNew();
        var errors = new ConcurrentDictionary<string, int>();
        var latency = new LatencyHistogram();
        await Parallel.ForEachAsync(keys, new ParallelOptions { MaxDegreeOfParallelism = 32 }, async (key, _) =>
        {
            var opWatch = Stopwatch.StartNew();
            try
            {
                await client.GetGrain<IReminderLoadGrain>(key).Unregister();
                latency.Record(opWatch.Elapsed);
            }
            catch (Exception ex)
            {
                var root = ex.GetBaseException();
                errors.AddOrUpdate($"{root.GetType().Name}: {(root.Message.Length <= 120 ? root.Message : root.Message[..120] + "...")}", 1, (_, count) => count + 1);
            }
        });

        Console.WriteLine($"Unregistered in {watch.Elapsed.TotalSeconds:F1}s = {keys.Length / watch.Elapsed.TotalSeconds:N0}/s at concurrency 32, "
                        + $"p50 {latency.PercentileMs(0.5):N1}ms p99 {latency.PercentileMs(0.99):N1}ms, {errors.Values.Sum()} error(s)");
        foreach (var (error, count) in errors.OrderByDescending(e => e.Value).Take(3))
        {
            Console.WriteLine($"  {count,5:N0} x {error}");
        }
    }

    public void Dispose() => client.DeleteObjectReference<IReminderTickObserver>(observer);
}
