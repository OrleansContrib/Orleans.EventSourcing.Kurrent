using LoadTest.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Load-tests the Kurrent reminder table using dedicated <see cref="IReminderLoadGrain"/>s.
///
/// 1. Registration ramp: registers reminders on new grains at a stepped rate. Every registration
///    is an upsert on the single reminder table grain, so this finds the reminder table's write ceiling.
/// 2. Observation: holds for <c>--observe</c> seconds while the registered reminders tick, then reports
///    delivered ticks/sec, missed ticks and lateness, bucketed per reminder period.
/// 3. Cleanup: unregisters every reminder (skip with <c>--no-cleanup</c>) so they don't keep firing
///    in later runs.
///
/// Tick times come from the silo clock and are bucketed against the client clock, so run the client
/// and silos on the same machine or on clock-synchronised hosts.
/// </summary>
internal static class RemindersScenario
{
    public static async Task<int> RunAsync(IClusterClient client, CommandLine args, CancellationToken cancellationToken)
    {
        var period = TimeSpan.FromSeconds(args.GetInt("period", 60));
        var observe = TimeSpan.FromSeconds(args.GetInt("observe", (int)period.TotalSeconds * 3));
        var cleanup = !args.Has("no-cleanup");
        var ramp = RampOptions.Parse(args, new RampOptions { StartRate = 20, StepRate = 20, StepSeconds = 30, MaxSteps = 10, MaxErrorRate = 0 });

        var runId = Guid.NewGuid().ToString("N")[..8];
        Console.WriteLine($"Reminders scenario: run={runId} period={period.TotalSeconds:N0}s observe={observe.TotalSeconds:N0}s cleanup={cleanup}");

        var runStart = DateTime.UtcNow;
        var attempted = new ConcurrentBag<string>();
        var registered = new ConcurrentDictionary<string, DateTime>();

        // --- 1. Registration ramp. The first tick is spread uniformly over (period, 2 x period] so
        // the tick load is even rather than arriving in waves aligned to each step.
        Console.WriteLine();
        Console.WriteLine("Phase 1: reminder registration ramp");
        await RampRunner.RunAsync("regs", ramp, async (sequence, _) =>
        {
            var key = $"{runId}/{sequence}";
            attempted.Add(key);
            var dueTime = period + period * Random.Shared.NextDouble();
            await client.GetGrain<IReminderLoadGrain>(key).Register(dueTime, period);
            registered[key] = DateTime.UtcNow + dueTime;
            return 1;
        }, cancellationToken);

        Console.WriteLine($"Registered {registered.Count:N0} reminders ({attempted.Count - registered.Count:N0} failed); expected tick rate at full load {registered.Count / period.TotalSeconds:N1}/s");

        // --- 2. Observe ticks.
        Console.WriteLine();
        Console.WriteLine($"Phase 2: observing reminder ticks for {observe.TotalSeconds:N0}s");
        var observeWatch = Stopwatch.StartNew();
        while (observeWatch.Elapsed < observe && !cancellationToken.IsCancellationRequested)
        {
            var remaining = observe - observeWatch.Elapsed;
            Console.WriteLine($"  {remaining.TotalSeconds,5:N0}s remaining");
            try
            {
                await Task.Delay(remaining < TimeSpan.FromSeconds(15) ? remaining : TimeSpan.FromSeconds(15), cancellationToken);
            }
            catch (OperationCanceledException)
            {
            }
        }

        var collectedAt = DateTime.UtcNow;
        var stats = new ConcurrentDictionary<string, ReminderLoadStats>();
        var collectErrors = 0;
        await Parallel.ForEachAsync(registered.Keys.ToArray(), new ParallelOptions { MaxDegreeOfParallelism = 64 }, async (key, _) =>
        {
            try
            {
                stats[key] = await client.GetGrain<IReminderLoadGrain>(key).GetStats();
            }
            catch
            {
                Interlocked.Increment(ref collectErrors);
            }
        });

        var passed = Report(registered, stats, period, runStart, collectedAt, collectErrors);

        // --- 3. Cleanup.
        if (cleanup)
        {
            Console.WriteLine();
            Console.WriteLine($"Phase 3: unregistering {attempted.Count:N0} reminders");
            var cleanupWatch = Stopwatch.StartNew();
            var cleanupErrors = 0;
            var latency = new LatencyHistogram();
            await Parallel.ForEachAsync(attempted, new ParallelOptions { MaxDegreeOfParallelism = 32 }, async (key, _) =>
            {
                var opWatch = Stopwatch.StartNew();
                try
                {
                    await client.GetGrain<IReminderLoadGrain>(key).Unregister();
                    latency.Record(opWatch.Elapsed);
                }
                catch
                {
                    Interlocked.Increment(ref cleanupErrors);
                }
            });

            Console.WriteLine($"Unregistered in {cleanupWatch.Elapsed.TotalSeconds:F1}s = {attempted.Count / cleanupWatch.Elapsed.TotalSeconds:N0}/s at concurrency 32, "
                            + $"p50 {latency.PercentileMs(0.5):N1}ms p99 {latency.PercentileMs(0.99):N1}ms, {cleanupErrors} error(s)");
        }
        else
        {
            Console.WriteLine($"Skipping cleanup: reminders for run {runId} remain registered and will keep firing.");
        }

        return passed ? 0 : 1;
    }

    private static bool Report(
        IReadOnlyDictionary<string, DateTime> registered,
        IReadOnlyDictionary<string, ReminderLoadStats> stats,
        TimeSpan period,
        DateTime runStart,
        DateTime collectedAt,
        int collectErrors)
    {
        // Ticks scheduled within this grace window before collection may legitimately not have arrived yet.
        var cutoff = collectedAt - TimeSpan.FromSeconds(10);
        long expected = 0, distinct = 0, duplicates = 0, neverFired = 0;
        var overall = new LatencyHistogram();
        var windows = new SortedDictionary<int, (long Ticks, LatencyHistogram Lateness)>();

        foreach (var (key, firstDue) in registered)
        {
            if (!stats.TryGetValue(key, out var stat))
            {
                continue;
            }

            var firstTick = stat.FirstTickUtc ?? firstDue;
            if (cutoff >= firstTick)
            {
                expected += (long)((cutoff - firstTick) / period) + 1;
            }

            if (stat.Ticks.Count == 0 && cutoff >= firstDue)
            {
                neverFired++;
            }

            var seen = new HashSet<long>();
            foreach (var tick in stat.Ticks)
            {
                if (!seen.Add(tick.Index))
                {
                    duplicates++;
                    continue;
                }

                if (firstTick + period * tick.Index > cutoff)
                {
                    continue;
                }

                distinct++;
                overall.RecordMs(tick.LatenessMs);
                var window = (int)((tick.ReceivedUtc - runStart) / period);
                if (!windows.TryGetValue(window, out var bucket))
                {
                    windows[window] = bucket = (0, new LatencyHistogram());
                }

                bucket.Lateness.RecordMs(tick.LatenessMs);
                windows[window] = (bucket.Ticks + 1, bucket.Lateness);
            }
        }

        Console.WriteLine();
        Console.WriteLine($"Reminder ticks per {period.TotalSeconds:N0}s window (lateness = received - scheduled):");
        Console.WriteLine($"{"window",12} {"active",8} {"ticks",8} {"ticks/s",8} {"p50",9} {"p99",9} {"max",9}");
        foreach (var (window, (ticks, lateness)) in windows)
        {
            var windowEnd = runStart + period * (window + 1);
            var active = registered.Values.Count(due => due <= windowEnd);
            var partial = windowEnd > cutoff ? "*" : "";
            Console.WriteLine($"{$"{window * period.TotalSeconds:N0}-{(window + 1) * period.TotalSeconds:N0}s{partial}",12} {active,8:N0} {ticks,8:N0} {ticks / period.TotalSeconds,8:N1} {lateness.PercentileMs(0.5),7:N0}ms {lateness.PercentileMs(0.99),7:N0}ms {lateness.MaxMs,7:N0}ms");
        }

        var missed = Math.Max(0, expected - distinct);
        Console.WriteLine("  * window extends past the end of observation, so its tick count is partial");
        Console.WriteLine();
        Console.WriteLine($"Ticks: expected {expected:N0}, delivered {distinct:N0}, missed {missed:N0} ({(expected == 0 ? 0 : (double)missed / expected):P2}), duplicates {duplicates:N0}");
        Console.WriteLine($"Reminders that never fired: {neverFired:N0}; stats collection errors: {collectErrors}");
        Console.WriteLine($"Lateness overall: p50 {overall.PercentileMs(0.5):N0}ms p90 {overall.PercentileMs(0.9):N0}ms p99 {overall.PercentileMs(0.99):N0}ms max {overall.MaxMs:N0}ms");

        var passed = neverFired == 0 && missed == 0 && collectErrors == 0;
        Console.WriteLine(passed ? "VERIFICATION PASSED" : "VERIFICATION FAILED");
        return passed;
    }
}
