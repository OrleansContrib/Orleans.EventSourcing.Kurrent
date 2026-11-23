using KurrentDB.Client;
using LoadTest.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Hosting;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Two clusters write to the same grains, so their conditional writes reject each other under load.
///
/// The clusters share the KurrentDB but have different cluster IDs, so each has its own activation of
/// every <see cref="IChainGrain"/>, writing to the same stream. Each event names the event it was
/// decided after, and workers in both clusters append to a small set of grains concurrently. The grain
/// is reentrant, so writes queue behind writes in flight, and a write the other cluster beat is rejected
/// with writes queued behind it.
///
/// Afterwards every stream is read and checked:
///   - each event follows the event it was decided after (a write decided on a tentative state that
///     included a rejected write must not be stored);
///   - every write reported as written is stored, exactly once;
///   - no write reported as rejected is stored.
/// A run with no rejections fails, because then nothing was contended.
/// </summary>
internal static class ContentionScenario
{
    public const string ClusterA = "contention-a";
    public const string ClusterB = "contention-b";

    public static async Task<int> RunAsync(IClusterClient clientA, CommandLine args, string connectionString, CancellationToken cancellationToken)
    {
        var grainCount = args.GetInt("grains", 20);
        var workers = args.GetInt("workers", 64);
        var duration = TimeSpan.FromSeconds(args.GetInt("duration", 60));
        var reportInterval = TimeSpan.FromSeconds(args.GetInt("report-interval", 5));

        using var hostB = await ConnectAsync(ClusterB, connectionString, cancellationToken);
        var clientB = hostB.Services.GetRequiredService<IClusterClient>();
        Console.WriteLine($"Connected to {ClusterB}.");

        Console.WriteLine($"Contention scenario: grains={grainCount:N0} workers={workers} per cluster, duration={duration.TotalSeconds:N0}s");
        var keys = Enumerable.Range(0, grainCount).Select(_ => Guid.NewGuid()).ToArray();
        var writers = new[]
        {
            new Writer(ClusterA, keys.Select(k => clientA.GetGrain<IChainGrain>(k)).ToArray()),
            new Writer(ClusterB, keys.Select(k => clientB.GetGrain<IChainGrain>(k)).ToArray()),
        };
        var outcomes = new ConcurrentDictionary<Guid, Outcome>();

        // Activate every grain in both clusters before the clock starts.
        foreach (var writer in writers)
        {
            await Parallel.ForEachAsync(writer.Grains, cancellationToken, async (grain, _) => await grain.GetChain());
        }

        Console.WriteLine();
        Console.WriteLine($"{"time",6} | {"cluster",-12} {"written/s",10} {"rejected/s",10} {"failed/s",9} {"p50 ms",7} {"p99 ms",7}");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(duration);
        var clock = Stopwatch.StartNew();
        var load = writers.SelectMany(writer => Enumerable.Range(0, workers).Select(_ => RunWorkerAsync(writer, outcomes, stop.Token))).ToArray();
        var report = ReportAsync(writers, clock, reportInterval, stop.Token);
        await Task.WhenAll(load);
        await report;
        Console.WriteLine();

        foreach (var writer in writers)
        {
            Console.WriteLine($"{writer.Cluster}: written {writer.Totals.Written:N0}, rejected {writer.Totals.Rejected:N0}, failed {writer.Totals.Failed:N0}, "
                            + $"p50 {writer.Latency.PercentileMs(0.5):N1}ms p99 {writer.Latency.PercentileMs(0.99):N1}ms max {writer.Latency.MaxMs:N0}ms");
            foreach (var (error, count) in writer.Errors.OrderByDescending(e => e.Value).Take(5))
            {
                Console.WriteLine($"  {count:N0} x {error}");
            }
        }

        var passed = await VerifyAsync(writers, keys, outcomes);
        await hostB.StopAsync(CancellationToken.None);
        return passed ? 0 : 1;
    }

    private static async Task<IHost> ConnectAsync(string clusterId, string connectionString, CancellationToken cancellationToken)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.AddFilter("Orleans", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
        builder.UseOrleansClient(client =>
        {
            client.Configure<ClusterOptions>(o => o.ClusterId = clusterId);
            client.UseKurrentClustering(x => x.ClientSettings = KurrentDBClientSettings.Create(connectionString));
        });

        var host = builder.Build();
        await host.StartAsync(cancellationToken);
        return host;
    }

    private static async Task RunWorkerAsync(Writer writer, ConcurrentDictionary<Guid, Outcome> outcomes, CancellationToken cancellationToken)
    {
        await Task.Yield();
        while (!cancellationToken.IsCancellationRequested)
        {
            var index = Random.Shared.Next(writer.Grains.Length);
            var id = Guid.NewGuid();
            var started = Stopwatch.GetTimestamp();
            try
            {
                var result = await writer.Grains[index].Append(id, writer.Cluster);
                writer.Record(Stopwatch.GetElapsedTime(started));
                if (result == ChainAppendResult.Written)
                {
                    outcomes[id] = new Outcome(index, OutcomeKind.Written);
                    writer.Count(OutcomeKind.Written);
                }
                else
                {
                    outcomes[id] = new Outcome(index, OutcomeKind.Rejected);
                    writer.Count(OutcomeKind.Rejected);
                }
            }
            catch (Exception ex)
            {
                // The write may or may not have been stored.
                outcomes[id] = new Outcome(index, OutcomeKind.Failed);
                writer.Count(OutcomeKind.Failed);
                var error = ex.GetBaseException();
                writer.Errors.AddOrUpdate($"{error.GetType().Name}: {error.Message}", 1, (_, n) => n + 1);
            }
        }
    }

    private static async Task ReportAsync(Writer[] writers, Stopwatch clock, TimeSpan interval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                foreach (var writer in writers)
                {
                    var ((written, rejected, failed), latency) = writer.TakeInterval();
                    var seconds = interval.TotalSeconds;
                    Console.WriteLine($"{clock.Elapsed.TotalSeconds,5:N0}s | {writer.Cluster,-12} {written / seconds,10:N0} {rejected / seconds,10:N0} {failed / seconds,9:N0} "
                                    + $"{latency.PercentileMs(0.5),7:N1} {latency.PercentileMs(0.99),7:N1}");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task<bool> VerifyAsync(Writer[] writers, Guid[] keys, ConcurrentDictionary<Guid, Outcome> outcomes)
    {
        Console.WriteLine();
        Console.WriteLine("Verifying every stream...");
        var brokenLinks = 0;
        var rejectedButStored = 0;
        var writtenButMissing = 0;
        var duplicates = 0;
        var unknown = 0;
        var disagreements = 0;
        var errors = 0;
        var stored = 0L;
        var problems = new ConcurrentQueue<string>();

        void Problem(ref int counter, string message)
        {
            Interlocked.Increment(ref counter);
            problems.Enqueue(message);
        }

        var written = outcomes.Where(o => o.Value.Kind == OutcomeKind.Written).ToLookup(o => o.Value.Grain, o => o.Key);
        await Parallel.ForAsync(0, keys.Length, new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (i, _) =>
        {
            IReadOnlyList<Linked> chain;
            IReadOnlyList<Linked> other;
            try
            {
                chain = await writers[0].Grains[i].GetChain();
                other = await writers[1].Grains[i].GetChain();
            }
            catch (Exception ex)
            {
                Problem(ref errors, $"ERROR reading {keys[i]}: {ex.GetBaseException().Message}");
                return;
            }

            Interlocked.Add(ref stored, chain.Count);
            if (!chain.Select(e => e.Id).SequenceEqual(other.Select(e => e.Id)))
            {
                Problem(ref disagreements, $"DISAGREE {keys[i]}: {writers[0].Cluster} reads {chain.Count:N0} events, {writers[1].Cluster} reads {other.Count:N0}");
            }

            var seen = new HashSet<Guid>();
            var previous = Guid.Empty;
            for (var position = 0; position < chain.Count; position++)
            {
                var e = chain[position];
                if (e.Prev != previous)
                {
                    Problem(ref brokenLinks, $"BROKEN LINK {keys[i]} at {position}: {e.Writer} wrote {e.Id} after {e.Prev}, but it follows {previous}");
                }

                previous = e.Id;
                if (!seen.Add(e.Id))
                {
                    Problem(ref duplicates, $"DUPLICATE {keys[i]} at {position}: {e.Id}");
                }

                if (!outcomes.TryGetValue(e.Id, out var outcome))
                {
                    Problem(ref unknown, $"UNKNOWN {keys[i]} at {position}: {e.Id} was never sent");
                }
                else if (outcome.Kind == OutcomeKind.Rejected)
                {
                    Problem(ref rejectedButStored, $"REJECTED BUT STORED {keys[i]} at {position}: {e.Writer} {e.Id}");
                }
            }

            foreach (var id in written[i].Where(id => !seen.Contains(id)))
            {
                Problem(ref writtenButMissing, $"WRITTEN BUT MISSING {keys[i]}: {id}");
            }
        });

        foreach (var problem in problems.Take(20))
        {
            Console.WriteLine($"  {problem}");
        }

        if (problems.Count > 20)
        {
            Console.WriteLine($"  ... and {problems.Count - 20:N0} more");
        }

        var totals = writers.Select(w => w.Totals).ToArray();
        var rejected = totals.Sum(t => t.Rejected);
        Console.WriteLine($"Stored events: {stored:N0} (written {totals.Sum(t => t.Written):N0}, rejected {rejected:N0}, failed/unknown {totals.Sum(t => t.Failed):N0})");
        Console.WriteLine($"Broken links {brokenLinks}, rejected but stored {rejectedButStored}, written but missing {writtenButMissing}, duplicates {duplicates}, "
                        + $"never sent {unknown}, clusters disagree {disagreements}, unreadable {errors}");

        var passed = brokenLinks + rejectedButStored + writtenButMissing + duplicates + unknown + disagreements + errors == 0;
        if (rejected == 0)
        {
            Console.WriteLine("CONTENTION VERIFICATION FAILED: no write was rejected, so the clusters never contended. Use fewer grains or more workers.");
            return false;
        }

        Console.WriteLine(passed ? "CONTENTION VERIFICATION PASSED" : "CONTENTION VERIFICATION FAILED");
        return passed;
    }

    private enum OutcomeKind
    {
        Written,
        Rejected,
        Failed,
    }

    private readonly record struct Outcome(int Grain, OutcomeKind Kind);

    private readonly record struct Counts(long Written, long Rejected, long Failed);

    private sealed class Writer(string cluster, IChainGrain[] grains)
    {
        private long written;
        private long rejected;
        private long failed;
        private Counts reported;
        private LatencyHistogram intervalLatency = new();

        public string Cluster { get; } = cluster;

        public IChainGrain[] Grains { get; } = grains;

        /// <summary>Every call that returned, over the whole run.</summary>
        public LatencyHistogram Latency { get; } = new();

        public void Record(TimeSpan latency)
        {
            Latency.Record(latency);
            Volatile.Read(ref intervalLatency).Record(latency);
        }

        public ConcurrentDictionary<string, int> Errors { get; } = new();

        public Counts Totals => new(Interlocked.Read(ref written), Interlocked.Read(ref rejected), Interlocked.Read(ref failed));

        public void Count(OutcomeKind kind)
        {
            switch (kind)
            {
                case OutcomeKind.Written:
                    Interlocked.Increment(ref written);
                    break;
                case OutcomeKind.Rejected:
                    Interlocked.Increment(ref rejected);
                    break;
                default:
                    Interlocked.Increment(ref failed);
                    break;
            }
        }

        /// <summary>The counts and latencies since the last call; only the reporting loop calls this.</summary>
        public (Counts Counts, LatencyHistogram Latency) TakeInterval()
        {
            var latency = Interlocked.Exchange(ref intervalLatency, new LatencyHistogram());
            var now = Totals;
            var interval = new Counts(now.Written - reported.Written, now.Rejected - reported.Rejected, now.Failed - reported.Failed);
            reported = now;
            return (interval, latency);
        }
    }
}
