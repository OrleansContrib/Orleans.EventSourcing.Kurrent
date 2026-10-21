using LoadTest.Grains;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Ramps the rate of event appends against a pool of <see cref="IEventWriterGrain"/>s to find the
/// events/sec the Kurrent log-consistency provider can sustain.
///
/// Each operation is one grain call that raises <c>--batch</c> events and confirms them as a single
/// Kurrent append, so events/sec = ops/sec x batch. Journaled grains process one call at a time, so
/// the ceiling is roughly grains / append latency: if throughput plateaus while Kurrent is idle,
/// add grains.
/// </summary>
internal static class EventsScenario
{
    public static async Task<int> RunAsync(IClusterClient client, CommandLine args, CancellationToken cancellationToken)
    {
        var workload = EventWorkload.Create(client, args);
        var ramp = RampOptions.Parse(args, new RampOptions { StartRate = 200, StepRate = 200, StepSeconds = 30 });

        Console.WriteLine($"Events scenario: {workload}");
        await workload.ActivateAsync(cancellationToken);
        Console.WriteLine();

        await RampRunner.RunAsync("events", ramp, workload.AppendAsync, cancellationToken);
        return await workload.VerifyAsync() ? 0 : 1;
    }
}

/// <summary>
/// Appends batches of events to a pool of <see cref="IEventWriterGrain"/>s, tracking acknowledged
/// appends per grain so the confirmed logs can be verified afterwards.
/// </summary>
internal sealed class EventWorkload
{
    private readonly IEventWriterGrain[] grains;
    private readonly long[] acknowledged;
    private readonly long[] unacknowledged;
    private readonly int batch;
    private readonly int payloadBytes;

    private EventWorkload(IClusterClient client, int grainCount, int batch, int payloadBytes)
    {
        grains = Enumerable.Range(0, grainCount).Select(_ => client.GetGrain<IEventWriterGrain>(Guid.NewGuid())).ToArray();
        acknowledged = new long[grainCount];
        unacknowledged = new long[grainCount];
        this.batch = batch;
        this.payloadBytes = payloadBytes;
    }

    public static EventWorkload Create(IClusterClient client, CommandLine args, int defaultGrains = 1_000)
        => new(client, args.GetInt("grains", defaultGrains), args.GetInt("batch", 1), args.GetInt("payload", 64));

    public override string ToString() => $"grains={grains.Length:N0} batch={batch} payload={payloadBytes}B";

    public async Task ActivateAsync(CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        await Parallel.ForAsync(0, grains.Length, new ParallelOptions { MaxDegreeOfParallelism = 64, CancellationToken = cancellationToken },
            async (i, _) => await grains[i].GetConfirmedVersion());
        Console.WriteLine($"Warm-up: activated {grains.Length:N0} event writer grains in {watch.Elapsed.TotalSeconds:F1}s");
    }

    public async Task<int> AppendAsync(long sequence, CancellationToken cancellationToken)
    {
        var index = Random.Shared.Next(grains.Length);
        try
        {
            await grains[index].Append(batch, payloadBytes);
            Interlocked.Add(ref acknowledged[index], batch);
            return batch;
        }
        catch
        {
            // A failed call (e.g. a timeout) may still have been appended, so it widens the accepted range.
            Interlocked.Add(ref unacknowledged[index], batch);
            throw;
        }
    }

    /// <summary>Every acknowledged event must be in the confirmed log, and nothing beyond what was attempted.</summary>
    public async Task<bool> VerifyAsync()
    {
        Console.WriteLine();
        Console.WriteLine("Verifying confirmed versions against acknowledged appends...");
        var mismatches = 0;
        var errors = 0;
        var totalConfirmed = 0L;
        await Parallel.ForAsync(0, grains.Length, new ParallelOptions { MaxDegreeOfParallelism = 64 }, async (i, _) =>
        {
            int version;
            try
            {
                version = await grains[i].GetConfirmedVersion();
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                Console.WriteLine($"  ERROR reading {grains[i].GetPrimaryKey()}: {ex.GetBaseException().Message}");
                return;
            }

            Interlocked.Add(ref totalConfirmed, version);
            if (version < acknowledged[i] || version > acknowledged[i] + unacknowledged[i])
            {
                Interlocked.Increment(ref mismatches);
                Console.WriteLine($"  MISMATCH {grains[i].GetPrimaryKey()}: confirmed {version}, acknowledged {acknowledged[i]}, failed {unacknowledged[i]}");
            }
        });

        Console.WriteLine($"Confirmed events: {totalConfirmed:N0} (acknowledged {acknowledged.Sum():N0}, failed/unknown {unacknowledged.Sum():N0})");
        var passed = mismatches == 0 && errors == 0;
        Console.WriteLine(passed ? "VERIFICATION PASSED" : $"VERIFICATION FAILED: {mismatches} mismatched grain(s), {errors} unreadable grain(s)");
        return passed;
    }
}
