using LoadTest.Grains;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Ramps the rate of durable state writes against a pool of <see cref="IDurableWriterGrain"/>s to find the
/// writes/sec the Kurrent journaling storage provider can sustain.
///
/// Each operation is one grain call that updates <c>--batch</c> entries of the grain's durable dictionary and
/// writes the state once, which is one append to the grain's journal stream; compaction periodically adds a
/// snapshot as well. Durable grains process one call at a time, so the ceiling is roughly grains / write latency:
/// if throughput plateaus while Kurrent is idle, add grains.
///
/// Verification deactivates every grain first, so the write counts it checks are recovered from the journal.
/// </summary>
internal static class DurableScenario
{
    public static async Task<int> RunAsync(IClusterClient client, CommandLine args, CancellationToken cancellationToken)
    {
        var workload = DurableWorkload.Create(client, args);
        var ramp = RampOptions.Parse(args, new RampOptions { StartRate = 200, StepRate = 200, StepSeconds = 30 });

        Console.WriteLine($"Durable scenario: {workload}");
        await workload.ActivateAsync(cancellationToken);
        Console.WriteLine();

        await RampRunner.RunAsync("updates", ramp, workload.WriteAsync, cancellationToken);
        return await workload.VerifyAsync() ? 0 : 1;
    }
}

/// <summary>
/// Writes durable state on a pool of <see cref="IDurableWriterGrain"/>s, tracking acknowledged writes per grain so
/// the recovered state can be verified afterwards.
/// </summary>
internal sealed class DurableWorkload
{
    private readonly IDurableWriterGrain[] grains;
    private readonly long[] acknowledged;
    private readonly long[] unacknowledged;
    private readonly int batch;
    private readonly int slots;
    private readonly int payloadBytes;

    private DurableWorkload(IClusterClient client, int grainCount, int batch, int slots, int payloadBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batch);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(slots);

        grains = Enumerable.Range(0, grainCount).Select(_ => client.GetGrain<IDurableWriterGrain>(Guid.NewGuid())).ToArray();
        acknowledged = new long[grainCount];
        unacknowledged = new long[grainCount];
        this.batch = batch;
        this.slots = slots;
        this.payloadBytes = payloadBytes;
    }

    public static DurableWorkload Create(IClusterClient client, CommandLine args, int defaultGrains = 1_000)
        => new(client, args.GetInt("grains", defaultGrains), args.GetInt("batch", 1), args.GetInt("slots", 16), args.GetInt("payload", 64));

    public override string ToString()
        => $"grains={grains.Length:N0} batch={batch} slots={slots} payload={payloadBytes}B (state ~{(long)slots * payloadBytes:N0}B per grain)";

    public async Task ActivateAsync(CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        await Parallel.ForAsync(0, grains.Length, new ParallelOptions { MaxDegreeOfParallelism = 64, CancellationToken = cancellationToken },
            async (i, _) => await grains[i].GetWrites());
        Console.WriteLine($"Warm-up: activated {grains.Length:N0} durable writer grains in {watch.Elapsed.TotalSeconds:F1}s");
    }

    public async Task<int> WriteAsync(long sequence, CancellationToken cancellationToken)
    {
        var index = Random.Shared.Next(grains.Length);
        try
        {
            await grains[index].Write(batch, slots, payloadBytes);
            Interlocked.Increment(ref acknowledged[index]);
            return batch;
        }
        catch
        {
            // A failed call (e.g. a timeout) may still have been written, so it widens the accepted range.
            Interlocked.Increment(ref unacknowledged[index]);
            throw;
        }
    }

    /// <summary>
    /// Every acknowledged write must survive recovery from the journal, and nothing beyond what was attempted.
    /// </summary>
    public async Task<bool> VerifyAsync()
    {
        Console.WriteLine();
        Console.WriteLine("Deactivating grains so their state is recovered from the journal...");
        await Parallel.ForAsync(0, grains.Length, new ParallelOptions { MaxDegreeOfParallelism = 64 }, async (i, _) =>
        {
            try
            {
                await grains[i].Deactivate();
            }
            catch
            {
                // Verification below reports a grain that cannot be read.
            }
        });

        // DeactivateOnIdle completes after the call returns; give the deactivations a moment to run.
        await Task.Delay(TimeSpan.FromSeconds(2));

        Console.WriteLine("Verifying recovered write counts against acknowledged writes...");
        var watch = Stopwatch.StartNew();
        var mismatches = 0;
        var errors = 0;
        var totalWrites = 0L;
        await Parallel.ForAsync(0, grains.Length, new ParallelOptions { MaxDegreeOfParallelism = 64 }, async (i, _) =>
        {
            long writes;
            try
            {
                writes = await grains[i].GetWrites();
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref errors);
                Console.WriteLine($"  ERROR reading {grains[i].GetPrimaryKey()}: {ex.GetBaseException().Message}");
                return;
            }

            Interlocked.Add(ref totalWrites, writes);
            if (writes < acknowledged[i] || writes > acknowledged[i] + unacknowledged[i])
            {
                Interlocked.Increment(ref mismatches);
                Console.WriteLine($"  MISMATCH {grains[i].GetPrimaryKey()}: recovered {writes}, acknowledged {acknowledged[i]}, failed {unacknowledged[i]}");
            }
        });

        Console.WriteLine($"Recovered {grains.Length:N0} grains in {watch.Elapsed.TotalSeconds:F1}s");
        Console.WriteLine($"Recovered writes: {totalWrites:N0} (acknowledged {acknowledged.Sum():N0}, failed/unknown {unacknowledged.Sum():N0})");
        var passed = mismatches == 0 && errors == 0;
        Console.WriteLine(passed ? "VERIFICATION PASSED" : $"VERIFICATION FAILED: {mismatches} mismatched grain(s), {errors} unreadable grain(s)");
        return passed;
    }
}
