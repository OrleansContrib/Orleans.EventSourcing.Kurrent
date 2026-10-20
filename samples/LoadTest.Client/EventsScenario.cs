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
        var grainCount = args.GetInt("grains", 1_000);
        var batch = args.GetInt("batch", 1);
        var payloadBytes = args.GetInt("payload", 64);
        var ramp = RampOptions.Parse(args, new RampOptions { StartRate = 200, StepRate = 200, StepSeconds = 30 });

        Console.WriteLine($"Events scenario: grains={grainCount:N0} batch={batch} payload={payloadBytes}B");

        var grains = Enumerable.Range(0, grainCount).Select(_ => client.GetGrain<IEventWriterGrain>(Guid.NewGuid())).ToArray();
        var acknowledged = new long[grainCount];
        var unacknowledged = new long[grainCount];

        var warmup = Stopwatch.StartNew();
        await Parallel.ForAsync(0, grainCount, new ParallelOptions { MaxDegreeOfParallelism = 64, CancellationToken = cancellationToken },
            async (i, _) => await grains[i].GetConfirmedVersion());
        Console.WriteLine($"Warm-up: activated {grainCount:N0} grains in {warmup.Elapsed.TotalSeconds:F1}s");
        Console.WriteLine();

        await RampRunner.RunAsync("events", ramp, async (_, _) =>
        {
            var index = Random.Shared.Next(grainCount);
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
        }, cancellationToken);

        // Verify: every acknowledged event must be in the confirmed log, and nothing beyond what was attempted.
        Console.WriteLine();
        Console.WriteLine("Verifying confirmed versions against acknowledged appends...");
        var mismatches = 0;
        var totalConfirmed = 0L;
        await Parallel.ForAsync(0, grainCount, new ParallelOptions { MaxDegreeOfParallelism = 64 }, async (i, _) =>
        {
            var version = await grains[i].GetConfirmedVersion();
            Interlocked.Add(ref totalConfirmed, version);
            if (version < acknowledged[i] || version > acknowledged[i] + unacknowledged[i])
            {
                Interlocked.Increment(ref mismatches);
                Console.WriteLine($"  MISMATCH {grains[i].GetPrimaryKey()}: confirmed {version}, acknowledged {acknowledged[i]}, failed {unacknowledged[i]}");
            }
        });

        Console.WriteLine($"Confirmed events: {totalConfirmed:N0} (acknowledged {acknowledged.Sum():N0}, failed/unknown {unacknowledged.Sum():N0})");
        Console.WriteLine(mismatches == 0 ? "VERIFICATION PASSED" : $"VERIFICATION FAILED: {mismatches} grain(s)");
        return mismatches == 0 ? 0 : 1;
    }
}
