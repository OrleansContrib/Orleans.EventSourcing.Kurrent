using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

namespace LoadTest.Client;

/// <summary>
/// Options for a stepped, open-loop load ramp: the target rate starts at <see cref="StartRate"/> and
/// increases by <see cref="StepRate"/> every <see cref="StepSeconds"/> until a limit or a stop
/// condition is reached.
/// </summary>
internal sealed record RampOptions
{
    public double StartRate { get; init; } = 100;
    public double StepRate { get; init; } = 100;
    public int StepSeconds { get; init; } = 30;
    public int MaxSteps { get; init; } = 20;
    public double MaxRate { get; init; } = double.MaxValue;
    public int MaxInFlight { get; init; } = 10_000;

    /// <summary>Stop once a step's failure ratio exceeds this (0..1).</summary>
    public double MaxErrorRate { get; init; } = 0.01;

    /// <summary>Stop once a step's p99 latency exceeds this many milliseconds (0 disables).</summary>
    public double MaxP99Ms { get; init; }

    /// <summary>Stop once a step completes less than this fraction of its target rate.</summary>
    public double MinAchievedRatio { get; init; } = 0.9;

    public string? CsvPath { get; init; }

    public static RampOptions Parse(CommandLine args, RampOptions defaults) => defaults with
    {
        StartRate = args.GetDouble("start-rate", defaults.StartRate),
        StepRate = args.GetDouble("step-rate", defaults.StepRate),
        StepSeconds = args.GetInt("step-seconds", defaults.StepSeconds),
        MaxSteps = args.GetInt("max-steps", defaults.MaxSteps),
        MaxRate = args.GetDouble("max-rate", defaults.MaxRate),
        MaxInFlight = args.GetInt("max-in-flight", defaults.MaxInFlight),
        MaxErrorRate = args.GetDouble("max-error-rate", defaults.MaxErrorRate),
        MaxP99Ms = args.GetDouble("max-p99-ms", defaults.MaxP99Ms),
        MinAchievedRatio = args.GetDouble("min-achieved", defaults.MinAchievedRatio),
        CsvPath = args.GetString("csv", defaults.CsvPath),
    };

    public override string ToString()
        => $"start={StartRate:N0}/s step=+{StepRate:N0}/s every {StepSeconds}s max-steps={MaxSteps} max-in-flight={MaxInFlight:N0} "
         + $"stop-on: errors>{MaxErrorRate:P1}{(MaxP99Ms > 0 ? $" p99>{MaxP99Ms:N0}ms" : "")} achieved<{MinAchievedRatio:P0}";
}

/// <summary>
/// Performs one operation of the load test and returns the number of work units it completed
/// (for example, the number of events appended).
/// </summary>
internal delegate Task<int> RampOperation(long sequence, CancellationToken cancellationToken);

internal sealed record StepResult(
    int Step,
    double TargetRate,
    double Seconds,
    long Succeeded,
    long Failed,
    long Shed,
    long Units,
    long InFlightAtEnd,
    double P50Ms,
    double P90Ms,
    double P99Ms,
    double MaxMs,
    IReadOnlyDictionary<string, long> Errors,
    string? StopReason)
{
    public double AchievedRate => Succeeded / Seconds;

    public double UnitRate => Units / Seconds;

    public double ErrorRate => Succeeded + Failed == 0 ? 0 : (double)Failed / (Succeeded + Failed);
}

/// <summary>
/// Drives an open-loop stepped ramp. Operations are scheduled at the target rate independently of
/// how quickly earlier operations complete, so a slow system shows up as rising latency rather than
/// a quietly reduced request rate. Latency is measured from each operation's intended start time,
/// which avoids coordinated omission when the client itself falls behind.
/// </summary>
internal static class RampRunner
{
    public static async Task<IReadOnlyList<StepResult>> RunAsync(string unitName, RampOptions options, RampOperation operation, CancellationToken cancellationToken)
    {
        var results = new List<StepResult>();
        var inFlight = 0L;
        var sequence = 0L;
        var current = new StepStats();
        var ticksPerSecond = (double)Stopwatch.Frequency;

        Console.WriteLine($"Ramp: {options}");
        Console.WriteLine($"{"step",4} {"target/s",9} {"ok/s",9} {unitName + "/s",10} {"ok",9} {"fail",7} {"shed",7} {"inflight",8} {"p50",8} {"p90",8} {"p99",8} {"max",8}");

        for (var step = 0; step < options.MaxSteps && !cancellationToken.IsCancellationRequested; step++)
        {
            var rate = Math.Min(options.MaxRate, options.StartRate + step * options.StepRate);
            var stats = new StepStats();
            Volatile.Write(ref current, stats);

            var stepStart = Stopwatch.GetTimestamp();
            var stepTicks = (long)(options.StepSeconds * ticksPerSecond);
            long issued = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var elapsed = Stopwatch.GetTimestamp() - stepStart;
                if (elapsed >= stepTicks)
                {
                    break;
                }

                // Issue every operation that should have started by now. Timer resolution (~15ms on
                // Windows) makes issuance bursty at that granularity, but the long-run rate is exact.
                var due = (long)(elapsed / ticksPerSecond * rate);
                for (; issued < due; issued++)
                {
                    var intended = stepStart + (long)(issued / rate * ticksPerSecond);
                    if (Interlocked.Read(ref inFlight) >= options.MaxInFlight)
                    {
                        stats.RecordShed();
                        continue;
                    }

                    Interlocked.Increment(ref inFlight);
                    _ = ExecuteAsync(sequence++, intended);
                }

                await Task.Delay(1, CancellationToken.None);
            }

            var seconds = (Stopwatch.GetTimestamp() - stepStart) / ticksPerSecond;
            var result = stats.ToResult(step + 1, rate, seconds, Interlocked.Read(ref inFlight));
            var stopReason = GetStopReason(result, options, rate);
            result = result with { StopReason = stopReason };
            results.Add(result);

            Console.WriteLine($"{result.Step,4} {rate,9:N0} {result.AchievedRate,9:N0} {result.UnitRate,10:N0} {result.Succeeded,9:N0} {result.Failed,7:N0} {result.Shed,7:N0} {result.InFlightAtEnd,8:N0} {Ms(result.P50Ms)} {Ms(result.P90Ms)} {Ms(result.P99Ms)} {Ms(result.MaxMs)}");
            foreach (var (error, count) in result.Errors.OrderByDescending(e => e.Value).Take(3))
            {
                Console.WriteLine($"       {count,7:N0} x {error}");
            }

            if (stopReason is not null)
            {
                Console.WriteLine($"Stopping ramp: {stopReason}");
                break;
            }

            if (rate >= options.MaxRate)
            {
                Console.WriteLine("Stopping ramp: reached --max-rate.");
                break;
            }
        }

        // Let outstanding operations finish so callers can verify state against what was acknowledged.
        Volatile.Write(ref current, new StepStats());
        var drain = Stopwatch.StartNew();
        while (Interlocked.Read(ref inFlight) > 0 && drain.Elapsed < TimeSpan.FromSeconds(60))
        {
            await Task.Delay(50, CancellationToken.None);
        }

        if (Interlocked.Read(ref inFlight) > 0)
        {
            Console.WriteLine($"Warning: {Interlocked.Read(ref inFlight):N0} operations still in flight after 60s drain.");
        }

        PrintSummary(unitName, results);
        WriteCsv(options.CsvPath, unitName, results);
        return results;

        async Task ExecuteAsync(long seq, long intendedTimestamp)
        {
            try
            {
                var units = await operation(seq, cancellationToken);
                Volatile.Read(ref current).RecordSuccess(Stopwatch.GetElapsedTime(intendedTimestamp), units);
            }
            catch (Exception ex)
            {
                Volatile.Read(ref current).RecordFailure(ex);
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        }
    }

    private static string? GetStopReason(StepResult result, RampOptions options, double rate)
    {
        if (result.ErrorRate > options.MaxErrorRate)
        {
            return $"error rate {result.ErrorRate:P2} exceeded {options.MaxErrorRate:P2}";
        }

        if (options.MaxP99Ms > 0 && result.P99Ms > options.MaxP99Ms)
        {
            return $"p99 {result.P99Ms:N0}ms exceeded {options.MaxP99Ms:N0}ms";
        }

        if (result.AchievedRate < rate * options.MinAchievedRatio)
        {
            return $"achieved {result.AchievedRate:N0}/s is below {options.MinAchievedRatio:P0} of target {rate:N0}/s (saturated)";
        }

        return null;
    }

    private static void PrintSummary(string unitName, IReadOnlyList<StepResult> results)
    {
        Console.WriteLine();
        var sustained = results.LastOrDefault(r => r.StopReason is null);
        var peak = results.OrderByDescending(r => r.UnitRate).FirstOrDefault();
        if (sustained is not null)
        {
            Console.WriteLine($"Highest sustained step: {sustained.TargetRate:N0} ops/s target -> {sustained.AchievedRate:N0} ops/s, {sustained.UnitRate:N0} {unitName}/s, p99 {sustained.P99Ms:N1}ms");
        }
        else
        {
            Console.WriteLine("No step met the pass criteria.");
        }

        if (peak is not null)
        {
            Console.WriteLine($"Peak observed throughput:  {peak.UnitRate:N0} {unitName}/s (step {peak.Step}, p99 {peak.P99Ms:N1}ms)");
        }
    }

    private static void WriteCsv(string? path, string unitName, IReadOnlyList<StepResult> results)
    {
        if (string.IsNullOrEmpty(path))
        {
            return;
        }

        using var writer = new StreamWriter(path);
        writer.WriteLine($"step,target_rate,seconds,succeeded,failed,shed,achieved_rate,{unitName}_per_sec,in_flight_at_end,p50_ms,p90_ms,p99_ms,max_ms,stop_reason");
        foreach (var r in results)
        {
            writer.WriteLine(string.Join(',',
                r.Step, F(r.TargetRate), F(r.Seconds), r.Succeeded, r.Failed, r.Shed, F(r.AchievedRate), F(r.UnitRate), r.InFlightAtEnd,
                F(r.P50Ms), F(r.P90Ms), F(r.P99Ms), F(r.MaxMs), $"\"{r.StopReason}\""));
        }

        Console.WriteLine($"Wrote {path}");

        static string F(double value) => value.ToString("F2", CultureInfo.InvariantCulture);
    }

    private static string Ms(double value) => value >= 1000 ? $"{value / 1000,6:F2}s " : $"{value,6:F1}ms";

    private sealed class StepStats
    {
        private readonly LatencyHistogram latency = new();
        private readonly ConcurrentDictionary<string, long> errors = new();
        private long succeeded;
        private long failed;
        private long shed;
        private long units;

        public void RecordSuccess(TimeSpan elapsed, int completedUnits)
        {
            latency.Record(elapsed);
            Interlocked.Increment(ref succeeded);
            Interlocked.Add(ref units, completedUnits);
        }

        public void RecordFailure(Exception ex)
        {
            Interlocked.Increment(ref failed);
            var key = ex.GetBaseException() is var root && root != ex ? $"{ex.GetType().Name} -> {root.GetType().Name}: {Truncate(root.Message)}" : $"{ex.GetType().Name}: {Truncate(ex.Message)}";
            errors.AddOrUpdate(key, 1, (_, count) => count + 1);
        }

        public void RecordShed() => Interlocked.Increment(ref shed);

        public StepResult ToResult(int step, double rate, double seconds, long inFlight) => new(
            step, rate, seconds,
            Interlocked.Read(ref succeeded), Interlocked.Read(ref failed), Interlocked.Read(ref shed), Interlocked.Read(ref units), inFlight,
            latency.PercentileMs(0.50), latency.PercentileMs(0.90), latency.PercentileMs(0.99), latency.MaxMs,
            new Dictionary<string, long>(errors), null);

        private static string Truncate(string message) => message.Length <= 120 ? message : message[..120] + "...";
    }
}
