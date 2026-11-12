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

    /// <summary>Seconds of load at <see cref="StartRate"/> before step 1 (JIT, connections, caches); not reported as a step.</summary>
    public int WarmupSeconds { get; init; } = 10;

    /// <summary>How long after a step ends to wait for its operations to complete before judging it.</summary>
    public int SettleSeconds { get; init; } = 5;

    /// <summary>Stop once a step's failure ratio exceeds this (0..1).</summary>
    public double MaxErrorRate { get; init; } = 0.01;

    /// <summary>Stop once a step's p99 latency exceeds this many milliseconds (0 disables).</summary>
    public double MaxP99Ms { get; init; }

    /// <summary>Stop once a step completes less than this fraction of its target rate.</summary>
    public double MinAchievedRatio { get; init; } = 0.9;

    /// <summary>Stop once the client issues operations later than this (p99, ms) — beyond it the client, not the server, is the limit.</summary>
    public double MaxClientLagMs { get; init; } = 100;

    public string? CsvPath { get; init; }

    public static RampOptions Parse(CommandLine args, RampOptions defaults) => defaults with
    {
        StartRate = args.GetDouble("start-rate", defaults.StartRate),
        StepRate = args.GetDouble("step-rate", defaults.StepRate),
        StepSeconds = args.GetInt("step-seconds", defaults.StepSeconds),
        MaxSteps = args.GetInt("max-steps", defaults.MaxSteps),
        MaxRate = args.GetDouble("max-rate", defaults.MaxRate),
        MaxInFlight = args.GetInt("max-in-flight", defaults.MaxInFlight),
        WarmupSeconds = args.GetInt("warmup-seconds", defaults.WarmupSeconds),
        SettleSeconds = args.GetInt("settle-seconds", defaults.SettleSeconds),
        MaxErrorRate = args.GetDouble("max-error-rate", defaults.MaxErrorRate),
        MaxP99Ms = args.GetDouble("max-p99-ms", defaults.MaxP99Ms),
        MinAchievedRatio = args.GetDouble("min-achieved", defaults.MinAchievedRatio),
        MaxClientLagMs = args.GetDouble("max-client-lag-ms", defaults.MaxClientLagMs),
        CsvPath = args.GetString("csv", defaults.CsvPath),
    };

    public override string ToString()
        => $"start={StartRate:N0}/s step=+{StepRate:N0}/s every {StepSeconds}s max-steps={MaxSteps} warm-up={WarmupSeconds}s settle={SettleSeconds}s max-in-flight={MaxInFlight:N0} "
         + $"stop-on: errors>{MaxErrorRate:P1}{(MaxP99Ms > 0 ? $" p99>{MaxP99Ms:N0}ms" : "")} achieved<{MinAchievedRatio:P0} client-lag-p99>{MaxClientLagMs:N0}ms";
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
    long Pending,
    double P50Ms,
    double P90Ms,
    double P99Ms,
    double MaxMs,
    double ClientLagP99Ms,
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
///
/// Every operation is attributed to the step that issued it, so a step's latency is not polluted by
/// stragglers from the previous, lower-rate step. A step is judged <see cref="RampOptions.SettleSeconds"/>
/// after it ends (while the next step is already running); operations still pending by then count
/// against its achieved rate. The final table is printed once everything has drained.
/// </summary>
internal static class RampRunner
{
    private const int MinSamplesForClientLag = 500;

    public static async Task<IReadOnlyList<StepResult>> RunAsync(string unitName, RampOptions options, RampOperation operation, CancellationToken cancellationToken)
    {
        var inFlight = 0L;
        var sequence = 0L;
        var judged = new List<StepRun>();
        StepRun? pending = null;
        string? stopReason = null;

        Console.WriteLine($"Ramp: {options}");

        if (options.WarmupSeconds > 0)
        {
            var warmup = new StepRun(0, options.StartRate);
            await IssueAsync(warmup, options.WarmupSeconds);
            Console.WriteLine($"Warm-up: {options.WarmupSeconds}s at {options.StartRate:N0}/s issued {warmup.Issued:N0} ops (excluded from results)");
        }

        Console.WriteLine(Header(unitName));
        for (var step = 0; step < options.MaxSteps && stopReason is null && !cancellationToken.IsCancellationRequested; step++)
        {
            var rate = Math.Min(options.MaxRate, options.StartRate + step * options.StepRate);
            var run = new StepRun(step + 1, rate);
            await IssueAsync(run, options.StepSeconds);

            if (stopReason is not null)
            {
                // The previous step failed its checks while this one was running: this step is abandoned.
                Console.WriteLine($"Step {run.Step} abandoned after {run.Seconds:F0}s.");
                break;
            }

            // A step shorter than the settle time can still have the previous step waiting.
            if (pending is not null)
            {
                Judge(force: true);
            }

            pending = run;
            if (rate >= options.MaxRate)
            {
                await SettleAsync();
                stopReason ??= "reached --max-rate";
            }
        }

        await SettleAsync();
        if (stopReason is not null)
        {
            Console.WriteLine($"Stopping ramp: {stopReason}");
        }

        // Let outstanding operations finish so the final numbers (and callers' verification) are complete.
        var drain = Stopwatch.StartNew();
        while (Interlocked.Read(ref inFlight) > 0 && drain.Elapsed < TimeSpan.FromSeconds(60))
        {
            await Task.Delay(50, CancellationToken.None);
        }

        if (Interlocked.Read(ref inFlight) > 0)
        {
            Console.WriteLine($"Warning: {Interlocked.Read(ref inFlight):N0} operations still in flight after 60s drain.");
        }

        // Final numbers include late completions; pass/fail verdicts are the ones made at judging time.
        var results = judged.Select(run => run.ToResult() with { StopReason = run.Verdict!.StopReason }).ToList();
        Console.WriteLine();
        Console.WriteLine("Final results (late completions included):");
        Console.WriteLine(Header(unitName));
        foreach (var result in results)
        {
            PrintRow(result);
        }

        PrintSummary(unitName, results);
        WriteCsv(options.CsvPath, unitName, results);
        return results;

        async Task IssueAsync(StepRun run, int seconds)
        {
            var ticksPerSecond = (double)Stopwatch.Frequency;
            var start = Stopwatch.GetTimestamp();
            var stepTicks = (long)(seconds * ticksPerSecond);
            long issued = 0;

            while (!cancellationToken.IsCancellationRequested && stopReason is null)
            {
                var now = Stopwatch.GetTimestamp();
                if (now - start >= stepTicks)
                {
                    break;
                }

                // Issue every operation that should have started by now. Timer resolution (~15ms on
                // Windows) makes issuance bursty at that granularity, but the long-run rate is exact.
                var due = (long)((now - start) / ticksPerSecond * run.Rate);
                for (; issued < due; issued++)
                {
                    var intended = start + (long)(issued / run.Rate * ticksPerSecond);
                    run.RecordIssue(Stopwatch.GetElapsedTime(intended));
                    if (Interlocked.Read(ref inFlight) >= options.MaxInFlight)
                    {
                        run.RecordShed();
                        continue;
                    }

                    Interlocked.Increment(ref inFlight);
                    run.Started();

                    // Run on the thread pool so synchronous work inside an operation can't delay
                    // issuing the next one; that cost still shows up in the operation's latency.
                    var seq = sequence++;
                    _ = Task.Run(() => ExecuteAsync(seq, intended, run), CancellationToken.None);
                }

                if (pending is not null)
                {
                    Judge(force: false);
                }

                await Task.Delay(1, CancellationToken.None);
            }

            run.Seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;
            run.EndedAt = Stopwatch.GetTimestamp();
        }

        async Task SettleAsync()
        {
            while (pending is not null)
            {
                Judge(force: cancellationToken.IsCancellationRequested);
                if (pending is not null)
                {
                    await Task.Delay(50, CancellationToken.None);
                }
            }
        }

        void Judge(bool force)
        {
            var run = pending!;
            if (!force && run.Pending > 0 && Stopwatch.GetElapsedTime(run.EndedAt) < TimeSpan.FromSeconds(options.SettleSeconds))
            {
                return;
            }

            var result = run.ToResult();
            result = result with { StopReason = GetStopReason(result, options) };
            run.Verdict = result;
            judged.Add(run);
            pending = null;
            PrintRow(result);
            stopReason ??= result.StopReason;
        }

        async Task ExecuteAsync(long seq, long intendedTimestamp, StepRun run)
        {
            try
            {
                var units = await operation(seq, cancellationToken);
                run.RecordSuccess(Stopwatch.GetElapsedTime(intendedTimestamp), units);
            }
            catch (Exception ex)
            {
                run.RecordFailure(ex);
            }
            finally
            {
                run.Completed();
                Interlocked.Decrement(ref inFlight);
            }
        }
    }

    private static string? GetStopReason(StepResult result, RampOptions options)
    {
        // Checked first: once the client lags, the server-side numbers below are not trustworthy.
        // Below a few hundred operations p99 is effectively the max, so one GC or JIT pause would trip it.
        if (result.Succeeded + result.Failed + result.Shed >= MinSamplesForClientLag && result.ClientLagP99Ms > options.MaxClientLagMs)
        {
            return $"client issue lag p99 {result.ClientLagP99Ms:N0}ms exceeded {options.MaxClientLagMs:N0}ms (client-limited; add client processes or cores)";
        }

        if (result.ErrorRate > options.MaxErrorRate)
        {
            return $"error rate {result.ErrorRate:P2} exceeded {options.MaxErrorRate:P2}";
        }

        if (options.MaxP99Ms > 0 && result.P99Ms > options.MaxP99Ms)
        {
            return $"p99 {result.P99Ms:N0}ms exceeded {options.MaxP99Ms:N0}ms";
        }

        if (result.AchievedRate < result.TargetRate * options.MinAchievedRatio)
        {
            return $"achieved {result.AchievedRate:N0}/s is below {options.MinAchievedRatio:P0} of target {result.TargetRate:N0}/s ({result.Pending:N0} still pending after settle; saturated)";
        }

        return null;
    }

    private static string Header(string unitName)
        => $"{"step",4} {"target/s",9} {"ok/s",9} {unitName + "/s",10} {"ok",9} {"fail",7} {"shed",7} {"pending",8} {"p50",8} {"p90",8} {"p99",8} {"max",8} {"lag p99",8}";

    private static void PrintRow(StepResult r)
    {
        Console.WriteLine($"{r.Step,4} {r.TargetRate,9:N0} {r.AchievedRate,9:N0} {r.UnitRate,10:N0} {r.Succeeded,9:N0} {r.Failed,7:N0} {r.Shed,7:N0} {r.Pending,8:N0} {Ms(r.P50Ms)} {Ms(r.P90Ms)} {Ms(r.P99Ms)} {Ms(r.MaxMs)} {Ms(r.ClientLagP99Ms)}{(r.StopReason is null ? "" : "  FAIL")}");
        foreach (var (error, count) in r.Errors.OrderByDescending(e => e.Value).Take(3))
        {
            Console.WriteLine($"       {count,7:N0} x {error}");
        }
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
        writer.WriteLine($"step,target_rate,seconds,succeeded,failed,shed,pending,achieved_rate,{unitName}_per_sec,p50_ms,p90_ms,p99_ms,max_ms,client_lag_p99_ms,stop_reason");
        foreach (var r in results)
        {
            writer.WriteLine(string.Join(',',
                r.Step, F(r.TargetRate), F(r.Seconds), r.Succeeded, r.Failed, r.Shed, r.Pending, F(r.AchievedRate), F(r.UnitRate),
                F(r.P50Ms), F(r.P90Ms), F(r.P99Ms), F(r.MaxMs), F(r.ClientLagP99Ms), $"\"{r.StopReason}\""));
        }

        Console.WriteLine($"Wrote {path}");

        static string F(double value) => value.ToString("F2", CultureInfo.InvariantCulture);
    }

    private static string Ms(double value) => value >= 1000 ? $"{value / 1000,6:F2}s " : $"{value,6:F1}ms";

    private sealed class StepRun(int step, double rate)
    {
        private readonly LatencyHistogram latency = new();
        private readonly LatencyHistogram issueLag = new();
        private readonly ConcurrentDictionary<string, long> errors = new();
        private long succeeded;
        private long failed;
        private long shed;
        private long units;
        private long pending;

        public int Step { get; } = step;

        public double Rate { get; } = rate;

        public double Seconds { get; set; }

        public long EndedAt { get; set; }

        public StepResult? Verdict { get; set; }

        public long Issued => issueLag.Count;

        public long Pending => Interlocked.Read(ref pending);

        public void RecordIssue(TimeSpan lag) => issueLag.Record(lag);

        public void Started() => Interlocked.Increment(ref pending);

        public void Completed() => Interlocked.Decrement(ref pending);

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

        public StepResult ToResult() => new(
            Step, Rate, Seconds,
            Interlocked.Read(ref succeeded), Interlocked.Read(ref failed), Interlocked.Read(ref shed), Interlocked.Read(ref units), Pending,
            latency.PercentileMs(0.50), latency.PercentileMs(0.90), latency.PercentileMs(0.99), latency.MaxMs, issueLag.PercentileMs(0.99),
            new Dictionary<string, long>(errors), null);

        private static string Truncate(string message) => message.Length <= 120 ? message : message[..120] + "...";
    }
}
