using System.Collections.Concurrent;
using System.Diagnostics;

namespace LoadTest.Client;

/// <summary>
/// Applies open-loop load at a constant rate while something else disrupts the cluster, printing a
/// timeline row every interval and keeping per-phase statistics. Latency is measured from each
/// operation's intended start time (no coordinated omission); completions count towards the
/// interval and phase in which they complete, which is what an observer of the system would see.
/// </summary>
internal sealed class SteadyLoad(double rate, int maxInFlight, RampOperation operation)
{
    private readonly long started = Stopwatch.GetTimestamp();
    private readonly List<Phase> phases = [];
    private Window interval = new();
    private Phase phase = new("start", TimeSpan.Zero);
    private long inFlight;
    private long lastSuccess = Stopwatch.GetTimestamp();

    public TimeSpan Elapsed => Stopwatch.GetElapsedTime(started);

    public IReadOnlyList<Phase> Phases => phases;

    /// <summary>Starts a new phase; subsequent completions count towards it.</summary>
    public void BeginPhase(string name)
    {
        var next = new Phase(name, Elapsed);
        lock (phases)
        {
            Volatile.Read(ref phase).End = next.Start;
            phases.Add(next);
            Volatile.Write(ref phase, next);
        }
    }

    public void Log(string message) => Console.WriteLine($"  [t+{Elapsed.TotalSeconds,4:N0}s] {message}");

    /// <summary>Issues operations at the configured rate until <paramref name="stop"/> is cancelled, then waits for them to drain.</summary>
    public async Task RunAsync(TimeSpan reportInterval, Func<Task<string>> status, CancellationToken stop)
    {
        var reporter = ReportAsync(reportInterval, status, stop);
        var ticksPerSecond = (double)Stopwatch.Frequency;
        var start = Stopwatch.GetTimestamp();
        long issued = 0;
        long sequence = 0;

        while (!stop.IsCancellationRequested)
        {
            var due = (long)((Stopwatch.GetTimestamp() - start) / ticksPerSecond * rate);
            for (; issued < due; issued++)
            {
                var intended = start + (long)(issued / rate * ticksPerSecond);
                if (Interlocked.Read(ref inFlight) >= maxInFlight)
                {
                    Volatile.Read(ref interval).RecordShed();
                    Volatile.Read(ref phase).Window.RecordShed();
                    continue;
                }

                Interlocked.Increment(ref inFlight);
                var seq = sequence++;
                _ = Task.Run(() => ExecuteAsync(seq, intended), CancellationToken.None);
            }

            await Task.Delay(1, CancellationToken.None);
        }

        var drain = Stopwatch.StartNew();
        while (Interlocked.Read(ref inFlight) > 0 && drain.Elapsed < TimeSpan.FromSeconds(90))
        {
            await Task.Delay(50, CancellationToken.None);
        }

        await reporter;
        lock (phases)
        {
            Volatile.Read(ref phase).End = Elapsed;
        }
    }

    public void PrintPhaseSummary()
    {
        Console.WriteLine();
        Console.WriteLine("Per-phase results (completions counted in the phase they completed):");
        Console.WriteLine($"{"phase",-34} {"secs",5} {"ok/s",7} {"ok",8} {"fail",6} {"shed",6} {"p50",8} {"p99",8} {"max",8} {"gap",7}");
        foreach (var p in phases)
        {
            var seconds = Math.Max(0.001, ((p.End ?? Elapsed) - p.Start).TotalSeconds);
            var w = p.Window;
            Console.WriteLine($"{Truncate(p.Name, 34),-34} {seconds,5:N0} {w.Succeeded / seconds,7:N0} {w.Succeeded,8:N0} {w.Failed,6:N0} {w.Shed,6:N0} {Ms(w.Latency.PercentileMs(0.5))} {Ms(w.Latency.PercentileMs(0.99))} {Ms(w.Latency.MaxMs)} {w.LongestGap.TotalSeconds,6:N1}s");
            foreach (var (error, count) in w.Errors.OrderByDescending(e => e.Value).Take(2))
            {
                Console.WriteLine($"{"",36}{count,6:N0} x {error}");
            }
        }

        Console.WriteLine("  gap = longest time with no successful completion in the phase");
    }

    private async Task ReportAsync(TimeSpan reportInterval, Func<Task<string>> status, CancellationToken stop)
    {
        Console.WriteLine($"{"",9} {"ok/s",7} {"fail",6} {"shed",6} {"pending",7} {"p50",8} {"p99",8} {"max",8}  cluster");
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(reportInterval, stop);
            }
            catch (OperationCanceledException)
            {
            }

            var w = Interlocked.Exchange(ref interval, new Window());
            string state;
            try
            {
                state = await status();
            }
            catch (Exception ex)
            {
                state = $"status unavailable ({ex.GetType().Name})";
            }

            Console.WriteLine($"  t+{Elapsed.TotalSeconds,4:N0}s {w.Succeeded / reportInterval.TotalSeconds,7:N0} {w.Failed,6:N0} {w.Shed,6:N0} {Interlocked.Read(ref inFlight),7:N0} {Ms(w.Latency.PercentileMs(0.5))} {Ms(w.Latency.PercentileMs(0.99))} {Ms(w.Latency.MaxMs)}  {state}");
        }
    }

    private async Task ExecuteAsync(long seq, long intendedTimestamp)
    {
        try
        {
            await operation(seq, CancellationToken.None);
            var latency = Stopwatch.GetElapsedTime(intendedTimestamp);
            var now = Stopwatch.GetTimestamp();
            var gap = Stopwatch.GetElapsedTime(Interlocked.Exchange(ref lastSuccess, now), now);
            Volatile.Read(ref interval).RecordSuccess(latency, gap);
            Volatile.Read(ref phase).Window.RecordSuccess(latency, gap);
        }
        catch (Exception ex)
        {
            Volatile.Read(ref interval).RecordFailure(ex);
            Volatile.Read(ref phase).Window.RecordFailure(ex);
        }
        finally
        {
            Interlocked.Decrement(ref inFlight);
        }
    }

    private static string Ms(double value) => value >= 1000 ? $"{value / 1000,6:F2}s " : $"{value,6:F1}ms";

    private static string Truncate(string value, int length) => value.Length <= length ? value : value[..(length - 1)] + "…";

    internal sealed class Phase(string name, TimeSpan start)
    {
        public string Name { get; } = name;

        public TimeSpan Start { get; } = start;

        public TimeSpan? End { get; set; }

        public Window Window { get; } = new();
    }

    internal sealed class Window
    {
        private readonly ConcurrentDictionary<string, long> errors = new();
        private long succeeded;
        private long failed;
        private long shed;
        private long longestGapTicks;

        public LatencyHistogram Latency { get; } = new();

        public long Succeeded => Interlocked.Read(ref succeeded);

        public long Failed => Interlocked.Read(ref failed);

        public long Shed => Interlocked.Read(ref shed);

        public TimeSpan LongestGap => TimeSpan.FromTicks(Interlocked.Read(ref longestGapTicks));

        public IReadOnlyDictionary<string, long> Errors => errors;

        public void RecordSuccess(TimeSpan latency, TimeSpan gapSincePreviousSuccess)
        {
            Latency.Record(latency);
            Interlocked.Increment(ref succeeded);
            var gap = gapSincePreviousSuccess.Ticks;
            var current = Interlocked.Read(ref longestGapTicks);
            while (gap > current)
            {
                var observed = Interlocked.CompareExchange(ref longestGapTicks, gap, current);
                if (observed == current)
                {
                    break;
                }

                current = observed;
            }
        }

        public void RecordFailure(Exception ex)
        {
            Interlocked.Increment(ref failed);
            var root = ex.GetBaseException();
            var message = root.Message.Length <= 100 ? root.Message : root.Message[..100] + "...";
            errors.AddOrUpdate($"{root.GetType().Name}: {message}", 1, (_, count) => count + 1);
        }

        public void RecordShed() => Interlocked.Increment(ref shed);
    }
}
