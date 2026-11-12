namespace LoadTest.Client;

/// <summary>
/// A fixed-size, lock-free latency histogram with logarithmic buckets (~2% relative precision from
/// 10µs upwards). Memory stays bounded regardless of the request rate, and recording is safe from any
/// thread.
/// </summary>
internal sealed class LatencyHistogram
{
    private const double MinMicros = 10;
    private const double Ratio = 1.02;
    private static readonly double LogRatio = Math.Log(Ratio);

    private readonly long[] buckets = new long[1200];
    private long count;
    private long maxMicros;

    public long Count => Interlocked.Read(ref count);

    public double MaxMs => Interlocked.Read(ref maxMicros) / 1000.0;

    public void Record(TimeSpan latency) => RecordMs(latency.TotalMilliseconds);

    public void RecordMs(double milliseconds)
    {
        var micros = Math.Max(0, milliseconds * 1000);
        var index = micros <= MinMicros ? 0 : Math.Min(buckets.Length - 1, (int)Math.Ceiling(Math.Log(micros / MinMicros) / LogRatio));
        Interlocked.Increment(ref buckets[index]);
        Interlocked.Increment(ref count);

        var asLong = (long)micros;
        var current = Interlocked.Read(ref maxMicros);
        while (asLong > current)
        {
            var observed = Interlocked.CompareExchange(ref maxMicros, asLong, current);
            if (observed == current)
            {
                break;
            }

            current = observed;
        }
    }

    /// <summary>Returns the upper bound of the bucket containing the given percentile (0..1), in ms.</summary>
    public double PercentileMs(double percentile)
    {
        var total = Count;
        if (total == 0)
        {
            return 0;
        }

        var target = Math.Max(1, (long)Math.Ceiling(percentile * total));
        long seen = 0;
        for (var i = 0; i < buckets.Length; i++)
        {
            seen += Interlocked.Read(ref buckets[i]);
            if (seen >= target)
            {
                return Math.Min(MaxMs, MinMicros * Math.Pow(Ratio, i) / 1000.0);
            }
        }

        return MaxMs;
    }
}
