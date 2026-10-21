namespace Orleans.EventSourcing.Kurrent.Configuration;

/// <summary>
///     Controls how Kurrent operations that fail while the cluster changes leader are retried.
/// </summary>
public sealed class KurrentRetryOptions
{
    /// <summary>
    ///     The maximum number of attempts for an operation, including the first one. Set to 1 to disable retries.
    /// </summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>
    ///     The delay before the first retry. The delay doubles on each subsequent attempt, with jitter applied.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     The maximum delay between attempts.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(2);
}
