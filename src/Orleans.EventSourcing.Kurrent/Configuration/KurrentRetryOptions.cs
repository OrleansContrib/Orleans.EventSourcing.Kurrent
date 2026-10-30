namespace Orleans.EventSourcing.Kurrent.Configuration;

/// <summary>
///     Controls the backoff when Kurrent operations that fail while the cluster changes leader are retried.
///     How long they keep retrying is set by the caller's cancellation token or, if it cannot be cancelled, the Orleans
///     response timeout.
/// </summary>
public sealed class KurrentRetryOptions
{
    /// <summary>
    ///     The delay before the first retry. The delay doubles on each subsequent attempt, with jitter applied.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     The maximum delay between attempts.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(2);
}
