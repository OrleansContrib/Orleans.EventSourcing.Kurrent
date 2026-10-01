namespace Orleans.EventSourcing.Kurrent.Configuration;

/// <summary>
///     Controls how transient Kurrent failures (for example a cluster leadership election) are retried.
/// </summary>
public sealed class KurrentRetryOptions
{
    /// <summary>
    ///     The maximum number of attempts for an operation, including the first one. Set to 1 to disable retries.
    ///     For catch-up subscriptions this is the number of consecutive failed attempts to (re)establish the subscription
    ///     before the failure is surfaced to the consumer; the count resets whenever a message is received.
    /// </summary>
    public int MaxAttempts { get; set; } = 8;

    /// <summary>
    ///     The delay before the first retry. The delay doubles on each subsequent attempt, with jitter applied.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     The maximum delay between attempts.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(5);
}
