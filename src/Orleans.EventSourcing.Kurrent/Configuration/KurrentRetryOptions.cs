namespace Orleans.EventSourcing.Kurrent.Configuration;

/// <summary>
///     Controls how Kurrent operations that fail while the cluster changes leader are retried.
/// </summary>
public sealed class KurrentRetryOptions
{
    /// <summary>
    ///     The maximum number of attempts for an operation, including the first one. Set to 1 to disable retries.
    /// </summary>
    public int MaxAttempts { get; set; } = 13;

    /// <summary>
    ///     The delay before the first retry. The delay doubles on each subsequent attempt, with jitter applied.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     The maximum delay between attempts.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    ///     The time budget for an operation whose caller cannot cancel it, such as a write from
    ///     <c>JournaledGrain</c>, or a storage call made with <see cref="CancellationToken.None"/>. It is measured from
    ///     when the operation started (for a queued write, from when it was submitted). A retry is only started if its
    ///     delay plus a full attempt, bounded by the client's <c>DefaultDeadline</c>, still fits within it. Defaults to
    ///     the Orleans response timeout, so that a retried operation finishes before the grain call that is waiting for
    ///     it times out. When the caller passes a cancellable token it decides how long to wait, so this does not apply
    ///     and retries are limited by <see cref="MaxAttempts"/> and that token.
    /// </summary>
    public TimeSpan? MaxDuration { get; set; }
}
