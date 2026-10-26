using System.Diagnostics;

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     How a provider retries: the configured options, the time budget for an operation and the longest a
///     single attempt can take.
/// </summary>
/// <param name="Options">Attempts and backoff.</param>
/// <param name="Metrics">Where retries are counted.</param>
/// <param name="MaxDuration">Time budget for an operation, from when it started.</param>
/// <param name="AttemptTimeout">The longest one attempt can take (the client's deadline), or null if unbounded.</param>
internal sealed record KurrentRetryPolicy(KurrentRetryOptions Options, KurrentMetrics Metrics, TimeSpan MaxDuration, TimeSpan? AttemptTimeout)
{
    /// <summary>
    ///     The Orleans default response timeout, used when the silo's messaging options are not available.
    /// </summary>
    private static readonly TimeSpan DefaultResponseTimeout = TimeSpan.FromSeconds(30);

    public static KurrentRetryPolicy Create(IServiceProvider serviceProvider, KurrentStorageOptions options)
    {
        var responseTimeout = serviceProvider.GetService<IOptions<SiloMessagingOptions>>()?.Value.ResponseTimeout ?? DefaultResponseTimeout;
        return new KurrentRetryPolicy(options.Retry,
                                      serviceProvider.GetRequiredService<KurrentMetrics>(),
                                      options.Retry.MaxDuration ?? responseTimeout,
                                      options.ClientSettings?.DefaultDeadline);
    }
}

/// <summary>
///     Retries Kurrent operations that fail because the node the client talks to is not, or is no longer, the leader.
/// </summary>
internal static class KurrentRetry
{
    /// <summary>
    ///     True for failures that clear once the client has found the new leader. <see cref="NotLeaderException"/>
    ///     and <see cref="DiscoveryException"/> are raised before the request is accepted. An
    ///     <see cref="StatusCode.Unavailable"/> failure, the old leader closing its HTTP/2 connection mid-call as it
    ///     shuts down, or a call to a node that died without closing its connection running into the client's
    ///     deadline (<see cref="StatusCode.DeadlineExceeded"/>) leave the outcome unknown. Retrying them is safe
    ///     because an append is only retried with the same event IDs and expected version, which Kurrent treats as
    ///     idempotent.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is NotLeaderException or DiscoveryException
            or RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded }
            or RpcException { StatusCode: StatusCode.Internal, Status.DebugException: HttpProtocolException };

    /// <summary>
    ///     Runs <paramref name="action"/>, retrying transient failures with exponential backoff until
    ///     <see cref="KurrentRetryOptions.MaxAttempts"/> is reached or another attempt would not finish within the
    ///     policy's time budget, measured from <paramref name="startTimestamp"/> (a <see cref="Stopwatch"/> timestamp;
    ///     0 means now). Stops as soon as <paramref name="cancellationToken"/> fires, so a requested cancellation
    ///     surfaces unchanged.
    /// </summary>
    public static async Task<T> ExecuteAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken, long startTimestamp = 0)
    {
        if (startTimestamp == 0)
        {
            startTimestamp = Stopwatch.GetTimestamp();
        }

        for (var attempt = 1; ; attempt++)
        {
            TimeSpan delay;
            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldRetry(policy, attempt, ex, Stopwatch.GetElapsedTime(startTimestamp), cancellationToken, out delay))
            {
                policy.Metrics.Retries.Add(1, new TagList
                {
                    { "Operation", operation },
                    { "Exception", ex.GetType().Name },
                });
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    public static Task ExecuteAsync(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task> action, CancellationToken cancellationToken, long startTimestamp = 0)
        => ExecuteAsync(policy, operation, async ct =>
        {
            await action(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken, startTimestamp);

    /// <summary>
    ///     Whether to retry after <paramref name="exception"/> ended attempt <paramref name="attempt"/>, and if so after
    ///     what <paramref name="delay"/>. A retry must still finish within the time budget, even if it runs until the
    ///     client's deadline.
    /// </summary>
    internal static bool ShouldRetry(KurrentRetryPolicy policy, int attempt, Exception exception, TimeSpan elapsed, CancellationToken cancellationToken, out TimeSpan delay)
    {
        delay = GetDelay(policy.Options, attempt);
        return attempt < policy.Options.MaxAttempts
               && !cancellationToken.IsCancellationRequested
               && IsTransient(exception)
               && elapsed + delay + (policy.AttemptTimeout ?? TimeSpan.Zero) <= policy.MaxDuration;
    }

    internal static TimeSpan GetDelay(KurrentRetryOptions options, int attempt)
    {
        var exponential = Math.Min(options.BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1), options.MaxDelay.TotalMilliseconds);
#pragma warning disable CA5394 // Random is fine for retry jitter
        var jitter = 0.5 + (Random.Shared.NextDouble() * 0.5);
#pragma warning restore CA5394
        return TimeSpan.FromMilliseconds(exponential * jitter);
    }
}
