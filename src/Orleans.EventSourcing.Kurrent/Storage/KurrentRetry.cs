using System.Diagnostics;

using Grpc.Core;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Retries Kurrent operations that fail because the node the client talks to is not, or is no longer, the leader.
/// </summary>
internal static class KurrentRetry
{
    /// <summary>
    ///     True for failures that clear once the client has found the new leader. <see cref="NotLeaderException"/>
    ///     and <see cref="DiscoveryException"/> are raised before the request is accepted. An
    ///     <see cref="StatusCode.Unavailable"/> failure, or the old leader closing its HTTP/2 connection mid-call
    ///     as it shuts down, leaves the outcome unknown; that is safe because an append is only retried with the
    ///     same event IDs and expected version, which Kurrent treats as idempotent.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is NotLeaderException or DiscoveryException
            or RpcException { StatusCode: StatusCode.Unavailable }
            or RpcException { StatusCode: StatusCode.Internal, Status.DebugException: HttpProtocolException };

    /// <summary>
    ///     Runs <paramref name="operation"/>, retrying transient failures with exponential backoff until
    ///     <see cref="KurrentRetryOptions.MaxAttempts"/> is reached. Stops as soon as <paramref name="cancellationToken"/>
    ///     fires, so a requested cancellation surfaces unchanged.
    /// </summary>
    public static async Task<T> ExecuteAsync<T>(KurrentRetryOptions options, KurrentMetrics metrics, string operation, Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < options.MaxAttempts && !cancellationToken.IsCancellationRequested && IsTransient(ex))
            {
                metrics.Retries.Add(1, new TagList
                {
                    { "Operation", operation },
                    { "Exception", ex.GetType().Name },
                });

                await Task.Delay(GetDelay(options, attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public static Task ExecuteAsync(KurrentRetryOptions options, KurrentMetrics metrics, string operation, Func<CancellationToken, Task> action, CancellationToken cancellationToken)
        => ExecuteAsync(options, metrics, operation, async ct =>
        {
            await action(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken);

    internal static TimeSpan GetDelay(KurrentRetryOptions options, int attempt)
    {
        var exponential = Math.Min(options.BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1), options.MaxDelay.TotalMilliseconds);
#pragma warning disable CA5394 // Random is fine for retry jitter
        var jitter = 0.5 + (Random.Shared.NextDouble() * 0.5);
#pragma warning restore CA5394
        return TimeSpan.FromMilliseconds(exponential * jitter);
    }
}
