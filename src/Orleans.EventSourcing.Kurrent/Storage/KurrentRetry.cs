using Grpc.Core;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Retries Kurrent operations that fail transiently during a cluster leadership election.
/// </summary>
internal static class KurrentRetry
{
    /// <summary>
    ///     True for failures that are expected to clear once a new leader has been elected.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is NotLeaderException
            || exception is RpcException { StatusCode: StatusCode.Unavailable };

    public static async ValueTask<T> ExecuteAsync<T>(KurrentRetryOptions options, Func<CancellationToken, ValueTask<T>> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(operation);

        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < options.MaxAttempts && IsTransient(ex))
            {
                await Task.Delay(GetDelay(options, attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public static Task<T> ExecuteAsync<T>(KurrentRetryOptions options, Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return ExecuteAsync<T>(options, ct => new ValueTask<T>(operation(ct)), cancellationToken).AsTask();
    }

    internal static TimeSpan GetDelay(KurrentRetryOptions options, int attempt)
    {
        ArgumentNullException.ThrowIfNull(options);

        var exponential = Math.Min(options.BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1), options.MaxDelay.TotalMilliseconds);
#pragma warning disable CA5394 // Random is fine for retry jitter
        var jitter = 0.5 + (Random.Shared.NextDouble() * 0.5);
#pragma warning restore CA5394
        return TimeSpan.FromMilliseconds(exponential * jitter);
    }
}
