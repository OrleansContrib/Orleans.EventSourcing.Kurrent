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
///     How a provider retries: the backoff to use and how long an operation may run if its caller cannot cancel it.
/// </summary>
/// <param name="Options">Backoff between attempts.</param>
/// <param name="Metrics">Where retries are counted.</param>
/// <param name="Timeout">How long an operation may run if its caller cannot cancel it.</param>
internal sealed record KurrentRetryPolicy(KurrentRetryOptions Options, KurrentMetrics Metrics, TimeSpan Timeout)
{
    /// <summary>
    ///     The Orleans default response timeout, used when the silo's messaging options are not available.
    /// </summary>
    private static readonly TimeSpan DefaultResponseTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     The longest delay <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/> accepts.
    /// </summary>
    private static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    public static KurrentRetryPolicy Create(IServiceProvider serviceProvider, KurrentStorageOptions options)
    {
        var responseTimeout = serviceProvider.GetService<IOptions<SiloMessagingOptions>>()?.Value.ResponseTimeout ?? DefaultResponseTimeout;
        return new KurrentRetryPolicy(options.Retry,
                                      serviceProvider.GetRequiredService<KurrentMetrics>(),
                                      responseTimeout > MaxTimeout ? System.Threading.Timeout.InfiniteTimeSpan : responseTimeout);
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
    ///     <see cref="StatusCode.Unavailable"/> failure, or the old leader closing its HTTP/2 connection mid-call as it
    ///     shuts down, leaves the outcome unknown; that is safe because an append is only retried with the same event
    ///     IDs and expected version, which Kurrent treats as idempotent. <see cref="StatusCode.DeadlineExceeded"/> is
    ///     not retried: the client does not reconnect after it, so a retry would hang on the same dead connection.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is NotLeaderException or DiscoveryException or RpcException { StatusCode: StatusCode.Unavailable }
           || (exception is RpcException { StatusCode: StatusCode.Internal, Status.DebugException: { } debug } && IsBrokenConnection(debug));

    /// <summary>
    ///     True when the HTTP/2 connection failed: the server closed it, or stopped answering keepalive pings. A call
    ///     that fails mid-call reports the <see cref="HttpProtocolException"/> directly. A call that fails while starting
    ///     reports an <see cref="HttpRequestException"/> wrapping it.
    /// </summary>
    private static bool IsBrokenConnection(Exception debug)
    {
        for (var current = debug; current is not null; current = current.InnerException)
        {
            if (current is HttpProtocolException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     Runs <paramref name="action"/>, retrying transient failures with exponential backoff.
    ///     <para>
    ///     A caller that passes a cancellable <paramref name="callerToken"/> decides how long to wait: retries continue
    ///     until it fires, and that surfaces as cancellation. Otherwise the operation is cancelled after the policy's
    ///     timeout, which surfaces as a <see cref="TimeoutException"/> holding the last failure. It must not look like
    ///     cancellation: nobody asked for it, and an attempt cut short leaves the outcome unknown.
    ///     </para>
    ///     <para>
    ///     <paramref name="disposeToken"/> is the adapter's own disposal. It stops the operation but says nothing about
    ///     the caller, so it has no bearing on the timeout.
    ///     </para>
    /// </summary>
    public static Task<T> ExecuteAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task<T>> action, CancellationToken callerToken, CancellationToken disposeToken = default)
        => ExecuteCoreAsync(policy, operation, action, streamingRead: false, callerToken, disposeToken);

    /// <summary>
    ///     As <see cref="ExecuteAsync{T}"/>, for a read of a whole stream whose caller cannot cancel it. The client sets
    ///     no deadline on those and a healthy one can outlast any timeout, so the timeout only stops it being retried,
    ///     not an attempt in progress.
    /// </summary>
    public static Task<T> ExecuteStreamingReadAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task<T>> action, CancellationToken disposeToken)
        => ExecuteCoreAsync(policy, operation, action, streamingRead: true, CancellationToken.None, disposeToken);

    private static async Task<T> ExecuteCoreAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task<T>> action, bool streamingRead, CancellationToken callerToken, CancellationToken disposeToken)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(callerToken, disposeToken);
        if (!callerToken.CanBeCanceled)
        {
            cancellation.CancelAfter(policy.Timeout);
        }

        var token = cancellation.Token;
        var attemptToken = streamingRead ? disposeToken : token;
        Exception? lastFailure = null;

        try
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    return await action(attemptToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransient(ex) && !token.IsCancellationRequested)
                {
                    lastFailure = ex;
                    policy.Metrics.Retries.Add(1, new TagList
                    {
                        { "Operation", operation },
                        { "Exception", ex.GetType().Name },
                    });
                }
                catch (Exception ex) when (IsTransient(ex) && (callerToken.IsCancellationRequested || disposeToken.IsCancellationRequested))
                {
                    // Cancellation landed as the attempt failed: report the cancellation that was asked for, not the failure
                    throw new OperationCanceledException($"{operation} was cancelled.", ex, token);
                }

                await Task.Delay(GetDelay(policy.Options, attempt), token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (token.IsCancellationRequested
                                   && !callerToken.IsCancellationRequested
                                   && !disposeToken.IsCancellationRequested
                                   && (IsTransient(ex) || KurrentExceptionConverter.IsRequestedCancellation(ex, token)))
        {
            throw new TimeoutException($"{operation} did not complete within {policy.Timeout}.", IsTransient(ex) ? ex : lastFailure ?? ex);
        }
    }

    public static Task ExecuteAsync(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task> action, CancellationToken callerToken, CancellationToken disposeToken = default)
        => ExecuteAsync(policy, operation, async ct =>
        {
            await action(ct).ConfigureAwait(false);
            return true;
        }, callerToken, disposeToken);

    internal static TimeSpan GetDelay(KurrentRetryOptions options, int attempt)
    {
        // Capped so that retrying indefinitely cannot overflow the doubling
        var exponential = Math.Min(options.BaseDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 30)), options.MaxDelay.TotalMilliseconds);
#pragma warning disable CA5394 // Random is fine for retry jitter
        var jitter = 0.5 + (Random.Shared.NextDouble() * 0.5);
#pragma warning restore CA5394
        return TimeSpan.FromMilliseconds(exponential * jitter);
    }
}
