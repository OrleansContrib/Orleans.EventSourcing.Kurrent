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
    internal static readonly TimeSpan MaxTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    public static KurrentRetryPolicy Create(IServiceProvider serviceProvider, KurrentStorageOptions options)
    {
        var responseTimeout = serviceProvider.GetService<IOptions<SiloMessagingOptions>>()?.Value.ResponseTimeout ?? DefaultResponseTimeout;
        return new KurrentRetryPolicy(options.Retry,
                                      serviceProvider.GetRequiredService<KurrentMetrics>(),
                                      responseTimeout > MaxTimeout ? System.Threading.Timeout.InfiniteTimeSpan : responseTimeout);
    }
}

/// <summary>
///     Lets an attempt at a streaming read say that it received data, so that time spent reading isn't counted as
///     time spent failing.
/// </summary>
internal sealed class KurrentReadProgress
{
    private bool reported;

    /// <summary>
    ///     Called by the read for each event it receives.
    /// </summary>
    public void Report() => reported = true;

    /// <summary>
    ///     Whether progress was reported since the last call, clearing it for the next attempt.
    /// </summary>
    internal bool TakeReported()
    {
        var result = reported;
        reported = false;
        return result;
    }
}

/// <summary>
///     Retries Kurrent operations that fail because the node the client talks to is not, or is no longer, the leader.
/// </summary>
internal static class KurrentRetry
{
    /// <summary>
    ///     True for failures that clear once the client has found the new leader or reconnected.
    ///     <see cref="NotLeaderException"/> and <see cref="DiscoveryException"/> are raised before the request is
    ///     accepted. An <see cref="StatusCode.Unavailable"/> failure, or a failed HTTP/2 stream or connection (reset,
    ///     closed or aborted by the server, or a keepalive ping that went unanswered, mid-call or while the call was
    ///     starting; see <see cref="IsBrokenConnection"/>), leaves the outcome unknown; that is safe because an append is only retried
    ///     with the same event IDs and expected version, which Kurrent treats as idempotent. <see cref="StatusCode.DeadlineExceeded"/> is
    ///     not retried: the client does not reconnect after it, so a retry would hang on the same dead connection.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is NotLeaderException or DiscoveryException or RpcException { StatusCode: StatusCode.Unavailable }
           || (exception is RpcException { StatusCode: StatusCode.Internal, Status.DebugException: { } debug } && IsBrokenConnection(debug));

    // The one HTTP/2 error code (RFC 9113 section 7) a retry can't clear: the endpoint doesn't support HTTP/2
    private const long Http2Http11Required = 0xd;

    /// <summary>
    ///     True when the server reset the call's HTTP/2 stream, or closed or aborted its connection, with any error code
    ///     but HTTP_1_1_REQUIRED, or when the node stopped answering keepalive pings (.NET reports PROTOCOL_ERROR). A call
    ///     that fails mid-call reports the <see cref="HttpProtocolException"/> directly. A call that fails while starting
    ///     reports an <see cref="HttpRequestException"/> wrapping it.
    ///     <para>
    ///     The error code says why the server gave up on the call, not whether a retry would succeed: a node whose
    ///     connection faulted can send any of them (KurrentDB sends STREAM_CLOSED when a frozen leader resumes), and the
    ///     code alone can't tell that apart from a bug. So every code is retried, as HTTP resilience libraries retry any
    ///     failed request; one that won't clear costs only retries until the timeout. HTTP_1_1_REQUIRED is the exception,
    ///     since it comes from an endpoint that will never accept the call. Codes that Grpc.Net.Client reports as another
    ///     status don't reach here: REFUSED_STREAM is <see cref="StatusCode.Unavailable"/>, and so retried, while CANCEL,
    ///     ENHANCE_YOUR_CALM and INADEQUATE_SECURITY are not.
    ///     </para>
    /// </summary>
    private static bool IsBrokenConnection(Exception debug)
    {
        for (var current = debug; current is not null; current = current.InnerException)
        {
            if (current is HttpProtocolException protocolException)
            {
                return protocolException.ErrorCode != Http2Http11Required;
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
        => ExecuteCoreAsync(policy, operation, action, readProgress: null, callerToken, disposeToken);

    /// <summary>
    ///     As <see cref="ExecuteAsync{T}"/>, for a read of a whole stream whose caller cannot cancel it. The client sets
    ///     no deadline on those and a healthy one can outlast any timeout, so the timeout never stops an attempt in
    ///     progress. It only limits how long the read may keep failing: the time since it first failed, or since an
    ///     attempt last reported progress through <see cref="KurrentReadProgress.Report"/>. Time spent reading must
    ///     not use up the retries.
    /// </summary>
    public static Task<T> ExecuteStreamingReadAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, KurrentReadProgress, Task<T>> action, CancellationToken disposeToken)
    {
        var progress = new KurrentReadProgress();
        return ExecuteCoreAsync(policy, operation, token => action(token, progress), progress, CancellationToken.None, disposeToken);
    }

    private static async Task<T> ExecuteCoreAsync<T>(KurrentRetryPolicy policy, string operation, Func<CancellationToken, Task<T>> action, KurrentReadProgress? readProgress, CancellationToken callerToken, CancellationToken disposeToken)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(callerToken, disposeToken);
        if (!callerToken.CanBeCanceled && readProgress is null)
        {
            cancellation.CancelAfter(policy.Timeout);
        }

        // For a streaming read nothing but disposal cancels this: its timeout is checked as each attempt fails
        var token = cancellation.Token;
        Exception? lastFailure = null;
        long failingSince = 0;

        try
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    return await action(token).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransient(ex) && !token.IsCancellationRequested)
                {
                    if (readProgress is not null)
                    {
                        // Always taken, so progress from this attempt can't be credited to the next one
                        var progressed = readProgress.TakeReported();
                        if (lastFailure is null || progressed)
                        {
                            failingSince = Stopwatch.GetTimestamp();
                        }
                        else if (policy.Timeout != System.Threading.Timeout.InfiniteTimeSpan && Stopwatch.GetElapsedTime(failingSince) >= policy.Timeout)
                        {
                            throw new TimeoutException($"{operation} was still failing {policy.Timeout} after it last made progress.", ex);
                        }
                    }

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
