using Grpc.Core;

using KurrentDB.Client;

using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal static class KurrentExceptionConverter
{
    /// <summary>
    ///     Whether <paramref name="exception"/> is the client observing cancellation the caller requested through <paramref name="cancellationToken"/>.
    ///     gRPC reports this as <see cref="RpcException"/> with <see cref="StatusCode.Cancelled"/> unless configured to throw <see cref="OperationCanceledException"/>.
    /// </summary>
    public static bool IsRequestedCancellation(Exception exception, CancellationToken cancellationToken)
        => cancellationToken.IsCancellationRequested
           && exception is OperationCanceledException or RpcException { StatusCode: StatusCode.Cancelled };

    public static InconsistentStateException ConvertException(Exception exception) => exception switch
    {
        // Passthrough
        InconsistentStateException inconsistentStateException => inconsistentStateException,

        // Pass as wrapped Innerexception
        RpcException or NotLeaderException or DiscoveryException or TimeoutException or ArgumentNullException or InvalidOperationException or ArgumentOutOfRangeException => new InconsistentStateException($"Kurrent encountered a problem: ${exception.Message}", exception),
        WrongExpectedVersionException ex => new InconsistentStateException($"Version mismatch: ${exception.Message}", $"{ex.ActualVersion:N}", $"{ex.ExpectedVersion:N}", ex),
        StreamDeletedException ex => new InconsistentStateException($"Stream deleted: ${exception.Message}", ex),

        // Cancelled mid-operation: a write or delete may already have been applied, so the grain's view of the stream is uncertain
        OperationCanceledException ex => new InconsistentStateException($"Kurrent operation was cancelled and its outcome is unknown: {exception.Message}", ex),

        // Replaced by InconsistentStateException with message, as actual exception may not be serializable
        _ => new InconsistentStateException($"Kurrent encountered an exception {exception.Message}"),
    };
}
