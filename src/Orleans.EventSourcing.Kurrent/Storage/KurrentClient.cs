using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

using Grpc.Core;

using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal sealed class KurrentClient : IKurrentClient
{
    private readonly KurrentDBClient client;
    private readonly Lazy<ServerCapabilitiesProbe> capabilitiesProbe;
    private readonly TimeSpan? defaultDeadline;
    private int disposed;

    public KurrentClient(KurrentDBClientSettings settings)
    {
        client = new(settings);
        capabilitiesProbe = new(() => new ServerCapabilitiesProbe(settings));

        // Infinite or out-of-range deadlines mean no deadline, as they do for the client's own calls.
        defaultDeadline = settings.DefaultDeadline is { } timeout && timeout > TimeSpan.Zero && timeout.TotalMilliseconds < int.MaxValue
            ? timeout
            : null;
    }

    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
        => client.SetStreamMetadataAsync(streamName,
                                         expectedRevision,
                                         streamMetadata,
                                         cancellationToken: token);

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
        => client.GetStreamMetadataAsync(streamName, cancellationToken: token);


    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => client.DeleteAsync(streamName,
                              expectedRevision,
                              cancellationToken: token);

    public async ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction,
                                                      string streamName,
                                                      StreamPosition position,
                                                      long maxCount,
                                                      bool resolveLinkTos,
                                                      CancellationToken cancellationToken)
    {
        var result = client.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken: cancellationToken);
        if (await result.ReadState.ConfigureAwait(false) == ReadState.Ok)
        {
            return new EventResult(result.Messages);
        }
        else
        {
            return StreamDoesNotExistReadResult.Instance;
        }
    }

    private sealed class EventResult(IAsyncEnumerable<StreamMessage> messages) : IStreamReadResult
    {
        public StreamPosition? LastStreamPosition { get; private set; }

        public async IAsyncEnumerator<ResolvedEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await foreach(var message in messages.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                switch (message)
                {
                    case StreamMessage.Event eventMessage:
                        yield return eventMessage.ResolvedEvent;
                        break;
                    case StreamMessage.LastStreamPosition lastStreamPositionMessage:
                        LastStreamPosition = lastStreamPositionMessage.StreamPosition;
                        break;
                }
            }
        }
    }


    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                       StreamState expectedRevision,
                                                                       IEnumerable<EventData> eventData,
                                                                       CancellationToken cancellationToken)
        => client.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken: cancellationToken);

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => client.SubscribeToAll(start, filterOptions: new SubscriptionFilterOptions(eventFilter, checkpointInterval), cancellationToken: cancellationToken).Messages;

    public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => client.TombstoneAsync(streamName, expectedRevision, cancellationToken: token);

    public async ValueTask<bool> SupportsMultiStreamAppendAsync(CancellationToken cancellationToken)
    {
        var capabilities = await capabilitiesProbe.Value.GetServerCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
        return capabilities.SupportsMultiStreamAppend;
    }

    public async ValueTask<MultiStreamAppendResponse> MultiStreamAppendAsync(IReadOnlyList<AppendStreamRequest> requests, CancellationToken cancellationToken)
    {
        // KurrentDB.Client 1.2.0 throws NullReferenceException instead of the server's error whenever a multi-stream
        // append fails with an RpcException that carries no rich status, e.g. a request over the server's
        // MaxAppendSize (ResourceExhausted), or Unavailable/DeadlineExceeded (fixed in KurrentDB.Client 1.4.0).
        // Capture the RpcException as it is thrown, so that it can be rethrown in place of the NullReferenceException.
        var thrown = new StrongBox<RpcException?>();
        EnsureRpcExceptionCaptureRegistered();
        capturedRpcException.Value = thrown;

        // The client sends a multi-stream append as a streaming call, which gets no deadline, unlike a single-stream
        // append, which gets DefaultDeadline. Apply DefaultDeadline here too. On .NET 8, a request over the server's
        // MaxAppendSize can leave the call stuck writing its request stream, where it ignores cancellation, so stop
        // waiting for it at the deadline as well as cancelling it.
        using var deadline = defaultDeadline.HasValue ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken) : null;
        deadline?.CancelAfter(defaultDeadline!.Value);
        var append = client.MultiStreamAppendAsync(requests, deadline?.Token ?? cancellationToken).AsTask();
        try
        {
            return await (defaultDeadline is { } timeout ? append.WaitAsync(timeout, cancellationToken) : append).ConfigureAwait(false);
        }
        catch (Exception ex) when ((ex is TimeoutException || deadline is { IsCancellationRequested: true }) && !cancellationToken.IsCancellationRequested)
        {
            if (deadline is not null)
            {
                await deadline.CancelAsync().ConfigureAwait(false);
            }

            ObserveAbandoned(append);
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, $"The multi-stream append did not complete within the client's DefaultDeadline of {defaultDeadline}.", ex));
        }
        catch (OperationCanceledException) when (!append.IsCompleted)
        {
            ObserveAbandoned(append);
            throw;
        }
        catch (NullReferenceException) when (thrown.Value is { } rpcException)
        {
            ExceptionDispatchInfo.Throw(rpcException);
            throw;
        }
        finally
        {
            capturedRpcException.Value = null;
        }
    }

    // An append that is no longer awaited may still fail later; observe it so it is not an unobserved task exception.
    private static void ObserveAbandoned(Task task)
        => task.ContinueWith(static x => _ = x.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

    private static readonly AsyncLocal<StrongBox<RpcException?>?> capturedRpcException = new();
    private static int rpcExceptionCaptureRegistered;

    private static void EnsureRpcExceptionCaptureRegistered()
    {
        if (Interlocked.Exchange(ref rpcExceptionCaptureRegistered, 1) == 0)
        {
            AppDomain.CurrentDomain.FirstChanceException += static (_, e) =>
            {
                if (e.Exception is RpcException rpcException && capturedRpcException.Value is { } box)
                {
                    box.Value = rpcException;
                }
            };
        }
    }

    // Disposal must be idempotent: DI can track the same instance under several registrations, and
    // KurrentDBClient throws ObjectDisposedException when disposed twice.
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        client.Dispose();
        if (capabilitiesProbe.IsValueCreated)
        {
            capabilitiesProbe.Value.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        await client.DisposeAsync().ConfigureAwait(false);
        if (capabilitiesProbe.IsValueCreated)
        {
            await capabilitiesProbe.Value.DisposeAsync().ConfigureAwait(false);
        }
    }

    // KurrentDBClient is sealed and only exposes the server capabilities it discovers on connect through the
    // protected KurrentDBClientBase.GetChannelInfo, so this minimal subclass exists solely to read them.
    // It keeps its own channel, which is cached per node, so repeated calls are cheap.
    private sealed class ServerCapabilitiesProbe(KurrentDBClientSettings settings)
        : KurrentDBClientBase(settings, new Dictionary<string, Func<Grpc.Core.RpcException, Exception>>())
    {
        public async ValueTask<ServerCapabilities> GetServerCapabilitiesAsync(CancellationToken cancellationToken)
            => (await GetChannelInfo(cancellationToken).ConfigureAwait(false)).ServerCapabilities;
    }
}
