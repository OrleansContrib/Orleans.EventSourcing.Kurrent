using System.Collections.Concurrent;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal sealed class ExceptionalKurrentClientWrapper(IKurrentClient passthrough) : IKurrentClient
{
    readonly ConcurrentDictionary<string, (Exception Exception, int Remaining)> dictionaryExceptionToThrowOnceForTesting = new();
    readonly ConcurrentDictionary<string, Exception> setMetadataExceptionToThrowOnce = new();
    readonly ConcurrentDictionary<string, CancellationTokenSource> cancelOnceForTesting = new();

    internal void AddExceptionToThrowOnceForTesting(string streamName, Exception exception) => AddExceptionToThrowForTesting(streamName, exception, 1);

    // Throws for the next count calls that touch the stream, simulating a window such as a leader election
    internal void AddExceptionToThrowForTesting(string streamName, Exception exception, int count) => dictionaryExceptionToThrowOnceForTesting[streamName] = (exception, count);

    internal void AddSetMetadataExceptionToThrowOnceForTesting(string streamName, Exception exception) => setMetadataExceptionToThrowOnce.AddOrUpdate(streamName, exception, (k, v) => v = exception);

    // Simulates the caller's token firing while the next call for the stream is in flight
    internal void CancelOnceForTesting(string streamName, CancellationTokenSource cancellation) => cancelOnceForTesting.AddOrUpdate(streamName, cancellation, (k, v) => v = cancellation);

    private void ThrowIfException(string streamName)
    {
        if (cancelOnceForTesting.Remove(streamName, out var cancellation))
        {
            cancellation.Cancel();
            throw new OperationCanceledException(cancellation.Token);
        }

        while (dictionaryExceptionToThrowOnceForTesting.TryGetValue(streamName, out var entry))
        {
            var claimed = entry.Remaining <= 1
                ? dictionaryExceptionToThrowOnceForTesting.TryRemove(new KeyValuePair<string, (Exception, int)>(streamName, entry))
                : dictionaryExceptionToThrowOnceForTesting.TryUpdate(streamName, (entry.Exception, entry.Remaining - 1), entry);

            if (claimed)
            {
                throw entry.Exception;
            }
        }
    }

    // Applies the next append, delete or tombstone of the stream and then throws, simulating a reply lost after the write landed
    internal void AddExceptionToThrowAfterWriteOnceForTesting(string streamName, Exception exception) => afterWriteExceptionToThrowOnce[streamName] = exception;

    readonly ConcurrentDictionary<string, Exception> afterWriteExceptionToThrowOnce = new();

    private void ThrowIfExceptionAfterWrite(string streamName)
    {
        if (afterWriteExceptionToThrowOnce.Remove(streamName, out var ex))
        {
            throw ex;
        }
    }

    #region IKurrentClient
    public async Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName, StreamState expectedRevision, IEnumerable<EventData> eventData, CancellationToken cancellationToken)
    {
        ThrowIfException(streamName);
        var result = await passthrough.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken).ConfigureAwait(false);
        ThrowIfExceptionAfterWrite(streamName);
        return result;
    }

    public async Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        ThrowIfException(streamName);
        var result = await passthrough.DeleteStreamAsync(streamName, expectedRevision, token).ConfigureAwait(false);
        ThrowIfExceptionAfterWrite(streamName);
        return result;
    }

    public void Dispose() => passthrough.Dispose();

    public ValueTask DisposeAsync() => passthrough.DisposeAsync();

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.GetStreamMetadata(streamName, token);
    }

    public ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
    {
        ThrowIfException(streamName);
        return passthrough.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);
    }

    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
    {
        ThrowIfException(streamName);
        if (setMetadataExceptionToThrowOnce.Remove(streamName, out var metaEx))
        {
            throw metaEx;
        }
        return passthrough.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token);
    }

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => passthrough.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

    public async Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        ThrowIfException(streamName);
        var result = await passthrough.TombstoneStreamAsync(streamName, expectedRevision, token).ConfigureAwait(false);
        ThrowIfExceptionAfterWrite(streamName);
        return result;
    }

    #endregion
}
