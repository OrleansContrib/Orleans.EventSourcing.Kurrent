using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal sealed class ExceptionalKurrentClientWrapper(IKurrentClient passthrough) : IKurrentClient
{
    readonly ConcurrentDictionary<string, (Exception Exception, int Remaining)> dictionaryExceptionToThrowOnceForTesting = new();
    readonly ConcurrentDictionary<string, Exception> setMetadataExceptionToThrowOnce = new();
    Exception? subscriptionExceptionToThrowOnce;
    int subscriptionFailuresRemaining;

    internal void AddExceptionToThrowOnceForTesting(string streamName, Exception exception) => AddExceptionToThrowForTesting(streamName, exception, 1);

    /// <summary>Throws <paramref name="exception"/> for the next <paramref name="count"/> operations that touch <paramref name="streamName"/>, simulating a failure window (e.g. a leadership election).</summary>
    internal void AddExceptionToThrowForTesting(string streamName, Exception exception, int count) => dictionaryExceptionToThrowOnceForTesting[streamName] = (exception, count);

    internal void AddSetMetadataExceptionToThrowOnceForTesting(string streamName, Exception exception) => setMetadataExceptionToThrowOnce.AddOrUpdate(streamName, exception, (k, v) => v = exception);

    /// <summary>Drops the next catch-up subscription with <paramref name="exception"/>, simulating a subscription dropped by a leader change.</summary>
    internal void AddSubscriptionExceptionToThrowOnceForTesting(Exception exception) => AddSubscriptionExceptionToThrowForTesting(exception, 1);

    /// <summary>Drops the next <paramref name="count"/> catch-up subscription attempts with <paramref name="exception"/>.</summary>
    internal void AddSubscriptionExceptionToThrowForTesting(Exception exception, int count)
    {
        subscriptionExceptionToThrowOnce = exception;
        Volatile.Write(ref subscriptionFailuresRemaining, count);
    }

    private void ThrowIfException(string streamName)
    {
        while (dictionaryExceptionToThrowOnceForTesting.TryGetValue(streamName, out var entry))
        {
            var removedOrDecremented = entry.Remaining <= 1
                ? dictionaryExceptionToThrowOnceForTesting.TryRemove(new KeyValuePair<string, (Exception, int)>(streamName, entry))
                : dictionaryExceptionToThrowOnceForTesting.TryUpdate(streamName, (entry.Exception, entry.Remaining - 1), entry);

            if (removedOrDecremented)
            {
                throw entry.Exception;
            }
        }
    }

    #region IKurrentClient
    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName, StreamState expectedRevision, IEnumerable<EventData> eventData, CancellationToken cancellationToken)
    {
        ThrowIfException(streamName);
        return passthrough.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken);
    }

    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.DeleteStreamAsync(streamName, expectedRevision, token);
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

    public async IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (subscriptionExceptionToThrowOnce is { } ex && Interlocked.Decrement(ref subscriptionFailuresRemaining) >= 0)
        {
            throw ex;
        }

        await foreach (var message in passthrough.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return message;
        }
    }

    public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.TombstoneStreamAsync(streamName, expectedRevision, token);
    }

    #endregion
}
