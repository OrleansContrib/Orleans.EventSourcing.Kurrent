using System.Diagnostics;
using System.Globalization;

using KurrentDB.Client;

using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Observability;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///      Kurrent-based log consistent storage provider.
///      Reads, writes and deletes are retried across a leader change (see <see cref="KurrentRetry"/>).
/// </summary>
internal sealed class KurrentGrainStorageProvider(IKurrentClient kurrentClient, IEventConverterFactory eventSerializer, IKurrentStreamNameProvider streamNameProvider, KurrentMetrics metrics, KurrentRetryPolicy retry) : IGrainStorage
{
    private sealed record StoredState<T>(StreamState Revision, T State, bool HasOlderEvents);

    private static StreamState ConvertETagToStreamState(string? eTag)
    {
        if (ulong.TryParse(eTag, CultureInfo.InvariantCulture, out var streamState))
        {
            return StreamState.StreamRevision(streamState);
        }
        else
        {
            return StreamState.NoStream;
        }
    }

    private static string ConvertStreamStateToETag(StreamState streamState)
    {
        if (streamState == StreamState.NoStream)
        {
            return string.Empty;
        }
        else
        {
            return streamState.ToInt64().ToString(CultureInfo.InvariantCulture);
        }
    }

    /// <inheritdoc/>
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => ClearStateAsync(stateName, grainId, grainState, CancellationToken.None);

    /// <inheritdoc/>
    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var startTimestamp = Stopwatch.GetTimestamp();

        if (grainState.RecordExists)
        {
            var streamState = ConvertETagToStreamState(grainState.ETag);
            if (streamState != StreamState.NoStream) // https://github.com/kurrent-io/KurrentDB/issues/4637
            {
                try
                {
                    // Resending a soft delete that already landed succeeds again, so an unknown outcome is safe to retry
                    var streamName = streamNameProvider.GetStreamName(stateName, grainId);
                    _ = await KurrentRetry.ExecuteAsync(retry, "ClearState", token => kurrentClient.DeleteStreamAsync(streamName, streamState, token), cancellationToken, startTimestamp).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw KurrentExceptionConverter.ConvertException(ex);
                }

                grainState.State = Activator.CreateInstance<T>();
                grainState.RecordExists = false;
                grainState.ETag = string.Empty;
            }
        }
    }

    /// <inheritdoc/>
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => ReadStateAsync(stateName, grainId, grainState, CancellationToken.None);

    /// <inheritdoc/>
    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var startTimestamp = Stopwatch.GetTimestamp();

        var observabilityTags = new TagList
        {
            { "GrainType", grainId.Type },
            { "StateName", stateName },
            { "Type", typeof(T).Name }
        };
        try
        {
            var streamName = streamNameProvider.GetStreamName(stateName, grainId);

            // Retry the whole read, not just opening it, and only update grainState once an attempt succeeds
            var stored = await KurrentRetry.ExecuteAsync(retry, "ReadState", token => ReadLatestAsync<T>(streamName, observabilityTags, token), cancellationToken, startTimestamp).ConfigureAwait(false);
            if (stored is null)
            {
                grainState.State = Activator.CreateInstance<T>();
                grainState.RecordExists = false;
                grainState.ETag = ConvertStreamStateToETag(StreamState.NoStream);
                return;
            }

            grainState.ETag = ConvertStreamStateToETag(stored.Revision);
            grainState.RecordExists = true;
            grainState.State = stored.State;

            // A second event would indicate the stream metadata wasn't written successfully on the first write
            if (stored.HasOlderEvents)
            {
                await EnsureMetadataMaxCountSet(streamName, startTimestamp, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (KurrentExceptionConverter.IsRequestedCancellation(ex, cancellationToken))
        {
            // A read has no side effects, so caller-requested cancellation surfaces as cancellation
            throw new OperationCanceledException("Reading grain state was cancelled.", ex, cancellationToken);
        }
        catch (Exception ex)
        {
            throw KurrentExceptionConverter.ConvertException(ex);
        }
    }

    /// <summary>
    ///     Reads the latest state event, and whether an older one is still there. Returns null if there is no state.
    /// </summary>
    private async Task<StoredState<T>?> ReadLatestAsync<T>(string streamName, TagList observabilityTags, CancellationToken cancellationToken)
    {
        var readResult = await kurrentClient.ReadStreamAsync(Direction.Backwards,
                                                              streamName,
                                                              StreamPosition.End,
                                                              2,
                                                              false,
                                                              cancellationToken).ConfigureAwait(false);

        var enumerator = readResult.GetAsyncEnumerator(cancellationToken);
        try
        {
            if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return null;
            }

            var revision = StreamState.StreamRevision(enumerator.Current.Event.EventNumber);

            var sw = Stopwatch.StartNew();
            var state = eventSerializer.GetEventConverter<T>().DeserializeEvent(enumerator.Current);
            metrics.StateDeserializationLatency.Record(sw.ElapsedMilliseconds, observabilityTags);

            return new StoredState<T>(revision, state, await enumerator.MoveNextAsync().ConfigureAwait(false));
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    // We need to set the stream metadata to prevent the stream from growing indefinitely.
    // The read and the conditional write are retried together, so a retry after a write that did land sees MaxCount set and does nothing.
    private Task EnsureMetadataMaxCountSet(string streamName, long startTimestamp, CancellationToken token)
        => KurrentRetry.ExecuteAsync(retry, "SetMaxCount", t => SetMaxCountAsync(streamName, t), token, startTimestamp);

    private async Task SetMaxCountAsync(string streamName, CancellationToken token)
    {
        // This stream may have metadata from a previous soft-delete
        // so we need to try to read the stream metadata first to get its stream revision and if MaxCount is set.
        var metadata = await kurrentClient.GetStreamMetadata(streamName, token).ConfigureAwait(false);

        // If it is not set (we explicitly don't care what it's set to, as it may be set to >1 for troublehsooting purposes)
        // we set it to 1
        if (!(metadata is { Metadata.MaxCount: not null }))
        {
            await kurrentClient.SetStreamMetadata(streamName,
                                                  metadata.MetastreamRevision.HasValue ? StreamState.StreamRevision(metadata.MetastreamRevision.Value) : StreamState.NoStream,
                                                  new StreamMetadata(
                                                        1,
                                                        metadata.Metadata.MaxAge,
                                                        metadata.Metadata.TruncateBefore,
                                                        metadata.Metadata.CacheControl,
                                                        metadata.Metadata.Acl,
                                                        metadata.Metadata.CustomMetadata),
                                                  token).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        => WriteStateAsync(stateName, grainId, grainState, CancellationToken.None);

    /// <inheritdoc/>
    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var startTimestamp = Stopwatch.GetTimestamp();

        var observabilityTags = new TagList
        {
            { "GrainType", grainId.Type },
            { "StateName", stateName },
            { "Type", typeof(T).Name }
        };
        try
        {
            var streamName = streamNameProvider.GetStreamName(stateName, grainId);
            var expectedStreamState = ConvertETagToStreamState(grainState.ETag);

            if (grainState.State is not { } state)
            {
                throw new ArgumentNullException(nameof(grainState), "Grain state cannot be null.");
            }

            var sw = Stopwatch.StartNew();
            var eventRecord = eventSerializer.GetEventConverter<T>().SerializeEvent(state);
            metrics.StateSerializationLatency.Record(sw.ElapsedMilliseconds, observabilityTags);

            // Serialized once, so every attempt resends the same event ID at the same expected revision and Kurrent
            // acknowledges a resend of an append that already landed instead of writing it twice
            EventData[] events = [eventRecord];
            var result = await KurrentRetry.ExecuteAsync(retry, "WriteState", token => kurrentClient.ConditionalAppendToStreamAsync(streamName,
                                                                                                                                            expectedStreamState,
                                                                                                                                            events,
                                                                                                                                            cancellationToken: token), cancellationToken, startTimestamp).ConfigureAwait(false);

            switch (result.Status)
            {
                case ConditionalWriteStatus.VersionMismatch:
                    throw new InconsistentStateException("StreamState version mismatch", ConvertStreamStateToETag(result.NextExpectedStreamState), grainState.ETag);
                case ConditionalWriteStatus.Succeeded:
                    break;
                default:
                    throw new NotSupportedException($"Unexpected result from Kurrent: {result.Status}");
            }

            // Important: The metadata operations must occur AFTER the write as if the stream has previously been soft deleted
            // it will have truncatebefore: long.MaxValue metadata which is how Kurrent achieves soft delete.

            // If we change the metadata before the first new write: we hit VersionMismatch on write.
            // If we change the metadata after the first new write: everything works as expected.

            // There is a small risk we fail between the write above and metadata write below, so 
            // on read we also write maxCount metadata if we find more than one event.

            // We set it to 1, so we only retain the last event
            if (expectedStreamState == StreamState.NoStream)
            {
                await EnsureMetadataMaxCountSet(streamName, startTimestamp, cancellationToken).ConfigureAwait(false);
            }

            grainState.RecordExists = true;
            grainState.ETag = ConvertStreamStateToETag(result.NextExpectedStreamState);
        }
        catch (Exception ex)
        {
            throw KurrentExceptionConverter.ConvertException(ex);
        }
    }
}
