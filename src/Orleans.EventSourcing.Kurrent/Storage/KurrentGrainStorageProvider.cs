using System.Diagnostics;
using System.Globalization;

using KurrentDB.Client;

using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Observability;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///      Kurrent-based log consistent storage provider.
/// </summary>
internal sealed class KurrentGrainStorageProvider(IKurrentClient kurrentClient, IEventConverterFactory eventSerializer, IKurrentStreamNameProvider streamNameProvider, KurrentMetrics metrics) : IGrainStorage
{
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

        if (grainState.RecordExists)
        {
            var streamState = ConvertETagToStreamState(grainState.ETag);
            if (streamState != StreamState.NoStream) // https://github.com/kurrent-io/KurrentDB/issues/4637
            {
                try
                {
                    _ = await kurrentClient.DeleteStreamAsync(streamNameProvider.GetStreamName(stateName, grainId), streamState, cancellationToken).ConfigureAwait(false);
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

        var observabilityTags = new TagList
        {
            { "GrainType", grainId.Type },
            { "StateName", stateName },
            { "Type", typeof(T).Name }
        };
        try
        {
            var streamName = streamNameProvider.GetStreamName(stateName, grainId);
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
                    grainState.State = Activator.CreateInstance<T>();
                    grainState.RecordExists = false;
                    grainState.ETag = ConvertStreamStateToETag(StreamState.NoStream);
                    return;
                }

                grainState.ETag = ConvertStreamStateToETag(StreamState.StreamRevision(enumerator.Current.Event.EventNumber));
                grainState.RecordExists = true;

                var sw = Stopwatch.StartNew();
                grainState.State = eventSerializer.GetEventConverter<T>().DeserializeEvent(enumerator.Current);
                metrics.StateDeserializationLatency.Record(sw.ElapsedMilliseconds, observabilityTags);

                // A second event would indicate the stream metadata wasn't written successfully on the first write
                if (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    await EnsureMetadataMaxCountSet(streamName, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
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

    // We need to set the stream metadata to prevent the stream from growing indefinitely
    private async Task EnsureMetadataMaxCountSet(string streamName, CancellationToken token)
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

            var result = await kurrentClient.ConditionalAppendToStreamAsync(streamName,
                                                                            expectedStreamState,
                                                                            [eventRecord],
                                                                            cancellationToken: cancellationToken).ConfigureAwait(false);

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
                await EnsureMetadataMaxCountSet(streamName, cancellationToken).ConfigureAwait(false);
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
