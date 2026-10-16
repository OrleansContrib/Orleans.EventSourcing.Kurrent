using System.Buffers;

using KurrentDB.Client;

using Microsoft.Extensions.Logging;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Journaling;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Stores one Orleans journal in one KurrentDB stream.
/// </summary>
/// <remarks>
///     <para>
///     Each append becomes one or more events; each compaction appends a snapshot and moves the stream's
///     <c>$tb</c> (truncate before) metadata to it so older events are hidden and later scavenged. When the server
///     supports multi-stream appends, the snapshot and the metadata are written in one atomic operation.
///     </para>
///     <para>
///     Optimistic concurrency uses the stream revision observed by the last read or write, so a second activation
///     writing the same journal causes <see cref="InconsistentStateException"/>.
///     Orleans serializes calls to a storage instance, so this type is not thread-safe.
///     </para>
/// </remarks>
internal sealed class KurrentJournalStorage : IJournalStorage
{
    private readonly string streamName;
    private readonly string metastreamName;
    private readonly byte[] header;
    private readonly IKurrentClient client;
    private readonly KurrentJournalStorageOptions options;
    private readonly ILogger logger;

    private StreamState streamState = StreamState.NoStream;
    private long eventsSinceSnapshot;
    private long bytesSinceSnapshot;

    public KurrentJournalStorage(string streamName, string formatKey, IKurrentClient client, KurrentJournalStorageOptions options, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamName);
        if (streamName.StartsWith('$'))
        {
            throw new ArgumentException($"Journal stream name '{streamName}' must not start with '$', which KurrentDB reserves for system streams.", nameof(streamName));
        }

        this.streamName = streamName;
        metastreamName = StreamMetadataJson.GetMetastreamName(streamName);
        header = KurrentJournalEvents.CreateHeader(formatKey);
        this.client = client;
        this.options = options;
        this.logger = logger;
    }

    internal string StreamName => streamName;

    /// <inheritdoc/>
    public bool IsCompactionRequested
        => eventsSinceSnapshot >= options.CompactionEventCountThreshold
           || bytesSinceSnapshot >= options.CompactionSizeThreshold;

    /// <inheritdoc/>
    public async ValueTask ReadAsync(IJournalStorageConsumer consumer, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        // Read backwards until the first event of the latest snapshot (or the start of the stream). Recovery must
        // not replay anything before that snapshot, even if a failed compaction left older events untruncated.
        var events = new List<JournalEvent>();
        var lastRevision = StreamState.NoStream;
        var position = StreamPosition.End;
        var foundStart = false;
        while (!foundStart)
        {
            var result = await client.ReadStreamAsync(Direction.Backwards, streamName, position, options.ReadPageSize, resolveLinkTos: false, cancellationToken).ConfigureAwait(false);

            var count = 0;
            var lowestEventNumber = 0UL;
            await foreach (var resolvedEvent in result.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                count++;
                lowestEventNumber = resolvedEvent.Event.EventNumber.ToUInt64();
                if (lastRevision == StreamState.NoStream)
                {
                    lastRevision = StreamState.StreamRevision(lowestEventNumber);
                }

                var journalEvent = KurrentJournalEvents.Parse(resolvedEvent.Event);
                events.Add(journalEvent);
                if (journalEvent.Kind == JournalEventKind.Snapshot)
                {
                    foundStart = true;
                    break;
                }
            }

            // A short page means the start of the stream, or its truncation point, was reached.
            if (count < options.ReadPageSize || lowestEventNumber == 0)
            {
                foundStart = true;
            }
            else
            {
                position = new StreamPosition(lowestEventNumber - 1);
            }
        }

        events.Reverse();

        streamState = lastRevision;
        eventsSinceSnapshot = 0;
        bytesSinceSnapshot = 0;
        foreach (var journalEvent in events)
        {
            if (journalEvent.Kind == JournalEventKind.Append)
            {
                eventsSinceSnapshot++;
                bytesSinceSnapshot += journalEvent.Payload.Length;
            }
        }

        if (events.Count == 0)
        {
            consumer.Complete(metadata: null);
            return;
        }

        // Orleans requires a single format for the whole read. A format migration always writes a snapshot first,
        // so everything after the latest snapshot shares one format key.
        var formatKey = events[0].FormatKey;
        if (events.Any(x => !string.Equals(x.FormatKey, formatKey, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException($"Journal stream '{streamName}' mixes journal format keys after its latest snapshot.");
        }

        consumer.Read(events.Select(x => x.Payload), new JournalMetadata(formatKey), complete: true);
    }

    /// <inheritdoc/>
    public async ValueTask AppendAsync(ReadOnlySequence<byte> value, CancellationToken cancellationToken)
    {
        if (value.IsEmpty)
        {
            return;
        }

        var events = KurrentJournalEvents.Create(value, header, isSnapshot: false, options.MaxEventSize);
        await AppendCoreAsync(events, cancellationToken).ConfigureAwait(false);

        eventsSinceSnapshot += events.Length;
        bytesSinceSnapshot += value.Length;
    }

    /// <inheritdoc/>
    public async ValueTask ReplaceAsync(ReadOnlySequence<byte> value, CancellationToken cancellationToken)
    {
        var events = KurrentJournalEvents.Create(value, header, isSnapshot: true, options.MaxEventSize);

        // A new (or deleted) stream has nothing before the snapshot to truncate.
        if (streamState == StreamState.NoStream)
        {
            await AppendCoreAsync(events, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var snapshotStart = StreamPosition.FromInt64(streamState.ToInt64() + 1);
            if (await UseAtomicCompactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await AppendSnapshotAtomicallyAsync(events, snapshotStart, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await AppendCoreAsync(events, cancellationToken).ConfigureAwait(false);
                await TryTruncateBeforeAsync(snapshotStart, cancellationToken).ConfigureAwait(false);
            }
        }

        eventsSinceSnapshot = 0;
        bytesSinceSnapshot = 0;
    }

    /// <inheritdoc/>
    public async ValueTask DeleteAsync(CancellationToken cancellationToken)
    {
        if (streamState != StreamState.NoStream)
        {
            // A soft delete, so the journal can be written again later; a tombstone would prevent that.
            try
            {
                await client.DeleteStreamAsync(streamName, streamState, cancellationToken).ConfigureAwait(false);
            }
            catch (WrongExpectedVersionException ex)
            {
                throw new InconsistentStateException($"Journal stream '{streamName}' was changed by another writer.", $"{ex.ActualStreamState}", $"{streamState}", ex);
            }
        }

        streamState = StreamState.NoStream;
        eventsSinceSnapshot = 0;
        bytesSinceSnapshot = 0;
    }

    private async ValueTask AppendCoreAsync(EventData[] events, CancellationToken cancellationToken)
    {
        ConditionalWriteResult result;
        try
        {
            result = await client.ConditionalAppendToStreamAsync(streamName, streamState, events, cancellationToken).ConfigureAwait(false);
        }
        catch (WrongExpectedVersionException ex)
        {
            throw new InconsistentStateException($"Journal stream '{streamName}' was changed by another writer.", $"{ex.ActualStreamState}", $"{streamState}", ex);
        }

        if (result.Status != ConditionalWriteStatus.Succeeded)
        {
            throw new InconsistentStateException($"Journal stream '{streamName}' was changed by another writer ({result.Status}).", $"{result.NextExpectedStreamState}", $"{streamState}");
        }

        streamState = result.NextExpectedStreamState;
    }

    private async ValueTask<bool> UseAtomicCompactionAsync(CancellationToken cancellationToken)
    {
        switch (options.AtomicCompaction)
        {
            case AtomicCompactionMode.Disabled:
                return false;
            case AtomicCompactionMode.Required:
                if (!await client.SupportsMultiStreamAppendAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new NotSupportedException(
                        $"{nameof(KurrentJournalStorageOptions.AtomicCompaction)} is {AtomicCompactionMode.Required}, but the KurrentDB server does not support multi-stream appends (requires 25.1 or later).");
                }
                return true;
            default:
                return await client.SupportsMultiStreamAppendAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // Appends the snapshot and sets $tb on the stream's metastream in one multi-stream append, so the
    // truncation can never be lost between two writes.
    private async ValueTask AppendSnapshotAtomicallyAsync(EventData[] events, StreamPosition snapshotStart, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            // Preserve whatever else is in the metadata (ACLs, max age, custom properties) and guard against
            // concurrent metadata changes with the metastream's revision.
            var metadata = await client.GetStreamMetadata(streamName, cancellationToken).ConfigureAwait(false);
            var metastreamState = metadata.MetastreamRevision.HasValue
                ? StreamState.StreamRevision(metadata.MetastreamRevision.Value)
                : StreamState.NoStream;

            AppendStreamRequest[] requests =
            [
                new(streamName, streamState, events),
                new(metastreamName, metastreamState, [StreamMetadataJson.CreateMetadataEvent(metadata.Metadata.WithTruncateBefore(snapshotStart))]),
            ];

            MultiStreamAppendResponse response;
            try
            {
                response = await client.MultiStreamAppendAsync(requests, cancellationToken).ConfigureAwait(false);
            }
            catch (WrongExpectedVersionException ex) when (ex.StreamName == metastreamName && attempt < options.MaxMetadataConflictRetries)
            {
                // Only the metadata changed; the journal itself is still ours. Re-read the metadata and try again.
                logger.CompactionMetadataConflict(streamName, attempt + 1, ex);
                continue;
            }
            catch (WrongExpectedVersionException ex)
            {
                throw new InconsistentStateException($"Journal stream '{streamName}' was changed by another writer.", $"{ex.ActualStreamState}", $"{streamState}", ex);
            }

            var streamResponse = response.Responses?.FirstOrDefault(x => x.Stream == streamName)
                ?? throw new InvalidOperationException($"The multi-stream append response did not include journal stream '{streamName}'.");

            streamState = StreamState.StreamRevision(checked((ulong)streamResponse.StreamRevision));
            return;
        }
    }

    // Best-effort truncation after the snapshot was appended in a separate write. Failing here does not lose data
    // or affect correctness: recovery starts from the latest snapshot regardless, so older events merely remain
    // until the next successful compaction.
    private async ValueTask TryTruncateBeforeAsync(StreamPosition snapshotStart, CancellationToken cancellationToken)
    {
        try
        {
            var metadata = await client.GetStreamMetadata(streamName, cancellationToken).ConfigureAwait(false);
            var metastreamState = metadata.MetastreamRevision.HasValue
                ? StreamState.StreamRevision(metadata.MetastreamRevision.Value)
                : StreamState.NoStream;

            await client.SetStreamMetadata(streamName, metastreamState, metadata.Metadata.WithTruncateBefore(snapshotStart), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.TruncateFailed(streamName, snapshotStart.ToUInt64(), ex);
        }
    }
}
