using KurrentDB.Client;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace Orleans.EventSourcing.Kurrent.Storage;

// A pretend subset of Kurrent implemented entirely in memory
internal sealed class InMemoryKurrentClient : IKurrentClient
{
#pragma warning disable CA2213 // Disposable fields should be disposed
    private readonly SemaphoreSlim singleAccess = new(1);
    private readonly Dictionary<string, List<int>> streams = [];
    private readonly Dictionary<string, List<StreamMetadata>> streamMetadata = [];
    private readonly HashSet<string> tombstoned = [];
    private readonly Collection<EventRecord> all = [];
    private readonly BroadcastPublisher<EventRecord> newEventWatcher = new();
#pragma warning restore CA2213 // Disposable fields should be disposed

    private static readonly ConstructorInfo ConditionalWriteResultConstructor = typeof(ConditionalWriteResult).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
                                                                                                                              null,
                                                                                                                              [typeof(StreamState), typeof(Position), typeof(ConditionalWriteStatus),],
                                                                                                                              null)!;

    // Because this type is statically rooted
    // and reused, we do not clean-up
    public void Dispose() { }

    public ValueTask DisposeAsync()
        =>ValueTask.CompletedTask;
    

    public async ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction,
                                      string streamName,
                                      StreamPosition revision,
                                      long maxCount,
                                      bool resolveLinkTos,
                                      CancellationToken cancellationToken)
    {
        await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {

            if (!streams.TryGetValue(streamName, out var events))
            {
                return StreamDoesNotExistReadResult.Instance;
            }

            var metadata = GetStreamMetadataCore(streamName);

            // --- LINQ Pipeline Setup ---
            var query = all.Where(x => x.EventStreamId == streamName); // Start with the full list of events for the stream

            if (metadata.Metadata.TruncateBefore is { } truncateBefore)
            {
                // Filter out events before the metadata truncate point
                query = query.Where(e => e.EventNumber.CompareTo(truncateBefore) >= 0);
            }

            // Filter based on direction and revision
            if (direction == Direction.Forwards)
            {
                // Reading forwards: include events AT or AFTER the revision
                query = query.Where(e => e.EventStreamId == streamName && e.EventNumber.CompareTo(revision) >= 0);
                if (metadata.Metadata.MaxCount is { } metadataMaxCount)
                {
                    query = query.Skip(events.Count - metadataMaxCount);
                }
            }
            else // Direction.Backwards
            {
                // Reading backwards: include events AT or BEFORE the revision
                query = query.Where(e => e.EventNumber.CompareTo(revision) <= 0);


                // Crucial for backwards: Reverse the *filtered* sequence
                // we process events from the revision point backwards.
                query = query.Reverse();

                if (metadata.Metadata.MaxCount is { } metadataMaxCount)
                {
                    query = query.Take(metadataMaxCount);
                }
            }

            // Take requires an int, ensure maxCount doesn't overflow int.MaxValue
            var countToTake = (int)Math.Min(maxCount, int.MaxValue);
            query = query.Take(countToTake);
    
            // Materialize while holding singleAccess: the result is enumerated after release, when concurrent appends may be modifying the event list
            return new InMemoryResult(query.Select(x => new ResolvedEvent(x, null, null)).ToArray().ToAsyncEnumerable(), StreamPosition.FromStreamRevision(checked((ulong)events.Count - 1)));
        }
        finally
        {
            singleAccess.Release();
        }
    }

    internal sealed record InMemoryResult(IAsyncEnumerable<ResolvedEvent> AsyncEnumerable, StreamPosition? LastStreamPosition) : IStreamReadResult
    {     
        public IAsyncEnumerator<ResolvedEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => AsyncEnumerable.GetAsyncEnumerator(cancellationToken);    
    }

    public async Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                       StreamState expectedRevision,
                                                                       IEnumerable<EventData> eventData,
                                                                       CancellationToken cancellationToken)
    {
        await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (tombstoned.Contains(streamName))
            {
                throw new StreamDeletedException(streamName);
            }

            var (streamState, isSoftDeleted) = GetStreamStateCore(streamName);

            // StreamState.Any means "no optimistic-concurrency check": accept the append regardless of the current revision.
            if (expectedRevision != StreamState.Any && streamState != expectedRevision && !isSoftDeleted)
            {
                // mismatch and return with expectedRevision (since it didn't change)
                return (ConditionalWriteResult)ConditionalWriteResultConstructor.Invoke([streamState, default, ConditionalWriteStatus.VersionMismatch,]);
            }

            streamState = AppendCore(streamName, eventData);
            return (ConditionalWriteResult)ConditionalWriteResultConstructor.Invoke([streamState, default, ConditionalWriteStatus.Succeeded,]);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private (StreamState StreamState, bool IsSoftDeleted) GetStreamStateCore(string streamName)
    {
        var streamState = streams.TryGetValue(streamName, out var events)
            ? StreamState.StreamRevision(all[events[^1]].EventNumber)
            : StreamState.NoStream;

        var metadata = GetStreamMetadataCore(streamName);
        var isSoftDeleted = metadata.Metadata.TruncateBefore is not null
                            && metadata.Metadata.TruncateBefore == StreamPosition.End;

        return (streamState, isSoftDeleted);
    }

    private StreamState AppendCore(string streamName, IEnumerable<EventData> eventData)
    {
        if (!streams.TryGetValue(streamName, out var events))
        {
            events = [];
            streams[streamName] = events;
        }

        // Real KurrentDB: writing to a soft-deleted stream resumes the stream and existing events stay hidden.
        // We model this by moving truncateBefore to the position of the first new event so subsequent reads
        // only return events written after the soft delete.
        var metadata = GetStreamMetadataCore(streamName);
        if (metadata.Metadata.TruncateBefore is not null && metadata.Metadata.TruncateBefore == StreamPosition.End)
        {
            var firstNewEventNumber = events.Count > 0
                ? StreamPosition.FromInt64(all[events[^1]].EventNumber.ToInt64() + 1)
                : StreamPosition.Start;

            SetStreamMetadataCore(streamName,
                                  metadata.MetastreamRevision.HasValue
                                      ? StreamState.StreamRevision(metadata.MetastreamRevision.Value)
                                      : StreamState.NoStream,
                                  new StreamMetadata(truncateBefore: firstNewEventNumber));
        }

        foreach (var newEvent in eventData)
        {
            var nextEventNumber = events.Count > 0 ? all[events[^1]].EventNumber.ToUInt64() + 1 : 0;
            var eventRecord = CreateEventRecord(newEvent, streamName, new StreamPosition(nextEventNumber), new Position((ulong)all.Count, (ulong)all.Count));
            events.Add(all.Count);
            all.Add(eventRecord);
            newEventWatcher.Publish(eventRecord);
        }

        return StreamState.StreamRevision(all[events[^1]].EventNumber);
    }

    /// <summary>
    /// Controls whether this emulator behaves like a KurrentDB 25.1+ server that supports atomic multi-stream appends.
    /// </summary>
    internal bool MultiStreamAppendSupported { get; set; } = true;

    public ValueTask<bool> SupportsMultiStreamAppendAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult(MultiStreamAppendSupported);

    public async ValueTask<MultiStreamAppendResponse> MultiStreamAppendAsync(IReadOnlyList<AppendStreamRequest> requests, CancellationToken cancellationToken)
    {
        if (!MultiStreamAppendSupported)
        {
            throw new InvalidOperationException($"{nameof(MultiStreamAppendAsync)} requires server version 25.1 or higher.");
        }

        if (requests.Select(x => x.Stream).Distinct(StringComparer.Ordinal).Count() != requests.Count)
        {
            throw new ArgumentException("Each stream can only appear once in a multi-stream append.", nameof(requests));
        }

        await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Validate every expected state before writing anything so the operation is all-or-nothing, as on the server.
            foreach (var request in requests)
            {
                if (StreamMetadataJson.TryGetOwningStream(request.Stream, out var owningStream))
                {
                    if (request.Messages.Any(x => x.Type != StreamMetadataJson.MetadataEventType))
                    {
                        throw new NotSupportedException($"The in-memory client only supports {StreamMetadataJson.MetadataEventType} events in metastreams.");
                    }

                    var metastreamState = streamMetadata.TryGetValue(owningStream, out var metadataEvents) && metadataEvents.Count > 0
                        ? StreamState.StreamRevision((ulong)(metadataEvents.Count - 1))
                        : StreamState.NoStream;

                    if (!ExpectedStateMatches(request.ExpectedState, metastreamState, isSoftDeleted: false))
                    {
                        throw new WrongExpectedVersionException(request.Stream, request.ExpectedState, metastreamState);
                    }
                }
                else
                {
                    if (tombstoned.Contains(request.Stream))
                    {
                        throw new StreamDeletedException(request.Stream);
                    }

                    var (streamState, isSoftDeleted) = GetStreamStateCore(request.Stream);
                    if (!ExpectedStateMatches(request.ExpectedState, streamState, isSoftDeleted))
                    {
                        throw new WrongExpectedVersionException(request.Stream, request.ExpectedState, streamState);
                    }
                }
            }

            var responses = new List<AppendResponse>(requests.Count);
            foreach (var request in requests)
            {
                if (StreamMetadataJson.TryGetOwningStream(request.Stream, out var owningStream))
                {
                    SuccessResult? result = null;
                    foreach (var metadataEvent in request.Messages)
                    {
                        result = SetStreamMetadataCore(owningStream, StreamState.Any, StreamMetadataJson.Deserialize(metadataEvent.Data.Span));
                    }
                    responses.Add(new AppendResponse(request.Stream, result?.NextExpectedStreamState.ToInt64() ?? -1));
                }
                else
                {
                    responses.Add(new AppendResponse(request.Stream, AppendCore(request.Stream, request.Messages).ToInt64()));
                }
            }

            return new MultiStreamAppendResponse(all.Count - 1, responses);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private static bool ExpectedStateMatches(StreamState expected, StreamState actual, bool isSoftDeleted)
    {
        if (expected == StreamState.Any || isSoftDeleted)
        {
            return true;
        }

        if (expected == StreamState.StreamExists)
        {
            return actual != StreamState.NoStream;
        }

        return expected == actual;
    }

    private static EventRecord CreateEventRecord(EventData eventData, string eventStreamId, StreamPosition eventNumber, Position position)
    {
        var metadata = new Dictionary<string, string>
                       {
                           { "type", eventData.Type },
                           { "created", DateTime.UtcNow.Ticks.ToString(NumberFormatInfo.InvariantInfo) },
                           { "content-type", eventData.ContentType },
                       };

        return new EventRecord(eventStreamId, eventData.EventId, eventNumber, position, metadata, eventData.Data, eventData.Metadata);
    }

    public async Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata metadata, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            return SetStreamMetadataCore(streamName, expectedRevision, metadata);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private SuccessResult SetStreamMetadataCore(string streamName, StreamState expectedRevision, StreamMetadata metadata)
    {
        streamMetadata.TryGetValue(streamName, out var events);

        // Metastream revisions are 0-based, matching real KurrentDB: the first metadata write is revision 0.
        var currentRevision = events is { Count: > 0 }
            ? StreamState.StreamRevision((ulong)(events.Count - 1))
            : StreamState.NoStream;

        // Real KurrentDB throws WrongExpectedVersionException when a conditional metadata write
        // does not match the current metastream revision; StreamState.Any bypasses the check.
        if (expectedRevision != StreamState.Any && expectedRevision != currentRevision)
        {
            throw new WrongExpectedVersionException(streamName, expectedRevision, currentRevision);
        }

        if (events is null)
        {
            events = [];
            streamMetadata.Add(streamName, events);
        }

        events.Add(metadata);

        return new SuccessResult(StreamState.StreamRevision((ulong)(events.Count - 1)), default);
    }

    public async Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (!streams.TryGetValue(streamName, out var events))
            {
                // Stream does not exist, return a successful delete result with no changes
                return new DeleteResult();
            }

            // Check if the expected revision matches the current stream revision
            var metadata = GetStreamMetadataCore(streamName);
            var currentRevision = StreamState.StreamRevision(all[events[^1]].EventNumber);

            // StreamState.Any means "no optimistic-concurrency check": delete regardless of the current revision.
            // Otherwise validate the revision, honoring metadata truncation.
            if (expectedRevision != StreamState.Any
                && currentRevision != expectedRevision
                && (metadata.Metadata.TruncateBefore is null || all[events[^1]].EventNumber >= metadata.Metadata.TruncateBefore))
            {
                // Revision mismatch, return a failed delete result
                throw new WrongExpectedVersionException(streamName, expectedRevision, currentRevision);
            }

            SetStreamMetadataCore(streamName,
                                    metadata.MetastreamRevision.HasValue ? StreamState.StreamRevision(metadata.MetastreamRevision.Value) : StreamState.NoStream,
                                    new StreamMetadata(truncateBefore: StreamPosition.End));


            // Return a successful delete result
            return new DeleteResult();
        }
        finally
        {
            singleAccess.Release();
        }
    }

    public async Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            return GetStreamMetadataCore(streamName);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private StreamMetadataResult GetStreamMetadataCore(string streamName)
    {
        if (!streamMetadata.TryGetValue(streamName, out var events))
        {
            return StreamMetadataResult.None(streamName);
        }

        return StreamMetadataResult.Create(streamName, StreamPosition.FromInt64(events.Count - 1), events[^1]);
    }

    public async IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Regex? eventTypeRegex = null;

        if (eventFilter.Regex.ToString() is { } eventFilterRegex)
        {
            eventTypeRegex = new Regex(eventFilterRegex);
        }

        ulong index = 1;

        bool IncludeEvent(EventRecord @event)
        {
            return (index++ >= start.ToUInt64().commitPosition) &&
             ((eventTypeRegex?.IsMatch(@event.EventType) ?? false) ||
                  eventFilter.Prefixes.Any(x => @event.EventStreamId.StartsWith(x!, StringComparison.OrdinalIgnoreCase)));
        }

        IAsyncEnumerator<EventRecord>? enumerator = null;
        try
        {
            await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                foreach (var @event in all)
                {
                    if (IncludeEvent(@event))
                    {
                        yield return new StreamMessage.Event(new ResolvedEvent(@event, null, index));
                    }
                }
                enumerator = newEventWatcher.GetAsyncEnumerator(cancellationToken);

                yield return new StreamMessage.CaughtUp();
            }
            finally
            {
                singleAccess.Release();
            }

            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var @event = enumerator.Current;

                if (IncludeEvent(@event))
                {
                    yield return new StreamMessage.Event(new ResolvedEvent(@event, null, index));
                }
            }
        }
        finally
        {
            if (enumerator is not null)
                await enumerator.DisposeAsync().ConfigureAwait(false);
        }

    }

    readonly static ConcurrentDictionary<string, IKurrentClient> Clients = [];

    internal static IKurrentClient Get(string name)
     => Clients.GetOrAdd(name, _ => new InMemoryKurrentClient());

    public async Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (!streams.TryGetValue(streamName, out var events))
            {
                return new DeleteResult();
            }

            var metadata = GetStreamMetadataCore(streamName);
            var currentRevision = StreamState.StreamRevision(all[events[^1]].EventNumber);

            // StreamState.Any means "no optimistic-concurrency check": tombstone regardless of the current revision.
            if (expectedRevision != StreamState.Any
                && currentRevision != expectedRevision
                && (metadata.Metadata.TruncateBefore is null || all[events[^1]].EventNumber >= metadata.Metadata.TruncateBefore))
            {
                throw new WrongExpectedVersionException(streamName, expectedRevision, currentRevision);
            }

            // Hard delete: remove stream data and mark as permanently tombstoned
            streams.Remove(streamName);
            tombstoned.Add(streamName);

            return new DeleteResult();
        }
        finally
        {
            singleAccess.Release();
        }
    }

    /// <summary>
    /// A simple multi-subscriber broadcast publisher. Each call to
    /// <see cref="GetAsyncEnumerator"/> creates an independent unbounded channel that
    /// receives every item published after the subscription starts. Items published
    /// before a subscriber attaches are not replayed.
    /// </summary>
    private sealed class BroadcastPublisher<T> : IDisposable
    {
#if NET9_0_OR_GREATER
        private readonly Lock gate = new();
#else
        private readonly object gate = new();
#endif
        private readonly List<Channel<T>> subscribers = [];
        private bool disposed;

        public void Publish(T item)
        {
            Channel<T>[] snapshot;
            lock (gate)
            {
                if (disposed)
                {
                    return;
                }
                snapshot = [.. subscribers];
            }

            foreach (var subscriber in snapshot)
            {
                subscriber.Writer.TryWrite(item);
            }
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            });

            lock (gate)
            {
                if (disposed)
                {
                    channel.Writer.TryComplete();
                }
                else
                {
                    subscribers.Add(channel);
                }
            }

            return ReadAllAsync(channel, cancellationToken).GetAsyncEnumerator(cancellationToken);
        }

        private async IAsyncEnumerable<T> ReadAllAsync(Channel<T> channel, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
            finally
            {
                lock (gate)
                {
                    subscribers.Remove(channel);
                }
                channel.Writer.TryComplete();
            }
        }

        public void Dispose()
        {
            Channel<T>[] snapshot;
            lock (gate)
            {
                if (disposed)
                {
                    return;
                }
                disposed = true;
                snapshot = [.. subscribers];
                subscribers.Clear();
            }

            foreach (var subscriber in snapshot)
            {
                subscriber.Writer.TryComplete();
            }
        }
    }
}

