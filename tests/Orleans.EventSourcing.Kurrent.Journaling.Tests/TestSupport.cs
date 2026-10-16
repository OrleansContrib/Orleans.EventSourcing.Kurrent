using System.Buffers;
using System.Text;

using KurrentDB.Client;

using Microsoft.Extensions.Logging.Abstractions;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Journaling;

namespace Orleans.EventSourcing.Kurrent.Journaling.Tests;

/// <summary>
/// Captures everything a journal storage read replays.
/// </summary>
internal sealed class CollectingConsumer : IJournalStorageConsumer
{
    private readonly List<byte> bytes = [];

    public List<IJournalMetadata?> Metadata { get; } = [];

    public bool Completed { get; private set; }

    public string Text => Encoding.UTF8.GetString([.. bytes]);

    public void Read(JournalBufferReader buffer, IJournalMetadata? metadata)
    {
        Metadata.Add(metadata);
        var data = buffer.ToArray();
        buffer.Skip(data.Length);
        bytes.AddRange(data);
        Completed |= buffer.IsCompleted;
    }

    public static async Task<CollectingConsumer> ReadAsync(IJournalStorage storage)
    {
        var consumer = new CollectingConsumer();
        await storage.ReadAsync(consumer, CancellationToken.None);
        return consumer;
    }
}

/// <summary>
/// Wraps a client so tests can count calls and inject failures.
/// </summary>
internal sealed class InterceptingKurrentClient(IKurrentClient inner) : IKurrentClient
{
    public int SetStreamMetadataCalls { get; private set; }

    public int MultiStreamAppendCalls { get; private set; }

    public Exception? ThrowOnSetStreamMetadata { get; set; }

    /// <summary>Runs once, just before the next multi-stream append reaches the inner client.</summary>
    public Func<Task>? BeforeNextMultiStreamAppend { get; set; }

    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
    {
        SetStreamMetadataCalls++;
        if (ThrowOnSetStreamMetadata is { } ex)
        {
            throw ex;
        }
        return inner.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token);
    }

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
        => inner.GetStreamMetadata(streamName, token);

    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => inner.DeleteStreamAsync(streamName, expectedRevision, token);

    public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => inner.TombstoneStreamAsync(streamName, expectedRevision, token);

    public ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
        => inner.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);

    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName, StreamState expectedRevision, IEnumerable<EventData> eventData, CancellationToken cancellationToken)
        => inner.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken);

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => inner.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

    public ValueTask<bool> SupportsMultiStreamAppendAsync(CancellationToken cancellationToken)
        => inner.SupportsMultiStreamAppendAsync(cancellationToken);

    public async ValueTask<MultiStreamAppendResponse> MultiStreamAppendAsync(IReadOnlyList<AppendStreamRequest> requests, CancellationToken cancellationToken)
    {
        MultiStreamAppendCalls++;
        if (BeforeNextMultiStreamAppend is { } before)
        {
            BeforeNextMultiStreamAppend = null;
            await before();
        }
        return await inner.MultiStreamAppendAsync(requests, cancellationToken);
    }

    public void Dispose() => inner.Dispose();

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal static class TestExtensions
{
    public static ValueTask AppendAsync(this IJournalStorage storage, string text)
        => storage.AppendAsync(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(text)), CancellationToken.None);

    public static ValueTask ReplaceAsync(this IJournalStorage storage, string text)
        => storage.ReplaceAsync(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(text)), CancellationToken.None);

    public static KurrentJournalStorage CreateStorage(IKurrentClient client, string streamName, KurrentJournalStorageOptions? options = null, string formatKey = "jsonl")
        => new(streamName, formatKey, client, options ?? new KurrentJournalStorageOptions(), NullLogger.Instance);

    public static async Task<List<ResolvedEvent>> ReadVisibleEventsAsync(this IKurrentClient client, string streamName)
    {
        var result = await client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start, long.MaxValue, false, CancellationToken.None);
        var events = new List<ResolvedEvent>();
        await foreach (var resolvedEvent in result)
        {
            events.Add(resolvedEvent);
        }
        return events;
    }
}
