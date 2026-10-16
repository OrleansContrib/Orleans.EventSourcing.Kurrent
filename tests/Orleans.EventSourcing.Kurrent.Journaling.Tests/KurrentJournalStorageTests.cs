using System.Text.Json;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Journaling.Tests;

public sealed class KurrentJournalStorageTests
{
    private readonly InMemoryKurrentClient kurrent = new();
    private readonly string streamName = $"orleansjournal-test/{Guid.NewGuid():N}";

    [Fact]
    public async Task ReadOfMissingJournalCompletesWithoutData()
    {
        var storage = TestExtensions.CreateStorage(kurrent, streamName);

        var consumer = await CollectingConsumer.ReadAsync(storage);

        Assert.True(consumer.Completed);
        Assert.Equal(string.Empty, consumer.Text);
        Assert.False(storage.IsCompactionRequested);
    }

    [Fact]
    public async Task AppendsAreReplayedInOrderWithTheirFormatKey()
    {
        var storage = TestExtensions.CreateStorage(kurrent, streamName, formatKey: "orleans-binary");
        await CollectingConsumer.ReadAsync(storage);

        await storage.AppendAsync("a;");
        await storage.AppendAsync("b;");
        await storage.AppendAsync("c;");

        var consumer = await CollectingConsumer.ReadAsync(TestExtensions.CreateStorage(kurrent, streamName));

        Assert.True(consumer.Completed);
        Assert.Equal("a;b;c;", consumer.Text);
        Assert.All(consumer.Metadata, x => Assert.Equal("orleans-binary", x?.FormatKey));
    }

    [Fact]
    public async Task LargeWritesAreSplitAcrossEventsAndReassembled()
    {
        var options = new KurrentJournalStorageOptions { MaxEventSize = 4 };
        var storage = TestExtensions.CreateStorage(kurrent, streamName, options);
        await CollectingConsumer.ReadAsync(storage);

        await storage.ReplaceAsync("snapshot-0123456789;");
        await storage.AppendAsync("append-abcdefghij;");

        var events = await kurrent.ReadVisibleEventsAsync(streamName);
        Assert.Equal(10, events.Count);
        Assert.Equal(KurrentJournalEvents.SnapshotEventType, events[0].Event.EventType);
        Assert.Equal(KurrentJournalEvents.SnapshotContinuedEventType, events[1].Event.EventType);
        Assert.Equal(KurrentJournalEvents.AppendEventType, events[^1].Event.EventType);

        // A page size smaller than the number of events exercises paging across chunks.
        var reader = TestExtensions.CreateStorage(kurrent, streamName, new KurrentJournalStorageOptions { ReadPageSize = 3 });
        var consumer = await CollectingConsumer.ReadAsync(reader);
        Assert.Equal("snapshot-0123456789;append-abcdefghij;", consumer.Text);
    }

    [Fact]
    public async Task AtomicCompactionWritesSnapshotAndTruncationTogether()
    {
        var client = new InterceptingKurrentClient(kurrent);
        var storage = TestExtensions.CreateStorage(client, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");
        await storage.AppendAsync("b;");

        await storage.ReplaceAsync("snapshot;");
        await storage.AppendAsync("c;");

        Assert.Equal(1, client.MultiStreamAppendCalls);
        Assert.Equal(0, client.SetStreamMetadataCalls);

        var metadata = await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Equal(StreamPosition.FromInt64(2), metadata.Metadata.TruncateBefore);

        var events = await kurrent.ReadVisibleEventsAsync(streamName);
        Assert.Equal([KurrentJournalEvents.SnapshotEventType, KurrentJournalEvents.AppendEventType], events.Select(x => x.Event.EventType));

        var consumer = await CollectingConsumer.ReadAsync(TestExtensions.CreateStorage(kurrent, streamName));
        Assert.Equal("snapshot;c;", consumer.Text);
    }

    [Fact]
    public async Task CompactionPreservesOtherStreamMetadata()
    {
        var storage = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");

        using var custom = JsonDocument.Parse("""{"owner":"orders-team"}""");
        await kurrent.SetStreamMetadata(streamName,
                                        StreamState.NoStream,
                                        new StreamMetadata(maxAge: TimeSpan.FromDays(30), acl: new StreamAcl(readRole: "$admins"), customMetadata: custom),
                                        CancellationToken.None);

        await storage.ReplaceAsync("snapshot;");

        var metadata = (await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken)).Metadata;
        Assert.Equal(StreamPosition.FromInt64(1), metadata.TruncateBefore);
        Assert.Equal(TimeSpan.FromDays(30), metadata.MaxAge);
        Assert.Equal(["$admins"], metadata.Acl!.ReadRoles!);
        Assert.Equal("orders-team", metadata.CustomMetadata!.RootElement.GetProperty("owner").GetString());
    }

    [Fact]
    public async Task AtomicCompactionRetriesWhenMetadataChangesConcurrently()
    {
        var client = new InterceptingKurrentClient(kurrent);
        var storage = TestExtensions.CreateStorage(client, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");

        // Someone else edits the metadata between our metadata read and the atomic append.
        client.BeforeNextMultiStreamAppend = () => kurrent.SetStreamMetadata(streamName, StreamState.Any, new StreamMetadata(maxCount: 100), TestContext.Current.CancellationToken);

        await storage.ReplaceAsync("snapshot;");

        Assert.Equal(2, client.MultiStreamAppendCalls);
        var metadata = (await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken)).Metadata;
        Assert.Equal(100, metadata.MaxCount);
        Assert.Equal(StreamPosition.FromInt64(1), metadata.TruncateBefore);
    }

    [Fact]
    public async Task CompactionFallsBackToSeparateMetadataWriteWhenServerLacksMultiStreamAppend()
    {
        kurrent.MultiStreamAppendSupported = false;
        var client = new InterceptingKurrentClient(kurrent);
        var storage = TestExtensions.CreateStorage(client, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");

        await storage.ReplaceAsync("snapshot;");

        Assert.Equal(0, client.MultiStreamAppendCalls);
        Assert.Equal(1, client.SetStreamMetadataCalls);
        var metadata = await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Equal(StreamPosition.FromInt64(1), metadata.Metadata.TruncateBefore);
    }

    [Fact]
    public async Task DisabledAtomicCompactionNeverUsesMultiStreamAppend()
    {
        var client = new InterceptingKurrentClient(kurrent);
        var storage = TestExtensions.CreateStorage(client, streamName, new KurrentJournalStorageOptions { AtomicCompaction = AtomicCompactionMode.Disabled });
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");

        await storage.ReplaceAsync("snapshot;");

        Assert.Equal(0, client.MultiStreamAppendCalls);
        Assert.Equal(1, client.SetStreamMetadataCalls);
    }

    [Fact]
    public async Task RequiredAtomicCompactionFailsWhenServerLacksMultiStreamAppend()
    {
        kurrent.MultiStreamAppendSupported = false;
        var storage = TestExtensions.CreateStorage(kurrent, streamName, new KurrentJournalStorageOptions { AtomicCompaction = AtomicCompactionMode.Required });
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");

        await Assert.ThrowsAsync<NotSupportedException>(async () => await storage.ReplaceAsync("snapshot;"));
    }

    [Fact]
    public async Task RecoveryIgnoresEventsBeforeSnapshotWhenTruncationFailed()
    {
        kurrent.MultiStreamAppendSupported = false;
        var client = new InterceptingKurrentClient(kurrent) { ThrowOnSetStreamMetadata = new InvalidOperationException("metadata write failed") };
        var storage = TestExtensions.CreateStorage(client, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("a;");
        await storage.AppendAsync("b;");

        // The snapshot is written; the truncation fails but must not fail the compaction.
        await storage.ReplaceAsync("snapshot;");
        await storage.AppendAsync("c;");

        // Nothing was truncated: a, b, snapshot and c are all still visible in the stream.
        Assert.Equal(4, (await kurrent.ReadVisibleEventsAsync(streamName)).Count);
        var consumer = await CollectingConsumer.ReadAsync(TestExtensions.CreateStorage(kurrent, streamName));
        Assert.Equal("snapshot;c;", consumer.Text);
    }

    [Fact]
    public async Task ConcurrentWriterCausesInconsistentStateOnAppend()
    {
        var first = TestExtensions.CreateStorage(kurrent, streamName);
        var second = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(first);
        await CollectingConsumer.ReadAsync(second);

        await first.AppendAsync("first;");

        await Assert.ThrowsAsync<InconsistentStateException>(async () => await second.AppendAsync("second;"));
    }

    [Fact]
    public async Task ConcurrentWriterCausesInconsistentStateOnAtomicCompaction()
    {
        var first = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(first);
        await first.AppendAsync("a;");

        var second = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(second);
        await first.AppendAsync("b;");

        await Assert.ThrowsAsync<InconsistentStateException>(async () => await second.ReplaceAsync("snapshot;"));

        // The failed compaction must not have changed the metadata either.
        var metadata = await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Null(metadata.Metadata.TruncateBefore);
    }

    [Fact]
    public async Task ConcurrentWriterCausesInconsistentStateOnDelete()
    {
        var first = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(first);
        await first.AppendAsync("a;");

        var second = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(second);
        await first.AppendAsync("b;");

        await Assert.ThrowsAsync<InconsistentStateException>(async () => await second.DeleteAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeletedJournalCanBeWrittenAgain()
    {
        var storage = TestExtensions.CreateStorage(kurrent, streamName);
        await CollectingConsumer.ReadAsync(storage);
        await storage.AppendAsync("old;");

        await storage.DeleteAsync(TestContext.Current.CancellationToken);
        Assert.Equal(string.Empty, (await CollectingConsumer.ReadAsync(TestExtensions.CreateStorage(kurrent, streamName))).Text);

        await storage.ReplaceAsync("new-snapshot;");
        await storage.AppendAsync("new;");
        await storage.ReplaceAsync("newer-snapshot;");

        var consumer = await CollectingConsumer.ReadAsync(TestExtensions.CreateStorage(kurrent, streamName));
        Assert.Equal("newer-snapshot;", consumer.Text);
    }

    [Fact]
    public async Task CompactionIsRequestedAfterEventOrSizeThreshold()
    {
        var byCount = TestExtensions.CreateStorage(kurrent, streamName, new KurrentJournalStorageOptions { CompactionEventCountThreshold = 3 });
        await CollectingConsumer.ReadAsync(byCount);
        await byCount.AppendAsync("a;");
        await byCount.AppendAsync("b;");
        Assert.False(byCount.IsCompactionRequested);
        await byCount.AppendAsync("c;");
        Assert.True(byCount.IsCompactionRequested);

        // A fresh instance recovers the counters from the stream.
        var recovered = TestExtensions.CreateStorage(kurrent, streamName, new KurrentJournalStorageOptions { CompactionEventCountThreshold = 3 });
        await CollectingConsumer.ReadAsync(recovered);
        Assert.True(recovered.IsCompactionRequested);

        await recovered.ReplaceAsync("snapshot;");
        Assert.False(recovered.IsCompactionRequested);

        var bySize = TestExtensions.CreateStorage(kurrent, streamName, new KurrentJournalStorageOptions { CompactionSizeThreshold = 10 });
        await CollectingConsumer.ReadAsync(bySize);
        await bySize.AppendAsync("0123456789");
        Assert.True(bySize.IsCompactionRequested);
    }

    [Fact]
    public async Task ReadRejectsEventsNotWrittenByThisProvider()
    {
        await kurrent.ConditionalAppendToStreamAsync(streamName,
                                                     StreamState.NoStream,
                                                     [new EventData(Uuid.NewUuid(), "SomethingElse", "{}"u8.ToArray())],
                                                     CancellationToken.None);

        var storage = TestExtensions.CreateStorage(kurrent, streamName);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await CollectingConsumer.ReadAsync(storage));
    }

    [Fact]
    public void SystemStreamNamesAreRejected()
        => Assert.Throws<ArgumentException>(() => TestExtensions.CreateStorage(kurrent, "$system"));
}
