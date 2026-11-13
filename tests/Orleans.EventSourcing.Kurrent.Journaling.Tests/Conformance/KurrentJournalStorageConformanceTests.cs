// The KurrentDB counterpart of dotnet/orleans, test/Orleans.Journaling.Tests/Conformance/JournalStorageProviderConformanceTests.cs
// (dotnet/orleans#11537, commit 50b55511e132704c013f6e57ddf2fb605bf17cb1), which runs JournalStorageConformanceTests
// against each of Orleans' own journal storage providers. Like the Azure Table provider there, it injects both fault kinds;
// it does so in the KurrentDB client rather than in an HTTP handler.
//
// Copyright (c) .NET Foundation and Contributors. Licensed under the MIT License:
// https://github.com/dotnet/orleans/blob/main/LICENSE

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Journaling;
using Orleans.Journaling.Json;

namespace Orleans.EventSourcing.Kurrent.Journaling.Tests.Conformance;

/// <summary>
/// Runs the Orleans journal storage conformance suite against <see cref="KurrentJournalStorage"/>, on the in-memory
/// emulator or, when <c>KURRENT_JOURNALING_TESTS_CONNECTION_STRING</c> is set, on that KurrentDB server.
/// </summary>
/// <remarks>
/// Compaction is atomic (a multi-stream append to the journal stream and its metastream) whenever the server supports it.
/// </remarks>
public sealed class KurrentJournalStorageConformanceTests : KurrentJournalStorageConformanceTestsBase
{
    protected override KurrentJournalStorageOptions CreateOptions() => new();
}

/// <summary>
/// Runs the conformance suite with compaction written as two separate operations: the snapshot append, then a
/// best-effort <c>$tb</c> update. This is how the provider compacts on servers older than KurrentDB 25.1.
/// </summary>
public sealed class KurrentJournalStorageWithoutAtomicCompactionConformanceTests : KurrentJournalStorageConformanceTestsBase
{
    protected override KurrentJournalStorageOptions CreateOptions() => new() { AtomicCompaction = AtomicCompactionMode.Disabled };
}

/// <summary>
/// Runs the conformance suite with small events and small read pages, so most segments span many events and most reads
/// span many pages.
/// </summary>
public sealed class KurrentJournalStorageSmallEventsConformanceTests : KurrentJournalStorageConformanceTestsBase
{
    protected override KurrentJournalStorageOptions CreateOptions() => new() { MaxEventSize = 1_000, ReadPageSize = 7 };
}

public abstract class KurrentJournalStorageConformanceTestsBase : JournalStorageConformanceTests
{
    private const string FormatKey = JsonLinesJournalFormat.JournalFormatKey;

    private readonly IKurrentClient kurrent = TestKurrent.CreateClient();
    private KurrentJournalStorageOptions? options;
    private long? maxAppendBytes;

    protected abstract KurrentJournalStorageOptions CreateOptions();

    protected override long? MaxAppendBytes => maxAppendBytes;

    protected override string AppendLimitSkipReason
        => "Against a real server the largest segment depends on how the server counts each event's size against MaxAppendSize, "
           + "which this suite does not model. KurrentJournalStorageTests.AppendLargerThanServerMaxAppendSizeFails covers the server's limit.";

    protected override string? MetadataOperationsSkipReason
        => "KurrentJournalStorage does not implement the journal metadata operations (GetMetadataAsync, UpdateMetadataAsync, CreateIfNotExistsAsync).";

    protected override IReadOnlySet<JournalStorageFaultKind> SupportedFaults { get; } = new HashSet<JournalStorageFaultKind>
    {
        JournalStorageFaultKind.AbortRequestBody,
        JournalStorageFaultKind.LoseResponse,
    };

    public override async ValueTask InitializeAsync()
    {
        options = CreateOptions();
        if (kurrent is InMemoryKurrentClient emulator)
        {
            // The emulator enforces MaxAppendSize exactly, over the data of every event in one append, and every event's
            // data starts with the provider's header, so the largest segment one append accepts is known exactly.
            maxAppendBytes = GetLargestSegment(emulator.MaxAppendSize, options.MaxEventSize, KurrentJournalEvents.CreateHeader(FormatKey).Length);
        }

        await base.InitializeAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await kurrent.DisposeAsync();
    }

    protected override ValueTask<IJournalStorageProvider> CreateProviderAsync(JournalStorageFaultInjector? faults, CancellationToken cancellationToken)
    {
        // Providers dispose their client, so each gets a wrapper and the shared client is disposed with the test.
        var client = new FaultInjectingKurrentClient(kurrent, faults);
        var provider = new KurrentJournalStorageProvider(
            options!,
            client,
            Options.Create(new JournaledStateManagerOptions { JournalFormatKey = FormatKey }),
            NullLogger<KurrentJournalStorageProvider>.Instance);
        return new(provider);
    }

    // A KurrentDB journal exists while its stream's last visible event is a journal write: a missing or soft-deleted
    // stream has none, and a delete marker left visible by a failed soft delete ends a deleted journal.
    protected override async Task<bool> JournalExistsAsync(JournalId journalId)
    {
        var result = await kurrent.ReadStreamAsync(Direction.Backwards, options!.GetStreamName(journalId), StreamPosition.End, 1, resolveLinkTos: false, TestContext.Current.CancellationToken);
        await foreach (var resolvedEvent in result.WithCancellation(TestContext.Current.CancellationToken))
        {
            return resolvedEvent.Event.EventType != KurrentJournalEvents.DeletedEventType;
        }

        return false;
    }

    // The largest segment of n bytes for which n plus one header per event, at most maxEventSize bytes of segment per event, fits in maxAppendSize.
    private static long GetLargestSegment(long maxAppendSize, int maxEventSize, int headerLength)
    {
        long low = 0, high = maxAppendSize;
        while (low < high)
        {
            var n = (low + high + 1) / 2;
            if (n + (n + maxEventSize - 1) / maxEventSize * headerLength <= maxAppendSize)
            {
                low = n;
            }
            else
            {
                high = n - 1;
            }
        }

        return low;
    }
}

/// <summary>
/// Injects <see cref="JournalStorageFaultKind"/> faults into the write requests a journal storage provider sends.
/// </summary>
/// <remarks>
/// Faults are injected between the provider and the KurrentDB client, as the transport failures the client surfaces to the
/// provider: an aborted request reaches the server incompletely, so it is not applied; a lost response follows a write
/// which was applied. gRPC reports both as <see cref="StatusCode.Unavailable"/>. Reads pass through unchanged.
/// </remarks>
internal sealed class FaultInjectingKurrentClient(IKurrentClient inner, JournalStorageFaultInjector? faults) : IKurrentClient
{
    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
        => WriteAsync(() => inner.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token));

    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => WriteAsync(() => inner.DeleteStreamAsync(streamName, expectedRevision, token));

    public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => WriteAsync(() => inner.TombstoneStreamAsync(streamName, expectedRevision, token));

    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName, StreamState expectedRevision, IEnumerable<EventData> eventData, CancellationToken cancellationToken)
        => WriteAsync(() => inner.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken));

    public async ValueTask<MultiStreamAppendResponse> MultiStreamAppendAsync(IReadOnlyList<AppendStreamRequest> requests, CancellationToken cancellationToken)
        => await WriteAsync(async () => await inner.MultiStreamAppendAsync(requests, cancellationToken));

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
        => inner.GetStreamMetadata(streamName, token);

    public ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
        => inner.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => inner.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

    public ValueTask<bool> SupportsMultiStreamAppendAsync(CancellationToken cancellationToken)
        => inner.SupportsMultiStreamAppendAsync(cancellationToken);

    // The shared client belongs to the test.
    public void Dispose() { }

    public ValueTask DisposeAsync() => default;

    private async Task<T> WriteAsync<T>(Func<Task<T>> write)
    {
        if (faults?.TryTake(JournalStorageFaultKind.AbortRequestBody) == true)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "Injected fault: the connection dropped while sending the request."));
        }

        var result = await write();
        if (faults?.TryTake(JournalStorageFaultKind.LoseResponse) == true)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "Injected fault: the response to an applied write was lost."));
        }

        return result;
    }
}
