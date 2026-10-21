using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests
{
    public sealed class KurrentLogViewAdapterTests
    {
        [Fact]
        public async Task SoftDeleteThenWrite()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var innerClient = new InMemoryKurrentClient();
            var blockingClient = new BlockingKurrentClient(innerClient);
            var adapter = CreateAdapter(blockingClient, services, streamNameProvider);

            try
            {
                await blockingClient.DeleteStreamAsync(streamNameProvider.GetStreamName(services.GrainId), StreamState.NoStream, TestContext.Current.CancellationToken);
                Assert.True(await adapter.TryAppend(new Applied(100)));

                blockingClient.BlockNextAppend();
                adapter.SubmitRange([new Applied(25), new Truncate(), new Applied(42)]);

                await blockingClient.WaitForAppendToStart();

                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(42, adapter.TentativeView.Balance);

                blockingClient.ReleaseAppend();
                await adapter.ConfirmSubmittedEntries();

                Assert.Equal(42, adapter.ConfirmedView.Balance);
                Assert.Equal(42, adapter.TentativeView.Balance);
            }
            finally
            {
                adapter.Dispose();
                await blockingClient.DisposeAsync();
            }
        }

        [Fact]
        public async Task EmptyReadPreservesSoftDeletedStreamPosition()
        {
            var client = new InMemoryKurrentClient();
            const string streamName = "soft-deleted";

            var missing = await client.ReadStreamAsync(Direction.Forwards,
                                                       streamName,
                                                       StreamPosition.Start,
                                                       1,
                                                       false,
                                                       TestContext.Current.CancellationToken);
            var missingResult = Assert.IsType<StreamDoesNotExistReadResult>(missing);
            Assert.Null(missingResult.LastStreamPosition);

            var append = await client.ConditionalAppendToStreamAsync(streamName,
                                                                       StreamState.NoStream,
                                                                       [new EventData(Uuid.NewUuid(), "event", Array.Empty<byte>(), null)],
                                                                       TestContext.Current.CancellationToken);
            Assert.Equal(ConditionalWriteStatus.Succeeded, append.Status);

            await client.DeleteStreamAsync(streamName,
                                           append.NextExpectedStreamState,
                                           TestContext.Current.CancellationToken);

            var softDeleted = await client.ReadStreamAsync(Direction.Forwards,
                                                            streamName,
                                                            StreamPosition.Start,
                                                            1,
                                                            false,
                                                            TestContext.Current.CancellationToken);
      
            Assert.Equal(StreamPosition.FromStreamRevision(0), softDeleted.LastStreamPosition);

            var resumed = await client.ConditionalAppendToStreamAsync(streamName,
                                                                       StreamState.StreamRevision(softDeleted.LastStreamPosition!.Value),
                                                                       [new EventData(Uuid.NewUuid(), "event", Array.Empty<byte>(), null)],
                                                                       TestContext.Current.CancellationToken);
            Assert.Equal(ConditionalWriteStatus.Succeeded, resumed.Status);
        }

        [Fact]
        public async Task SubmitRangeResetsTentativeViewBeforePersistence()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var innerClient = new InMemoryKurrentClient();
            var blockingClient = new BlockingKurrentClient(innerClient);
            var adapter = CreateAdapter(blockingClient, services, streamNameProvider);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                blockingClient.BlockNextAppend();
                adapter.SubmitRange([new Applied(25), new Truncate(), new Applied(42)]);

                await blockingClient.WaitForAppendToStart();

                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(42, adapter.TentativeView.Balance);

                blockingClient.ReleaseAppend();
                await adapter.ConfirmSubmittedEntries();

                Assert.Equal(42, adapter.ConfirmedView.Balance);
                Assert.Equal(42, adapter.TentativeView.Balance);
            }
            finally
            {
                adapter.Dispose();
                await blockingClient.DisposeAsync();
            }
        }

        [Fact]
        public async Task SynchronizeRebuildsFromTruncationMarkerAndHealsMetadata()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new InMemoryKurrentClient();
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(client, services, streamNameProvider, serializer);

            try
            {
                var seedEvents = new TestLogEntry[] { new Applied(100), new Applied(25), new Truncate(), new Applied(42) };
                var result = await client.ConditionalAppendToStreamAsync(streamName,
                                                                        StreamState.NoStream,
                                                                        seedEvents.Select(serializer.SerializeEvent),
                                                                        TestContext.Current.CancellationToken);

                Assert.Equal(ConditionalWriteStatus.Succeeded, result.Status);

                await adapter.Synchronize();

                Assert.Equal(42, adapter.ConfirmedView.Balance);
                Assert.Equal(42, adapter.TentativeView.Balance);

                var readResult = await client.ReadStreamAsync(Direction.Forwards,
                                                              streamName,
                                                              StreamPosition.Start,
                                                              int.MaxValue,
                                                              false,
                                                              TestContext.Current.CancellationToken);
   
                var visibleEvents = new List<TestLogEntry>();
                await foreach (var resolvedEvent in readResult)
                {
                    visibleEvents.Add(serializer.DeserializeEvent(resolvedEvent));
                }

                Assert.Collection(visibleEvents,
                                  entry => Assert.IsType<Truncate>(entry),
                                  entry => Assert.Equal(42, Assert.IsType<Applied>(entry).Amount));
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        [Fact]
        public async Task ConditionalAppendRetryPatternHealsAfterExternalWrite()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new InMemoryKurrentClient();
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(client, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                // Another writer appends behind the adapter's back (e.g. a duplicate activation).
                var externalWrite = await client.ConditionalAppendToStreamAsync(streamName,
                                                                                StreamState.StreamRevision(0),
                                                                                [serializer.SerializeEvent(new Applied(7))],
                                                                                TestContext.Current.CancellationToken);
                Assert.Equal(ConditionalWriteStatus.Succeeded, externalWrite.Status);

                // The conditional append is rejected because the expected version is stale.
                Assert.False(await adapter.TryAppend(new Applied(50)));

                // Until the log is resynchronized the views are diverged: the tentative view still
                // contains the rejected entry and the confirmed view is missing the external write.
                Assert.Equal(150, adapter.TentativeView.Balance);
                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(1, adapter.ConfirmedVersion);

                // The documented retry pattern: synchronize, re-validate, then retry the append.
                await adapter.Synchronize();

                Assert.Equal(107, adapter.ConfirmedView.Balance);
                Assert.Equal(107, adapter.TentativeView.Balance);
                Assert.Equal(2, adapter.ConfirmedVersion);

                Assert.True(await adapter.TryAppend(new Applied(50)));

                Assert.Equal(157, adapter.ConfirmedView.Balance);
                Assert.Equal(157, adapter.TentativeView.Balance);
                Assert.Equal(3, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        [Fact]
        public async Task RejectionStaysReportedUntilResynchronized()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new InMemoryKurrentClient();
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(client, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                // Another writer appends behind the adapter's back (e.g. a duplicate activation).
                var externalWrite = await client.ConditionalAppendToStreamAsync(streamName,
                                                                                StreamState.StreamRevision(0),
                                                                                [serializer.SerializeEvent(new Applied(7))],
                                                                                TestContext.Current.CancellationToken);
                Assert.Equal(ConditionalWriteStatus.Succeeded, externalWrite.Status);

                // The conditional append is rejected because the expected version is stale.
                Assert.False(await adapter.TryAppend(new Applied(50)));

                // The stream state is unknown until a reload happens, so the rejection is not
                // consumed by simply observing it once: further calls (even with no new writes
                // queued) keep reporting the stale rejection.
                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ConfirmSubmittedEntries());
                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ConfirmSubmittedEntries());
                Assert.False(await adapter.TryAppend(new Applied(1)));

                // Resynchronizing re-establishes a known state and clears the rejection.
                await adapter.Synchronize();

                await adapter.ConfirmSubmittedEntries();
                Assert.True(await adapter.TryAppend(new Applied(50)));
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        [Fact]
        public async Task NotLeaderOnAppendIsRetried()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 3);

                Assert.True(await adapter.TryAppend(new Applied(5)));
                Assert.Equal(105, adapter.ConfirmedView.Balance);
                Assert.Equal(2, adapter.ConfirmedVersion);

                // Written exactly once
                Assert.Equal(2, (await adapter.RetrieveLogSegment(0, 10)).Count);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task NotLeaderDoesNotFailWritesQueuedBehindIt()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(1)));

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 2);

                // Several writes queued while the leader changes, as a reentrant grain would
                adapter.Submit(new Applied(10));
                adapter.SubmitRange([new Applied(20), new Applied(30)]);
                adapter.Submit(new Applied(40));

                await adapter.ConfirmSubmittedEntries();

                Assert.Equal(101, adapter.ConfirmedView.Balance);
                Assert.Equal(5, adapter.ConfirmedVersion);
                Assert.Empty(adapter.UnconfirmedSuffix);
                Assert.True(await adapter.TryAppend(new Applied(1)));
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task UnavailableAfterAppendLandedIsRetriedIdempotently()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                // The write lands but the reply is lost; the retry resends the same event IDs
                client.AddExceptionToThrowAfterAppendOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));

                Assert.True(await adapter.TryAppend(new Applied(5)));
                Assert.Equal(2, adapter.ConfirmedVersion);
                Assert.Equal(2, (await adapter.RetrieveLogSegment(0, 10)).Count);

                await adapter.Synchronize();
                Assert.Equal(105, adapter.ConfirmedView.Balance);
                Assert.Equal(2, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task NotLeaderOnLoadIsRetried()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppendRange([new Applied(100), new Applied(7)]));

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 2);

                await adapter.Synchronize();

                Assert.Equal(107, adapter.ConfirmedView.Balance);
                Assert.Equal(107, adapter.TentativeView.Balance);
                Assert.Equal(2, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task NotLeaderOnRetrieveLogSegmentIsRetried()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppendRange([new Applied(1), new Applied(2), new Applied(3)]));
                var expected = await adapter.RetrieveLogSegment(1, 2);

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 2);

                var segment = await adapter.RetrieveLogSegment(1, 2);
                Assert.Equal(2, segment.Count);
                Assert.Equal(expected, segment);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task NotLeaderBeyondRetryBudgetSurfacesUntilResynchronized()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(1)));

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), FastRetry.MaxAttempts);

                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.TryAppend(new Applied(10)));
                Assert.IsType<NotLeaderException>(ex.InnerException);

                await adapter.Synchronize();
                Assert.Equal(1, adapter.ConfirmedView.Balance);
                Assert.True(await adapter.TryAppend(new Applied(10)));
                Assert.Equal(11, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task NonTransientFailuresAreNotRetried()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(1)));

                // A retry would succeed, so the failure only surfaces if the append is not retried
                client.AddExceptionToThrowOnceForTesting(streamName, new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")));

                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.TryAppend(new Applied(10)));
                Assert.IsType<RpcException>(ex.InnerException);

                await adapter.Synchronize();
                Assert.Equal(1, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task DisposeStopsRetryingPromptly()
        {
            var slowRetry = new KurrentRetryOptions { MaxAttempts = 100, BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromSeconds(10) };
            var (adapter, client, streamName) = CreateLeaderChangeAdapter(slowRetry);

            Assert.True(await adapter.TryAppend(new Applied(1)));

            client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 100);
            adapter.Submit(new Applied(10));

            // Lets the append start and back off before the adapter is disposed
            await Task.Delay(50, TestContext.Current.CancellationToken);
            await adapter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        [Theory]
        [MemberData(nameof(TransientExceptions))]
        public void TransientClassification(Exception exception, bool transient)
            => Assert.Equal(transient, KurrentRetry.IsTransient(exception));

        public static TheoryData<Exception, bool> TransientExceptions => new()
        {
            { new NotLeaderException("localhost", 2313), true },
            { new DiscoveryException(10), true },
            { new RpcException(new Status(StatusCode.Unavailable, "unavailable")), true },
            { new RpcException(new Status(StatusCode.Cancelled, "cancelled")), false },
            { new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")), false },
            { new WrongExpectedVersionException("stream", StreamState.NoStream, StreamState.StreamRevision(1)), false },
            { new OperationCanceledException(), false },
            { new TimeoutException(), false },
        };

        private static readonly KurrentRetryOptions FastRetry = new() { MaxAttempts = 5, BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) };

        private static (KurrentLogViewAdapter<TestView, TestLogEntry> Adapter, ExceptionalKurrentClientWrapper Client, string StreamName) CreateLeaderChangeAdapter(KurrentRetryOptions? retry = null)
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new ExceptionalKurrentClientWrapper(new InMemoryKurrentClient());
            var adapter = CreateAdapter(client, services, streamNameProvider, retry: retry ?? FastRetry);
            return (adapter, client, streamNameProvider.GetStreamName(services.GrainId));
        }

        private static KurrentLogViewAdapter<TestView, TestLogEntry> CreateAdapter(IKurrentClient client,
                                                                                    TestLogConsistencyProtocolServices services,
                                                                                    IKurrentStreamNameProvider streamNameProvider,
                                                                                    TestEventSerializer? serializer = null,
                                                                                    KurrentRetryOptions? retry = null)
            => new(new TestLogViewAdaptorHost(),
                   client,
                   serializer ?? new TestEventSerializer(),
                   services,
                   streamNameProvider,
                   Metrics,
                   retry ?? new KurrentRetryOptions());

        // The same default IMeterFactory that Orleans registers for silos and clients
        private static readonly KurrentMetrics Metrics = new(new ServiceCollection().AddMetrics().BuildServiceProvider().GetRequiredService<IMeterFactory>());

        private sealed class TestLogViewAdaptorHost : ILogViewAdaptorHost<TestView, TestLogEntry>
        {
            public void UpdateView(TestView view, TestLogEntry entry)
            {
                switch (entry)
                {
                    case Applied applied:
                        view.Balance += applied.Amount;
                        break;
                    case Truncate:
                        view.Balance = 0;
                        break;
                    default:
                        throw new NotSupportedException(entry.GetType().FullName);
                }
            }

            public void OnViewChanged(bool tentativeViewChanged, bool confirmedViewChanged)
            {
            }

            public void OnConnectionIssue(ConnectionIssue issue)
            {
            }

            public void OnConnectionIssueResolved(ConnectionIssue issue)
            {
            }
        }

        private sealed class TestLogConsistencyProtocolServices : ILogConsistencyProtocolServices
        {
            public GrainId GrainId { get; } = GrainId.Parse("test/unit-test");

            public string MyClusterId => "test-cluster";

            public T DeepCopy<T>(T value) => value;

            public void ProtocolError(string message, bool throwException)
            {
                if (throwException)
                {
                    throw new InvalidOperationException(message);
                }
            }

            public void CaughtException(string message, Exception exception)
            {
            }

            public void CaughtUserCodeException(string message, string where, Exception exception)
            {
            }

            public void Log(LogLevel level, string format, params object[] args)
            {
            }
        }

        private sealed class BlockingKurrentClient(IKurrentClient inner) : IKurrentClient
        {
            private TaskCompletionSource appendStarted = CreateSignal();
            private TaskCompletionSource releaseAppend = CreateSignal();
            private volatile bool blockNextAppend;

            public void BlockNextAppend()
            {
                appendStarted = CreateSignal();
                releaseAppend = CreateSignal();
                blockNextAppend = true;
            }

            public Task WaitForAppendToStart() => appendStarted.Task;

            public void ReleaseAppend() => releaseAppend.TrySetResult();

            public async Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                                     StreamState expectedRevision,
                                                                                     IEnumerable<EventData> eventData,
                                                                                     CancellationToken cancellationToken)
            {
                if (blockNextAppend)
                {
                    blockNextAppend = false;
                    appendStarted.TrySetResult();
                    await releaseAppend.Task.WaitAsync(cancellationToken);
                }

                return await inner.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken);
            }

            public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
                => inner.DeleteStreamAsync(streamName, expectedRevision, token);

            public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
                => inner.TombstoneStreamAsync(streamName, expectedRevision, token);

            public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
                => inner.GetStreamMetadata(streamName, token);

            public ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
                => inner.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);

            public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
                => inner.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token);

            public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
                => inner.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

            public void Dispose() => inner.Dispose();

            public ValueTask DisposeAsync() => inner.DisposeAsync();

            private static TaskCompletionSource CreateSignal()
                => new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private sealed class TestEventSerializer : IEventConverter<TestLogEntry>
        {
            public TestLogEntry DeserializeEvent(ResolvedEvent logEntry)
            {
                return logEntry.Event.EventType switch
                {
                    nameof(Applied) => new Applied(int.Parse(Encoding.UTF8.GetString(logEntry.Event.Data.ToArray()), CultureInfo.InvariantCulture)),
                    nameof(Truncate) => new Truncate(),
                    _ => throw new NotSupportedException(logEntry.Event.EventType),
                };
            }

            public EventData SerializeEvent(TestLogEntry entry)
            {
                return entry switch
                {
                    Applied applied => new EventData(Uuid.NewUuid(),
                                                     nameof(Applied),
                                                     Encoding.UTF8.GetBytes(applied.Amount.ToString(CultureInfo.InvariantCulture)).AsMemory(),
                                                     null,
                                                     "application/json"),
                    Truncate => new EventData(Uuid.NewUuid(),
                                              nameof(Truncate),
                                              ReadOnlyMemory<byte>.Empty,
                                              null,
                                              "application/octet-stream"),
                    _ => throw new NotSupportedException(entry.GetType().FullName),
                };
            }
        }

        private sealed class TestView
        {
            public int Balance { get; set; }
        }

        private abstract record TestLogEntry;

        private sealed record Applied(int Amount) : TestLogEntry;

#pragma warning disable OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        [DiscardPriorEvents]
#pragma warning restore OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        private sealed record Truncate : TestLogEntry;
    }
}
