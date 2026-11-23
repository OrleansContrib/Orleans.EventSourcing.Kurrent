using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net.Http;
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

                // The rejected entry is rolled back from the tentative view; until the log is
                // resynchronized both views are missing the external write.
                Assert.Equal(100, adapter.TentativeView.Balance);
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

        // Issue #16: a write queued behind a rejected one expects a version that counts the
        // rejected entries. If another writer appended as many events, that version matches.
        [Fact]
        public async Task ConditionalWritesQueuedBehindARejectedWriteAreNotSent()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var inner = new InMemoryKurrentClient();
            var blocking = new BlockingKurrentClient(inner);
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(blocking, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                blocking.BlockNextAppend();
                var first = adapter.TryAppend(new Applied(10));
                await blocking.WaitForAppendToStart();

                // Decided on the tentative view, which includes the first write
                var second = adapter.TryAppend(new Applied(20));
                Assert.Equal(130, adapter.TentativeView.Balance);

                // Another writer appends one event while the first write is in flight
                await AppendExternallyAsync(inner, serializer, streamName, 0, 7);
                blocking.ReleaseAppend();

                Assert.False(await first);
                Assert.False(await second);

                // The second write did not take the first one's place
                Assert.Equal(new[] { 100, 7 }, await ReadAmountsAsync(inner, serializer, streamName));
                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(100, adapter.TentativeView.Balance);
                Assert.Equal(1, adapter.ConfirmedVersion);
                Assert.Empty(adapter.UnconfirmedSuffix);

                await adapter.Synchronize();
                Assert.True(await adapter.TryAppend(new Applied(20)));
                Assert.Equal(127, adapter.ConfirmedView.Balance);
                Assert.Equal(127, adapter.TentativeView.Balance);
                Assert.Equal(new[] { 100, 7, 20 }, await ReadAmountsAsync(inner, serializer, streamName));
            }
            finally
            {
                adapter.Dispose();
                await inner.DisposeAsync();
            }
        }

        // The same as above through RaiseEvent and ConfirmEvents, which needs no reentrancy
        [Fact]
        public async Task SubmittedWritesQueuedBehindARejectedWriteAreNotSent()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var inner = new InMemoryKurrentClient();
            var blocking = new BlockingKurrentClient(inner);
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(blocking, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                blocking.BlockNextAppend();
                adapter.Submit(new Applied(1));
                await blocking.WaitForAppendToStart();
                adapter.SubmitRange([new Applied(2), new Applied(3)]);
                adapter.Submit(new Applied(4));

                // Exactly as many events as the first write, so the next one would line up
                await AppendExternallyAsync(inner, serializer, streamName, 0, 7);
                blocking.ReleaseAppend();

                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ConfirmSubmittedEntries());

                Assert.Equal(new[] { 100, 7 }, await ReadAmountsAsync(inner, serializer, streamName));
                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(100, adapter.TentativeView.Balance);
                Assert.Equal(1, adapter.ConfirmedVersion);
                Assert.Empty(adapter.UnconfirmedSuffix);
            }
            finally
            {
                adapter.Dispose();
                await inner.DisposeAsync();
            }
        }

        // A reload clears the rejection, but a write queued behind the reload can still count a write
        // ahead of it that is then rejected. Found by the load test's contention scenario.
        [Fact]
        public Task AWriteQueuedBehindAReloadIsNotSentWhenAWriteItCountedIsRejected() => RunOnGrainLikeSchedulerAsync(async () =>
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var inner = new InMemoryKurrentClient();
            var blocking = new BlockingKurrentClient(inner);
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(blocking, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                var firstGate = blocking.BlockNextAppend();
                var first = adapter.TryAppend(new Applied(10));
                await firstGate.Started;

                // Waits for the first write, then queues a reload behind whatever is queued by then
                var synchronize = adapter.Synchronize();
                var secondGate = blocking.BlockNextAppend();
                var second = adapter.TryAppend(new Applied(20));

                firstGate.Release();
                Assert.True(await first);
                await secondGate.Started;
                await DrainAsync();

                // Queued behind the reload, and decided on a tentative view that includes the second write
                var third = adapter.TryAppend(new Applied(30));
                Assert.Equal(160, adapter.TentativeView.Balance);

                // Another writer appends one event while the second write is in flight
                await AppendExternallyAsync(inner, serializer, streamName, 1, 7);
                secondGate.Release();

                Assert.False(await second);
                await synchronize;
                Assert.False(await third);

                // The third write did not take the second one's place
                Assert.Equal(new[] { 100, 10, 7 }, await ReadAmountsAsync(inner, serializer, streamName));
                Assert.Equal(117, adapter.ConfirmedView.Balance);
                Assert.Equal(117, adapter.TentativeView.Balance);
                Assert.Equal(3, adapter.ConfirmedVersion);
                Assert.Empty(adapter.UnconfirmedSuffix);
            }
            finally
            {
                adapter.Dispose();
                await inner.DisposeAsync();
            }
        });

        [Fact]
        public async Task WritesAfterARejectionAreRefusedWithoutBeingSentUntilResynchronized()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();
            var serializer = new TestEventSerializer();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));
                await AppendExternallyAsync(client, serializer, streamName, 0, 7);
                Assert.False(await adapter.TryAppend(new Applied(50)));

                // Fails anything that reaches Kurrent, so a write that was sent would throw
                client.AddExceptionToThrowForTesting(streamName, new RpcException(new Status(StatusCode.DeadlineExceeded, "sent")), int.MaxValue);

                Assert.False(await adapter.TryAppend(new Applied(1)));
                Assert.False(await adapter.TryAppendRange([new Applied(2), new Applied(3)]));

                // With nothing ahead of it, a raised entry is refused and rolled back as it is submitted
                adapter.Submit(new Applied(5));
                Assert.Equal(100, adapter.TentativeView.Balance);
                Assert.Empty(adapter.UnconfirmedSuffix);
                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ConfirmSubmittedEntries());

                client.ClearExceptionForTesting(streamName);
                await adapter.Synchronize();
                Assert.True(await adapter.TryAppend(new Applied(1)));
                Assert.Equal(108, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ARejectionStillReportsFalseWhenTheViewCannotBeCopied(bool copierThrows)
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices
            {
                Copier = copierThrows ? static _ => throw new InvalidOperationException("no copier") : static value => value,
            };
            var client = new InMemoryKurrentClient();
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(client, services, streamNameProvider, serializer);

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));
                await AppendExternallyAsync(client, serializer, streamName, 0, 7);

                Assert.False(await adapter.TryAppend(new Applied(50)));

                // The tentative view is left as it was; the confirmed view is untouched
                Assert.Equal(150, adapter.TentativeView.Balance);
                Assert.Equal(100, adapter.ConfirmedView.Balance);
                Assert.Equal(copierThrows ? 1 : 0, services.CaughtExceptions.Count);

                await adapter.Synchronize();
                Assert.Equal(107, adapter.TentativeView.Balance);
                Assert.Equal(107, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        // Scenarios below are adapted from the Orleans reference adaptor's tests
        // (dotnet/orleans, test/Orleans.EventSourcing.Tests/EventSourcingTests/PrimaryBasedLogViewAdaptorTests.cs, MIT).

        [Fact]
        public async Task TryAppendRangeIsRejectedAsAWholeAndLeavesNothingTentative()
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
                await AppendExternallyAsync(client, serializer, streamName, 0, 7);

                Assert.False(await adapter.TryAppendRange([new Applied(1), new Applied(2), new Applied(3)]));

                Assert.Equal(new[] { 100, 7 }, await ReadAmountsAsync(client, serializer, streamName));
                Assert.Empty(adapter.UnconfirmedSuffix);
                Assert.Equal(1, adapter.ConfirmedVersion);
                Assert.Equal(adapter.ConfirmedView.Balance, adapter.TentativeView.Balance);
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        [Fact]
        public async Task TryAppendRangeWithAnEmptyRangeSucceedsWithoutTouchingKurrent()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                // Fails anything that reaches Kurrent
                client.AddExceptionToThrowForTesting(streamName, new RpcException(new Status(StatusCode.DeadlineExceeded, "sent")), int.MaxValue);

                Assert.True(await adapter.TryAppendRange([]));
                Assert.Equal(0, adapter.ConfirmedVersion);
                Assert.Empty(adapter.UnconfirmedSuffix);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task TryAppendRangeEnumeratesOnceAndKeepsOrder()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new InMemoryKurrentClient();
            var serializer = new TestEventSerializer();
            var streamName = streamNameProvider.GetStreamName(services.GrainId);
            var adapter = CreateAdapter(client, services, streamNameProvider, serializer);

            try
            {
                var entries = new SingleUseEnumerable<TestLogEntry>([new Applied(1), new Applied(2), new Applied(3)]);

                Assert.True(await adapter.TryAppendRange(entries));

                Assert.Equal(1, entries.EnumerationCount);
                Assert.Equal(new[] { 1, 2, 3 }, await ReadAmountsAsync(client, serializer, streamName));
                Assert.Equal(3, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
                await client.DisposeAsync();
            }
        }

        [Fact]
        public async Task SynchronizeDuringABlockedWriteWaitsForTheWriteThenReads()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var blocking = new BlockingKurrentClient(new InMemoryKurrentClient());
            var adapter = CreateAdapter(blocking, services, streamNameProvider);

            try
            {
                blocking.BlockNextAppend();
                adapter.Submit(new Applied(5));
                await blocking.WaitForAppendToStart();

                var synchronize = adapter.Synchronize();
                await Task.Delay(50, TestContext.Current.CancellationToken);
                Assert.False(synchronize.IsCompleted);

                blocking.ReleaseAppend();
                await synchronize;

                Assert.Equal(1, adapter.ConfirmedVersion);
                Assert.Equal(5, adapter.ConfirmedView.Balance);
                Assert.Equal(5, adapter.TentativeView.Balance);
                Assert.Empty(adapter.UnconfirmedSuffix);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task AFailedClearPropagatesAndKeepsBothViews()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppendRange([new Applied(100), new Applied(5)]));

                client.AddExceptionToThrowOnceForTesting(streamName, new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")));
                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ClearLogAsync(TestContext.Current.CancellationToken));

                Assert.Equal(2, adapter.ConfirmedVersion);
                Assert.Equal(105, adapter.ConfirmedView.Balance);
                Assert.Equal(105, adapter.TentativeView.Balance);

                // The stream was not deleted, so it can still be written to
                Assert.True(await adapter.TryAppend(new Applied(1)));
                Assert.Equal(106, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        private static async Task AppendExternallyAsync(IKurrentClient client, TestEventSerializer serializer, string streamName, ulong expectedRevision, int amount)
        {
            var result = await client.ConditionalAppendToStreamAsync(streamName,
                                                                     StreamState.StreamRevision(expectedRevision),
                                                                     [serializer.SerializeEvent(new Applied(amount))],
                                                                     TestContext.Current.CancellationToken);
            Assert.Equal(ConditionalWriteStatus.Succeeded, result.Status);
        }

        private static async Task<int[]> ReadAmountsAsync(IKurrentClient client, TestEventSerializer serializer, string streamName)
        {
            var ct = TestContext.Current.CancellationToken;
            var amounts = new List<int>();
            await foreach (var resolved in await client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start, int.MaxValue, false, ct))
            {
                amounts.Add(((Applied)serializer.DeserializeEvent(resolved)).Amount);
            }

            return [.. amounts];
        }

        private sealed class SingleUseEnumerable<T>(IEnumerable<T> values) : IEnumerable<T>
        {
            public int EnumerationCount { get; private set; }

            public IEnumerator<T> GetEnumerator()
            {
                if (++EnumerationCount > 1)
                {
                    throw new InvalidOperationException("Enumerated more than once.");
                }

                return values.GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
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
                client.AddExceptionToThrowAfterWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));

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
        public async Task ClearLogSucceedsWhenTheTombstoneLandedButItsReplyWasLost()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppendRange([new Applied(100), new Applied(5)]));

                // The tombstone lands but the reply is lost; the retry then finds the stream already deleted
                client.AddExceptionToThrowAfterWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));

                await adapter.ClearLogAsync(TestContext.Current.CancellationToken);

                Assert.Equal(0, adapter.ConfirmedVersion);
                Assert.Equal(0, adapter.ConfirmedView.Balance);
                await Assert.ThrowsAsync<StreamDeletedException>(() => client.TombstoneStreamAsync(streamName, StreamState.Any, TestContext.Current.CancellationToken));
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task TruncationWhoseReplyWasLostIsRetriedWithoutWritingTwice()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                // The $tb update lands but its reply is lost; the retry re-reads the metadata, finds $tb set and does nothing
                client.AddExceptionToThrowAfterMetadataWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));

                Assert.True(await adapter.TryAppendRange([new Applied(25), new Truncate(), new Applied(42)]));
                Assert.Equal(42, adapter.ConfirmedView.Balance);

                var metadata = await client.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
                Assert.Equal(StreamPosition.FromInt64(1), metadata.Metadata.TruncateBefore);
                Assert.Equal(0UL, metadata.MetastreamRevision);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task ClearLogSucceedsWhenSomeoneElseAlreadyDeletedTheStream()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter();

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(100)));

                // Deleted by someone else before this adapter's first attempt; the stream ends up deleted either way
                await client.TombstoneStreamAsync(streamName, StreamState.Any, TestContext.Current.CancellationToken);

                await adapter.ClearLogAsync(TestContext.Current.CancellationToken);

                Assert.Equal(0, adapter.ConfirmedVersion);
                Assert.Equal(0, adapter.ConfirmedView.Balance);

                // The stream is still tombstoned, so it can't be written to again
                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.TryAppend(new Applied(10)));
                Assert.Contains("permanently deleted", Assert.IsType<InvalidOperationException>(ex.InnerException).Message);
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
        public async Task NotLeaderPastTheTimeoutSurfacesUntilResynchronized()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter(Policy(FastRetry, TimeSpan.FromMilliseconds(100)));

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(1)));

                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), int.MaxValue);

                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.TryAppend(new Applied(10)));
                Assert.IsType<NotLeaderException>(Assert.IsType<TimeoutException>(ex.InnerException).InnerException);

                client.ClearExceptionForTesting(streamName);

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
        public async Task AWriteCutShortByTheTimeoutFailsRatherThanLookingLikeDisposal()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var blocking = new BlockingKurrentClient(new InMemoryKurrentClient());
            var adapter = CreateAdapter(blocking, services, streamNameProvider, retry: Policy(FastRetry, TimeSpan.FromMilliseconds(100)));

            try
            {
                blocking.BlockNextAppend();
                adapter.Submit(new Applied(1));
                await blocking.WaitForAppendToStart();

                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ConfirmSubmittedEntries());
                Assert.IsType<TimeoutException>(ex.InnerException);

                // The outcome is unknown, so later writes must not assume where the stream is
                await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.TryAppend(new Applied(10)));

                await adapter.Synchronize();
                Assert.True(await adapter.TryAppend(new Applied(10)));
                Assert.Equal(10, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task ALoadThatOutlastsTheTimeoutIsNotCutShort()
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new BlockingKurrentClient(new InMemoryKurrentClient());
            var adapter = CreateAdapter(client, services, streamNameProvider, retry: Policy(FastRetry, TimeSpan.FromMilliseconds(100)));

            try
            {
                Assert.True(await adapter.TryAppendRange([new Applied(1), new Applied(2)]));

                client.ReadDelay = TimeSpan.FromMilliseconds(300);
                await adapter.Synchronize();
                Assert.Equal(3, adapter.ConfirmedView.Balance);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task ACancellableCallerTokenTakesThePlaceOfTheTimeout()
        {
            var (adapter, client, streamName) = CreateLeaderChangeAdapter(Policy(FastRetry, TimeSpan.FromMilliseconds(100)));

            try
            {
                Assert.True(await adapter.TryAppend(new Applied(1)));

                // The adapter's disposal token can always be cancelled, but the caller's cannot
                client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), int.MaxValue);
                var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => adapter.ClearLogAsync(CancellationToken.None));
                Assert.IsType<TimeoutException>(ex.InnerException);

                using var cancellation = new CancellationTokenSource();
                var clear = adapter.ClearLogAsync(cancellation.Token);

                await Task.Delay(300, TestContext.Current.CancellationToken);
                Assert.False(clear.IsCompleted);

                client.ClearExceptionForTesting(streamName);
                await clear;
                Assert.Equal(0, adapter.ConfirmedVersion);
            }
            finally
            {
                adapter.Dispose();
            }
        }

        [Fact]
        public async Task DisposeStopsRetryingPromptly()
        {
            var slowRetry = new KurrentRetryOptions { BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromSeconds(10) };
            var (adapter, client, streamName) = CreateLeaderChangeAdapter(Policy(slowRetry));

            Assert.True(await adapter.TryAppend(new Applied(1)));

            client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 100);
            adapter.Submit(new Applied(10));

            // Lets the append start and back off before the adapter is disposed
            await Task.Delay(50, TestContext.Current.CancellationToken);
            await adapter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        [Fact]
        public async Task DisposeStopsRetrieveLogSegmentRetryingPromptly()
        {
            var slowRetry = new KurrentRetryOptions { BaseDelay = TimeSpan.FromSeconds(10), MaxDelay = TimeSpan.FromSeconds(10) };
            var (adapter, client, streamName) = CreateLeaderChangeAdapter(Policy(slowRetry));

            Assert.True(await adapter.TryAppend(new Applied(1)));

            client.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 100);
            var segment = adapter.RetrieveLogSegment(0, 1);

            // Lets the read fail and back off before the adapter is disposed
            await Task.Delay(50, TestContext.Current.CancellationToken);
            await adapter.DisposeAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => segment.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        }

        [Fact]
        public async Task InMemoryClientMatchesKurrentIdempotentReplay()
        {
            // Mirrors what a real 3-node KurrentDB cluster returns for each of these
            var client = new InMemoryKurrentClient();
            var ct = TestContext.Current.CancellationToken;
            EventData NewEvent() => new(Uuid.NewUuid(), "event", Array.Empty<byte>(), null);

            var a = NewEvent();
            var stream = $"replay-{Guid.NewGuid():N}";
            Assert.Equal(ConditionalWriteStatus.Succeeded, (await client.ConditionalAppendToStreamAsync(stream, StreamState.NoStream, [a], ct)).Status);
            Assert.Equal(ConditionalWriteStatus.Succeeded, (await client.ConditionalAppendToStreamAsync(stream, StreamState.StreamRevision(0), [NewEvent()], ct)).Status);

            // A resend is still recognised after another writer appended behind it
            var replayed = await client.ConditionalAppendToStreamAsync(stream, StreamState.NoStream, [a], ct);
            Assert.Equal(ConditionalWriteStatus.Succeeded, replayed.Status);
            Assert.Equal(StreamState.StreamRevision(0), replayed.NextExpectedStreamState);

            var c = NewEvent();
            var d = NewEvent();
            var stream2 = $"replay-{Guid.NewGuid():N}";
            Assert.Equal(ConditionalWriteStatus.Succeeded, (await client.ConditionalAppendToStreamAsync(stream2, StreamState.NoStream, [c, d], ct)).Status);

            var whole = await client.ConditionalAppendToStreamAsync(stream2, StreamState.NoStream, [c, d], ct);
            Assert.Equal(ConditionalWriteStatus.Succeeded, whole.Status);
            Assert.Equal(StreamState.StreamRevision(1), whole.NextExpectedStreamState);

            var prefix = await client.ConditionalAppendToStreamAsync(stream2, StreamState.NoStream, [c], ct);
            Assert.Equal(ConditionalWriteStatus.Succeeded, prefix.Status);
            Assert.Equal(StreamState.StreamRevision(0), prefix.NextExpectedStreamState);

            Assert.Equal(ConditionalWriteStatus.VersionMismatch, (await client.ConditionalAppendToStreamAsync(stream2, StreamState.NoStream, [c, NewEvent()], ct)).Status);
            Assert.Equal(2, (await client.ReadStreamAsync(Direction.Forwards, stream2, StreamPosition.Start, 10, false, ct)).ToBlockingEnumerable(ct).Count());
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
            // The server closed (NO_ERROR) or aborted (INTERNAL_ERROR) the connection mid-call, e.g. as it shut down
            { new RpcException(new Status(StatusCode.Internal, "Error reading next message.", new HttpProtocolException(0x0, "The HTTP/2 server closed the connection.", null))), true },
            { new RpcException(new Status(StatusCode.Internal, "Error reading next message.", new HttpProtocolException(0x2, "The HTTP/2 server closed the connection.", null))), true },
            // A call that fails while starting because the node stopped answering keepalive pings (.NET reports PROTOCOL_ERROR)
            { new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", new HttpRequestException(HttpRequestError.HttpProtocolError, "The HTTP/2 server didn't respond to a ping request within the configured KeepAlivePingDelay.", new HttpProtocolException(0x1, "The HTTP/2 server didn't respond to a ping request within the configured KeepAlivePingDelay.", null)))), true },
            { new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", new HttpRequestException("Some other failure."))), false },
            // Any other HTTP/2 error code is retried, such as STREAM_CLOSED from a frozen leader that resumed
            { new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", new HttpRequestException(HttpRequestError.HttpProtocolError, "The HTTP/2 server closed the connection. HTTP/2 error code 'STREAM_CLOSED' (0x5).", new HttpProtocolException(0x5, "The HTTP/2 server closed the connection. HTTP/2 error code 'STREAM_CLOSED' (0x5).", null)))), true },
            { new RpcException(new Status(StatusCode.Internal, "Error reading next message.", new HttpProtocolException(0x3, "Flow control error.", null))), true },
            // HTTP_1_1_REQUIRED won't clear on a retry, whether reported directly or wrapped (see Http2ResetClassificationTests for what a real connection reports)
            { new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", new HttpRequestException(HttpRequestError.HttpProtocolError, "HTTP/1.1 required.", new HttpProtocolException(0xd, "HTTP/1.1 required.", null)))), false },
            { new RpcException(new Status(StatusCode.Internal, "Error reading next message.", new HttpProtocolException(0xd, "HTTP/1.1 required.", null))), false },
            { new RpcException(new Status(StatusCode.Internal, "internal")), false },
            { new RpcException(new Status(StatusCode.Cancelled, "cancelled")), false },
            { new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")), false },
            { new RpcException(new Status(StatusCode.PermissionDenied, "denied")), false },
            { new WrongExpectedVersionException("stream", StreamState.NoStream, StreamState.StreamRevision(1)), false },
            { new OperationCanceledException(), false },
            { new TimeoutException(), false },
        };

        private static readonly KurrentRetryOptions FastRetry = new() { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(5) };

        // A 30s timeout, the Orleans default response timeout
        private static KurrentRetryPolicy Policy(KurrentRetryOptions options, TimeSpan? timeout = null)
            => new(options, Metrics, timeout ?? TimeSpan.FromSeconds(30));

        private static (KurrentLogViewAdapter<TestView, TestLogEntry> Adapter, ExceptionalKurrentClientWrapper Client, string StreamName) CreateLeaderChangeAdapter(KurrentRetryPolicy? retry = null)
        {
            var streamNameProvider = new KurrentStreamName();
            var services = new TestLogConsistencyProtocolServices();
            var client = new ExceptionalKurrentClientWrapper(new InMemoryKurrentClient());
            var adapter = CreateAdapter(client, services, streamNameProvider, retry: retry ?? Policy(FastRetry));
            return (adapter, client, streamNameProvider.GetStreamName(services.GrainId));
        }

        private static KurrentLogViewAdapter<TestView, TestLogEntry> CreateAdapter(IKurrentClient client,
                                                                                    TestLogConsistencyProtocolServices services,
                                                                                    IKurrentStreamNameProvider streamNameProvider,
                                                                                    TestEventSerializer? serializer = null,
                                                                                    KurrentRetryPolicy? retry = null)
            => new(new TestLogViewAdaptorHost(),
                   client,
                   serializer ?? new TestEventSerializer(),
                   services,
                   streamNameProvider,
                   Metrics,
                   retry ?? Policy(new KurrentRetryOptions()));

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

            // Copies views the way the Orleans copier would; a test can replace it
            public Func<object, object> Copier { get; set; } = static value => value is TestView view ? new TestView { Balance = view.Balance } : value;

            public List<Exception> CaughtExceptions { get; } = [];

            public T DeepCopy<T>(T value) => (T)Copier(value!);

            public void ProtocolError(string message, bool throwException)
            {
                if (throwException)
                {
                    throw new InvalidOperationException(message);
                }
            }

            public void CaughtException(string message, Exception exception) => CaughtExceptions.Add(exception);

            public void CaughtUserCodeException(string message, string where, Exception exception)
            {
            }

            public void Log(LogLevel level, string format, params object[] args)
            {
            }
        }

        private sealed class BlockingKurrentClient(IKurrentClient inner) : IKurrentClient
        {
            // One gate per append to block, in the order the appends arrive
            private readonly System.Collections.Concurrent.ConcurrentQueue<AppendGate> gates = new();
            private AppendGate latest = new();

            public TimeSpan ReadDelay { get; set; }

            /// <summary>Blocks the next append that is not already blocked, until its gate is released.</summary>
            public AppendGate BlockNextAppend()
            {
                var gate = new AppendGate();
                latest = gate;
                gates.Enqueue(gate);
                return gate;
            }

            /// <summary>Waits for the append blocked by the latest <see cref="BlockNextAppend"/>.</summary>
            public Task WaitForAppendToStart() => latest.Started;

            /// <summary>Releases the append blocked by the latest <see cref="BlockNextAppend"/>.</summary>
            public void ReleaseAppend() => latest.Release();

            public async Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                                     StreamState expectedRevision,
                                                                                     IEnumerable<EventData> eventData,
                                                                                     CancellationToken cancellationToken)
            {
                if (gates.TryDequeue(out var gate))
                {
                    await gate.Enter(cancellationToken);
                }

                return await inner.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken);
            }

            public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
                => inner.DeleteStreamAsync(streamName, expectedRevision, token);

            public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
                => inner.TombstoneStreamAsync(streamName, expectedRevision, token);

            public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
                => inner.GetStreamMetadata(streamName, token);

            public async ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
            {
                await Task.Delay(ReadDelay, cancellationToken);
                return await inner.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);
            }

            public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
                => inner.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token);

            public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
                => inner.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

            public void Dispose() => inner.Dispose();

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }

        private sealed class AppendGate
        {
            private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>Completes when the blocked append reaches the client.</summary>
            public Task Started => started.Task;

            public void Release() => released.TrySetResult();

            public Task Enter(CancellationToken cancellationToken)
            {
                started.TrySetResult();
                return released.Task.WaitAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Runs <paramref name="test"/> on a scheduler that runs one task at a time, as a grain's does, so the
        /// adapter's continuations resume on it in a fixed order and <see cref="DrainAsync"/> can let them run.
        /// </summary>
        private static Task RunOnGrainLikeSchedulerAsync(Func<Task> test)
            => Task.Factory.StartNew(test, CancellationToken.None, TaskCreationOptions.None, new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler).Unwrap();

        /// <summary>Lets everything already queued on the scheduler from <see cref="RunOnGrainLikeSchedulerAsync"/> run.</summary>
        private static async Task DrainAsync()
        {
            for (var i = 0; i < 20; i++)
            {
                await Task.Yield();
            }
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
