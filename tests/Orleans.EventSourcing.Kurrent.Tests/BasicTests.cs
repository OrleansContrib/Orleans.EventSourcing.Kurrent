using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics.Metrics;

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.Core.Internal;
using Orleans.Providers;
using Orleans.Storage;
using Orleans.TestingHost;


using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Observability;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.EventSourcing.Kurrent.Tests.Grains;
using Orleans.EventSourcing.Kurrent.Projections;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class BasicTests : IAsyncLifetime
{
    private const int IntegrationTestTimeout = 30_000;

    static readonly KurrentDBClientSettings clientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

    private InProcessTestCluster cluster = null!;
    private ExceptionalKurrentClientWrapper kurrentClient = null!;
    private IKurrentStreamNameProvider streamNameProvider = null!;

    public async ValueTask InitializeAsync()
    {
        bool useRealKurrent = false; // Toggle to use real kurrent rather than in-memory                 
        
        clientSettings.DefaultDeadline = TimeSpan.FromSeconds(5);
        kurrentClient = new ExceptionalKurrentClientWrapper(useRealKurrent ? new KurrentClient(clientSettings) : new InMemoryKurrentClient()); // Lets us throw exceptions for testing

        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureClient(c => { c.AddActivityPropagation(); });
        builder.ConfigureSilo((s, c) =>
                              {
                                  c.Services.AddKeyedSingleton<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, kurrentClient);
                                  c.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = clientSettings);
                                  c.AddKurrentBasedGrainStorageProviderAsDefault();
                                  c.AddActivityPropagation();
                              });
        builder.Options.ConfigureFileLogging = false;
        cluster = builder.Build();
        await cluster.DeployAsync();

        streamNameProvider = cluster.Silos.First().ServiceProvider
            .GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<Configuration.KurrentStorageOptions>>()
            .Get(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME)
            .StreamNameProvider;
    }

    public async ValueTask DisposeAsync()
    {
        await cluster.DisposeAsync();
        kurrentClient.Dispose();
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Metrics_ArePublishedThroughEachSilosMeterFactory()
    {
        var siloMeters = cluster.Silos.Select(silo =>
        {
            var metrics = silo.ServiceProvider.GetRequiredService<KurrentMetrics>();
            Assert.Equal(KurrentMetrics.MeterName, metrics.Meter.Name);
            Assert.Same(silo.ServiceProvider.GetRequiredService<IMeterFactory>(), metrics.Meter.Scope);
            return metrics.Meter;
        }).ToHashSet();

        var recorded = new ConcurrentBag<string>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (siloMeters.Contains(instrument.Meter))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, _, _) => recorded.Add(instrument.Name));
        listener.Start();

        await cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid()).SetValue(1, TestContext.Current.CancellationToken);
        await cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid()).Deposit(10, TestContext.Current.CancellationToken);

        Assert.Contains("State.Serialization", recorded);
        Assert.Contains("Event.Serialization", recorded);
    }

    private IGrainStorage GrainStorage
        => cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_PreCancelledToken_ThrowsWithoutWriting()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var cancelled = new CancellationToken(canceled: true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GrainStorage.WriteStateAsync("test", grainId, new GrainState<GrainState>(new GrainState { Value = 1 }), cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GrainStorage.ReadStateAsync("test", grainId, new GrainState<GrainState>(), cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GrainStorage.ClearStateAsync("test", grainId, new GrainState<GrainState>(), cancelled));

        var stored = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync("test", grainId, stored, TestContext.Current.CancellationToken);
        Assert.False(stored.RecordExists);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_CancelledDuringRead_ThrowsOperationCanceled()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        using var cancellation = new CancellationTokenSource();
        kurrentClient.CancelOnceForTesting(streamNameProvider.GetStreamName("test", grainId), cancellation);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GrainStorage.ReadStateAsync("test", grainId, new GrainState<GrainState>(), cancellation.Token));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_CancelledDuringWrite_ThrowsInconsistentState()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        using var cancellation = new CancellationTokenSource();
        kurrentClient.CancelOnceForTesting(streamNameProvider.GetStreamName("test", grainId), cancellation);

        // The append may already have been applied, so the outcome is uncertain and recovery is needed
        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => GrainStorage.WriteStateAsync("test", grainId, new GrainState<GrainState>(new GrainState { Value = 1 }), cancellation.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_ReadHealsMissingMaxCountOnTheGrainsStream()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var stateName = $"heal-{Guid.NewGuid():N}";
        var streamName = streamNameProvider.GetStreamName(stateName, grainId);

        // Simulates a crash between the first append and setting $maxCount: the state is written but the metadata is not
        kurrentClient.AddSetMetadataExceptionToThrowOnceForTesting(streamName, new Exception("simulated crash after append"));
        await Assert.ThrowsAsync<InconsistentStateException>(() => GrainStorage.WriteStateAsync(stateName, grainId, new GrainState<GrainState>(new GrainState { Value = 1 }), TestContext.Current.CancellationToken));

        var state = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync(stateName, grainId, state, TestContext.Current.CancellationToken);
        state.State.Value = 2;
        await GrainStorage.WriteStateAsync(stateName, grainId, state, TestContext.Current.CancellationToken);

        // Two events and no $maxCount, so the read repairs the metadata
        var reread = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync(stateName, grainId, reread, TestContext.Current.CancellationToken);
        Assert.Equal(2, reread.State.Value);

        var metadata = await kurrentClient.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Equal(1, metadata.Metadata.MaxCount);

        var misdirected = await kurrentClient.GetStreamMetadata(stateName, TestContext.Current.CancellationToken);
        Assert.Null(misdirected.Metadata.MaxCount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_NotLeaderOnRead_IsRetried()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        await GrainStorage.WriteStateAsync("test", grainId, new GrainState<GrainState>(new GrainState { Value = 7 }), TestContext.Current.CancellationToken);

        kurrentClient.AddExceptionToThrowForTesting(streamNameProvider.GetStreamName("test", grainId), new NotLeaderException("localhost", 2313), 2);

        var state = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.True(state.RecordExists);
        Assert.Equal(7, state.State.Value);
        Assert.Equal("0", state.ETag);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_NotLeaderOnWrite_IsRetried()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var state = new GrainState<GrainState>(new GrainState { Value = 1 });
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);

        kurrentClient.AddExceptionToThrowForTesting(streamNameProvider.GetStreamName("test", grainId), new NotLeaderException("localhost", 2313), 2);

        state.State.Value = 2;
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.Equal("1", state.ETag);

        var reread = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync("test", grainId, reread, TestContext.Current.CancellationToken);
        Assert.Equal(2, reread.State.Value);
        Assert.Equal("1", reread.ETag);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_WriteWhoseReplyWasLost_IsRetriedWithoutDuplicating()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var streamName = streamNameProvider.GetStreamName("test", grainId);
        var state = new GrainState<GrainState>(new GrainState { Value = 1 });

        // The first write lands but its reply is lost; the retry resends the same event ID
        kurrentClient.AddExceptionToThrowAfterWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.Equal("0", state.ETag);

        // $maxCount is still set after the first write
        var metadata = await kurrentClient.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Equal(1, metadata.Metadata.MaxCount);

        kurrentClient.AddExceptionToThrowAfterWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));
        state.State.Value = 2;
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.Equal("1", state.ETag);

        var reread = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync("test", grainId, reread, TestContext.Current.CancellationToken);
        Assert.Equal(2, reread.State.Value);
        Assert.Equal("1", reread.ETag);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_ClearAcrossLeaderChange_IsRetried()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var streamName = streamNameProvider.GetStreamName("test", grainId);
        var state = new GrainState<GrainState>(new GrainState { Value = 1 });
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);

        kurrentClient.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 1);
        await GrainStorage.ClearStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.False(state.RecordExists);

        // A second clear whose delete lands but whose reply is lost; resending a soft delete succeeds again
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        kurrentClient.AddExceptionToThrowAfterWriteOnceForTesting(streamName, new RpcException(new Status(StatusCode.Unavailable, "connection reset")));
        await GrainStorage.ClearStateAsync("test", grainId, state, TestContext.Current.CancellationToken);
        Assert.False(state.RecordExists);

        var reread = new GrainState<GrainState>();
        await GrainStorage.ReadStateAsync("test", grainId, reread, TestContext.Current.CancellationToken);
        Assert.False(reread.RecordExists);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_NonTransientFailures_AreNotRetried()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));

        // A retry would succeed, so the failure only surfaces if the write is not retried
        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName("test", grainId), new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")));

        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => GrainStorage.WriteStateAsync("test", grainId, new GrainState<GrainState>(new GrainState { Value = 1 }), TestContext.Current.CancellationToken));
        Assert.IsType<RpcException>(ex.InnerException);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateGrain_ActivatesAndWritesAcrossLeaderChange()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        var streamName = streamNameProvider.GetStreamName("test", stateGrain.GetGrainId());
        await stateGrain.SetValue(5, TestContext.Current.CancellationToken);
        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        // Activation reads the state from the old leader first
        kurrentClient.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 2);
        Assert.Equal(5, await stateGrain.GetValue(TestContext.Current.CancellationToken));

        kurrentClient.AddExceptionToThrowForTesting(streamName, new NotLeaderException("localhost", 2313), 2);
        await stateGrain.SetValue(6, TestContext.Current.CancellationToken);
        Assert.Equal(6, await stateGrain.GetValue(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainStorage_CancelledDuringClear_ThrowsInconsistentState()
    {
        var grainId = GrainId.Create("test", Guid.NewGuid().ToString("N"));
        var state = new GrainState<GrainState>(new GrainState { Value = 1 });
        await GrainStorage.WriteStateAsync("test", grainId, state, TestContext.Current.CancellationToken);

        using var cancellation = new CancellationTokenSource();
        kurrentClient.CancelOnceForTesting(streamNameProvider.GetStreamName("test", grainId), cancellation);

        // The delete may already have been applied, so the outcome is uncertain and recovery is needed
        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => GrainStorage.ClearStateAsync("test", grainId, state, cancellation.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task DepositWithdraw()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        await account.Deposit(10, TestContext.Current.CancellationToken);
        Assert.Equal(10, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.False(await account.Withdraw(15, TestContext.Current.CancellationToken));
        Assert.True(await account.Withdraw(5, TestContext.Current.CancellationToken));
        Assert.Equal(5, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(5, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        var @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Equal(2, @events.Count);
        var deposited = Assert.IsType<AccountEvent.Deposited>(events[0]);
        Assert.Equal(10, deposited.Amount);
        var withdrawn = Assert.IsType<AccountEvent.Withdrawn>(events[1]);
        Assert.Equal(5, withdrawn.Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task CheckEventsNumbering()
    {
        // GetEvents calls internally RetrieveConfirmedEvents(0, ConfirmedVersion);
        // by checking the explicit version we check events start at 1
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        var @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Empty(@events);
        Assert.Equal(0, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        await account.Deposit(10, TestContext.Current.CancellationToken);

        @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Single(@events);
        Assert.Equal(1, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(10, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1, TestContext.Current.CancellationToken)).Amount);

        await account.RefreshNow(TestContext.Current.CancellationToken); // re-read state from storage

        @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Single(@events);
        Assert.Equal(1, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(10, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1, TestContext.Current.CancellationToken)).Amount);

        await account.Withdraw(5, TestContext.Current.CancellationToken);

        @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Equal(2, @events.Count);
        Assert.Equal(2, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(5, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1, TestContext.Current.CancellationToken)).Amount);
        Assert.Equal(5, Assert.IsType<AccountEvent.Withdrawn>(await account.GetEventAtVersion(2, TestContext.Current.CancellationToken)).Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task EventIdPropagation()
    {
        var eventId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10, eventId, TestContext.Current.CancellationToken);
        var @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Single(@events);
        var deposited = Assert.IsType<AccountEvent.Deposited>(events[0]);
        Assert.Equal(10, deposited.Amount);
        Assert.Equal(eventId, deposited.EventId);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task CheckVersionMismatch()
    {
        var accountGrainId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        await account.Deposit(100, TestContext.Current.CancellationToken);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName(account.GetGrainId()));

        Assert.Equal(100, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.False(await account.Withdraw(66, TestContext.Current.CancellationToken));
        Assert.Equal(34, await account.GetTentativeBalance(TestContext.Current.CancellationToken));
        Assert.Equal(100, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.False(await account.Withdraw(33, TestContext.Current.CancellationToken));
        Assert.Equal(1, await account.GetTentativeBalance(TestContext.Current.CancellationToken));
        Assert.Equal(100, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        await Assert.ThrowsAsync<InconsistentStateException>(async () => { await account.Deposit(11, TestContext.Current.CancellationToken); }); // causes grain restart
        Assert.Equal(200, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        await account.Deposit(22, TestContext.Current.CancellationToken);
        Assert.Equal(222, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        var @events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Equal(3, @events.Count);
        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(events[0]).Amount);
        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(events[1]).Amount);
        Assert.Equal(22, Assert.IsType<AccountEvent.Deposited>(events[2]).Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task CheckHandleTruncation()
    {
        var kurrentClient = cluster.Silos.First().ServiceProvider.GetRequiredKeyedService<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        Assert.NotNull(kurrentClient);

        var accountGrainId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        var powersOfTwo = Enumerable.Range(0, 10).Select(i => (int)Math.Pow(2, i)).ToList();

        foreach (var i in powersOfTwo)
        {
            await account.Deposit(i, TestContext.Current.CancellationToken);
        }
        foreach (var i in powersOfTwo)
        {
            await account.Deposit(i, TestContext.Current.CancellationToken);
            await account.Withdraw(i, TestContext.Current.CancellationToken);
        }

        Assert.Equal((powersOfTwo[^1] * 2) - 1, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(powersOfTwo.Count * 3, (await account.GetEvents(TestContext.Current.CancellationToken)).Count);

        // remove the first iteration of deposits
        await kurrentClient.SetStreamMetadata(streamNameProvider.GetStreamName(account.GetGrainId()), StreamState.NoStream, new StreamMetadata(truncateBefore: StreamPosition.FromStreamRevision(10)), TestContext.Current.CancellationToken);
        Assert.Equal(powersOfTwo.Count * 2, (await account.GetEvents(TestContext.Current.CancellationToken)).Count);

        Assert.Equal((powersOfTwo[^1] * 2) - 1, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        await account.RefreshNow(TestContext.Current.CancellationToken); // re-read state from storage

        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        // remove the events from all above iterations and check the last added event remains
        await account.Deposit(77, TestContext.Current.CancellationToken);
        Assert.Equal(77, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        await kurrentClient.SetStreamMetadata(streamNameProvider.GetStreamName(account.GetGrainId()), StreamState.StreamRevision(0), new StreamMetadata(truncateBefore: StreamPosition.FromStreamRevision(30)), TestContext.Current.CancellationToken);
        Assert.Single(await account.GetEvents(TestContext.Current.CancellationToken));
        await account.RefreshNow(TestContext.Current.CancellationToken);
        Assert.Equal(77, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task CheckHandleExceptionTest()
    {
        var accountGrainId = Guid.NewGuid();

        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new TimeoutException("Test read exception"));

        var ex1 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(11, TestContext.Current.CancellationToken));
        Assert.IsType<TimeoutException>(ex1.InnerException);

        await account.Deposit(22, TestContext.Current.CancellationToken);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new RpcException(new Status(StatusCode.DeadlineExceeded, "RPC timeout blah blah")));

        await account.DepositWithoutConfirm(33, TestContext.Current.CancellationToken); // exception will be seen on following write

        var ex2 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(5, TestContext.Current.CancellationToken));
        Assert.IsType<RpcException>(ex2.InnerException);

        Assert.Equal(22, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new StreamDeletedException("stream name"));

        var ex3 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(11, TestContext.Current.CancellationToken));
        Assert.IsType<StreamDeletedException>(ex3.InnerException);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateGrainCRUD()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        Assert.False(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal(42, await stateGrain.GetValue(TestContext.Current.CancellationToken));
        Assert.Equal(string.Empty, await stateGrain.GetEtag(TestContext.Current.CancellationToken));

        await stateGrain.SetValue(99393, TestContext.Current.CancellationToken);
        Assert.True(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal("0", await stateGrain.GetEtag(TestContext.Current.CancellationToken));

        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(99393, await stateGrain.GetValue(TestContext.Current.CancellationToken));
        Assert.True(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal("0", await stateGrain.GetEtag(TestContext.Current.CancellationToken));

        await stateGrain.ClearValue(TestContext.Current.CancellationToken);
        Assert.Equal(string.Empty, await stateGrain.GetEtag(TestContext.Current.CancellationToken));
        Assert.False(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal(42, await stateGrain.GetValue(TestContext.Current.CancellationToken));

        await stateGrain.ClearValue(TestContext.Current.CancellationToken);
        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        // Check state after clear
        Assert.False(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal(string.Empty, await stateGrain.GetEtag(TestContext.Current.CancellationToken));
        Assert.Equal(42, await stateGrain.GetValue(TestContext.Current.CancellationToken));

        // Check reading and writing after clearing

        await stateGrain.SetValue(519235351, TestContext.Current.CancellationToken);
        Assert.True(await stateGrain.RecordExists(TestContext.Current.CancellationToken));
        Assert.Equal(519235351, await stateGrain.GetValue(TestContext.Current.CancellationToken));
        Assert.Equal("1", await stateGrain.GetEtag(TestContext.Current.CancellationToken));

        await stateGrain.ClearValue(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TestDeleteOnNonExistantState()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        await stateGrain.ClearValue(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateGrainWriteETagTest()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        await stateGrain.SetValue(99393, TestContext.Current.CancellationToken);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()));

        await Assert.ThrowsAsync<InconsistentStateException>(() => stateGrain.SetValue(356, TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateGrainClearETagTest()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        await stateGrain.SetValue(99393, TestContext.Current.CancellationToken);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()));

        await Assert.ThrowsAsync<InconsistentStateException>(() => stateGrain.ClearValue(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionAbstractBaseType()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<NotSupportedException>(() => grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionNoTypes()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() => grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionCannotCastToBaseEvent()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() => grainProjectionProvider.SubscribeToGrainEvents<string>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(object)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionAggregation()
    {
        Guid[] idsForThisTest = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];

        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);

        var projection = Task.Run<(decimal totalBalance, decimal averageCreditRating)>(async () =>
        {
            await Task.Yield();
            decimal total = 0;
            int expectedEvents = 4;
            int creditRatingSum = 0;
            int creditRatingCount = 0;

            await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(UserEvent.UserCreditRatingChanged)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
            {
                if (update is not GrainEvent<object> grainEvent
                || !idsForThisTest.Contains(grainEvent.EventGrainId.GetGuidKey()))
                {
                    continue; // skip events from other unit tests
                }

                switch (grainEvent.Event)
                {
                    case AccountEvent.Deposited deposited:
                        total += deposited.Amount;
                        break;
                    case UserEvent.UserCreditRatingChanged userCreditRatingChanged:
                        creditRatingSum += userCreditRatingChanged.CreditRating;
                        creditRatingCount++;
                        break;
                }
                if (--expectedEvents == 0)
                {
                    break;
                }
            }
            return (total, creditRatingCount > 0 ? creditRatingSum / creditRatingCount : 0);
        });

        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[0]).Deposit(45, TestContext.Current.CancellationToken);
        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[1]).Deposit(831, TestContext.Current.CancellationToken);
        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[2]).Deposit(1563, TestContext.Current.CancellationToken);
        await cluster.Client.GetGrain<IUserGrain>(idsForThisTest[3]).UpdateCreditRating(5);

        var (totalBalance, average) = await projection;

        Assert.Equal(45 + 831 + 1563, totalBalance);
        Assert.Equal(5, average);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionSubscribeReturnsEventsInOrder()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100, TestContext.Current.CancellationToken);
        await account.Deposit(200, TestContext.Current.CancellationToken);
        await account.Withdraw(50, TestContext.Current.CancellationToken);

        var receivedEvents = new List<object>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.Event);
                if (receivedEvents.Count == 3)
                    break;
            }
        }

        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(receivedEvents[0]).Amount);
        Assert.Equal(200, Assert.IsType<AccountEvent.Deposited>(receivedEvents[1]).Amount);
        Assert.Equal(50, Assert.IsType<AccountEvent.Withdrawn>(receivedEvents[2]).Amount);
    }



    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionSubscribeNotificationReturnsEventsInOrder()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100, TestContext.Current.CancellationToken);
        await account.Deposit(200, TestContext.Current.CancellationToken);
        await account.Withdraw(50, TestContext.Current.CancellationToken);

        var receivedEvents = new List<int>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEventNotifications(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken).ConfigureAwait(false))
        {
            if (update is GrainEventNotification grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.EventGrainVersion);
                if (receivedEvents.Count == 3)
                    break;
            }
        }

        Assert.Equal(1, receivedEvents[0]);
        Assert.Equal(2, receivedEvents[1]);
        Assert.Equal(3, receivedEvents[2]);
    }


    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionEventNumbersMatchGrainVersionNumbers()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100, TestContext.Current.CancellationToken);

        var receivedEventVersions = new List<int>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEventVersions.Add(grainEvent.EventGrainVersion);
                if (receivedEventVersions.Count == 1)
                    break;
            }
        }

        // Orleans event-sourcing starts with version 1 when the first event is written, version 0 means no events written. Kurrent starts with version 0 when first event is written
        Assert.Equal(1, receivedEventVersions[0]);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionSubscribeSkipsUnrelatedEvents()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var userId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);
        var user = cluster.Client.GetGrain<IUserGrain>(userId);

        await account.Deposit(123, TestContext.Current.CancellationToken);
        await user.UpdateCreditRating(7);

        var receivedEvents = new List<object>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.Event);
                break;
            }
        }

        Assert.Single(receivedEvents);
        Assert.Equal(123, Assert.IsType<AccountEvent.Deposited>(receivedEvents[0]).Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionThrowsOnNullTypeArray()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, null!, TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionThrowsOnDefaultSubscriber()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            grainProjectionProvider.SubscribeToGrainEvents<object>(default, GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ProjectionCaughtUp()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is CaughtUp)
            {
                break;
            }
        }
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ClearLog_OnEmptyStream_IsNoOp()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.ClearLog(TestContext.Current.CancellationToken);

        Assert.Equal(0, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Empty(await account.GetEvents(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ClearLog_AfterAppends_ResetsViewsAndAllowsReAppend()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.Deposit(10, TestContext.Current.CancellationToken);
        await account.Deposit(20, TestContext.Current.CancellationToken);
        Assert.Equal(2, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(30, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        await account.ClearLog(TestContext.Current.CancellationToken);

        Assert.Equal(0, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(0, await account.GetTentativeBalance(TestContext.Current.CancellationToken));
        Assert.Empty(await account.GetEvents(TestContext.Current.CancellationToken));

        // Re-appending after a hard delete should fail,
        await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(7, TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ClearLog_PersistsAcrossDeactivation()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(50, TestContext.Current.CancellationToken);
        await account.ClearLog(TestContext.Current.CancellationToken);

        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(0, await account.GetConfirmedVersion(TestContext.Current.CancellationToken));
        Assert.Equal(0, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Empty(await account.GetEvents(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ClearLog_PropagatesKurrentExceptions()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.Deposit(10, TestContext.Current.CancellationToken);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()),
                                                         new TimeoutException("Test delete exception"));

        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => account.ClearLog(TestContext.Current.CancellationToken));
        Assert.IsType<TimeoutException>(ex.InnerException);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TruncateAccount_HidesEventsPriorToMarker()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(10, TestContext.Current.CancellationToken);
        await account.Deposit(20, TestContext.Current.CancellationToken);
        await account.TruncateAccount(TestContext.Current.CancellationToken);   // truncation marker – events 0 and 1 should become hidden on reload
        await account.Deposit(5, TestContext.Current.CancellationToken);

        // Deactivate so the grain reloads state from storage (respecting $tb).
        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        // After reload only the post-truncation events should contribute to the balance.
        Assert.Equal(35, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));

        // Only the events from the truncation marker onwards should be visible.
        var events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Equal(2, events.Count); // Truncation + Deposited(5)
        Assert.IsType<AccountEvent.Truncation>(events[0]);
        Assert.IsType<AccountEvent.Deposited>(events[1]);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TruncateAccount_ResetsTentativeAndConfirmedViewsAroundPersistence()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100, TestContext.Current.CancellationToken);
        await account.DepositWithoutConfirm(25, TestContext.Current.CancellationToken);
        await account.TruncateAccountWithoutConfirm(TestContext.Current.CancellationToken);

        Assert.Equal(125, await account.GetTentativeBalance(TestContext.Current.CancellationToken));

        await account.DepositWithoutConfirm(42, TestContext.Current.CancellationToken);

        Assert.Equal(167, await account.GetTentativeBalance(TestContext.Current.CancellationToken));

        await account.ConfirmPendingEvents(TestContext.Current.CancellationToken);

        Assert.Equal(167, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Equal(167, await account.GetTentativeBalance(TestContext.Current.CancellationToken));

        var events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Equal(2, events.Count);
        Assert.IsType<AccountEvent.Truncation>(events[0]);
        Assert.Equal(42, Assert.IsType<AccountEvent.Deposited>(events[1]).Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TruncateAccount_ViewRebuiltCorrectlyAfterDeactivation()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100, TestContext.Current.CancellationToken);
        await account.TruncateAccount(TestContext.Current.CancellationToken);
        await account.Deposit(42, TestContext.Current.CancellationToken);

        // Deactivate so the grain reloads from storage
        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(142, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TruncateAccount_OnFirstEventTruncatesEntireHistory()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(50, TestContext.Current.CancellationToken);
        await account.Deposit(50, TestContext.Current.CancellationToken);

        // Raise truncation as the sole event in a batch.
        await account.TruncateAccount(TestContext.Current.CancellationToken);

        // Deactivate so the grain reloads from storage with $tb applied.
        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(100, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        Assert.Single(await account.GetEvents(TestContext.Current.CancellationToken)); // only the Truncation event
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task TruncateAccount_LoadHealsUnsetTruncationMetadata()
    {
        // Simulates a crash between the ConditionalAppendToStreamAsync and SetStreamMetadata calls:
        // the truncation marker event is persisted but $tb is never written. The next Load should
        // detect the marker, reconstruct the correct view, and idempotently re-apply $tb.
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);
        var streamName = streamNameProvider.GetStreamName(account.GetGrainId());

        await account.Deposit(100, TestContext.Current.CancellationToken);
        await account.Deposit(50, TestContext.Current.CancellationToken);

        // Make the SetStreamMetadata call throw so the truncation marker is written but $tb is not set.
        kurrentClient.AddSetMetadataExceptionToThrowOnceForTesting(streamName, new Exception("simulated crash after append"));
        await Assert.ThrowsAsync<InconsistentStateException>(() => account.TruncateAccount(TestContext.Current.CancellationToken));


        // After self-healing only the Truncation event should contribute to the view (balance = 0).
        Assert.Equal(150, await account.GetConfirmedBalance(TestContext.Current.CancellationToken));
        var events = await account.GetEvents(TestContext.Current.CancellationToken);
        Assert.Single(events);
        Assert.IsType<AccountEvent.Truncation>(events[0]);
    }
}
