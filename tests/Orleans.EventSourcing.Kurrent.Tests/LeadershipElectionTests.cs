using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.Core.Internal;
using Orleans.Providers;
using Orleans.TestingHost;

using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Projections;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.EventSourcing.Kurrent.Tests.Grains;

namespace Orleans.EventSourcing.Kurrent.Tests;

/// <summary>
///     Simulates the failures a KurrentDB cluster leadership election produces: in-flight operations fail with
///     <see cref="NotLeaderException"/> or <see cref="RpcException"/> (Unavailable), and catch-up subscriptions are
///     dropped. The official client rediscovers the new leader for subsequent calls, so these failures are transient;
///     the provider is expected to absorb them and complete the operation without surfacing an error to the grain.
/// </summary>
public sealed class LeadershipElectionTests : IAsyncLifetime
{
    private const int IntegrationTestTimeout = 30_000;

    private static readonly KurrentDBClientSettings clientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

    private InProcessTestCluster cluster = null!;
    private ExceptionalKurrentClientWrapper kurrentClient = null!;
    private IKurrentStreamNameProvider streamNameProvider = null!;

    private static NotLeaderException NewNotLeaderException()
        => new("127.0.0.1", 2114, new RpcException(new Status(StatusCode.NotFound, "Leader info available in exception")));

    private static RpcException NewUnavailableRpcException()
        => new(new Status(StatusCode.Unavailable, "Server is not ready to accept requests (leader election in progress)"));

    public async ValueTask InitializeAsync()
    {
        kurrentClient = new ExceptionalKurrentClientWrapper(new InMemoryKurrentClient());

        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureSilo((s, c) =>
                              {
                                  c.Services.AddKeyedSingleton<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, kurrentClient);
                                  c.AddKurrentBasedLogConsistencyProviderAsDefault(o =>
                                  {
                                      o.ClientSettings = clientSettings;
                                      o.Retry.BaseDelay = TimeSpan.FromMilliseconds(10);
                                      o.Retry.MaxDelay = TimeSpan.FromMilliseconds(50);
                                  });
                                  c.AddKurrentBasedGrainStorageProviderAsDefault();
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

    #region Log-consistency provider (JournaledGrain)

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Append_RecoversFromNotLeaderException()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewNotLeaderException());

        await account.Deposit(20); // must succeed despite the transient leader change

        Assert.Equal(30, await account.GetConfirmedBalance());
        Assert.Equal(2, (await account.GetEvents()).Count);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Append_RecoversFromUnavailableRpcException()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewUnavailableRpcException());

        await account.Deposit(20);

        Assert.Equal(30, await account.GetConfirmedBalance());
        Assert.Equal(2, (await account.GetEvents()).Count);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Append_RecoversFromElectionSpanningMultipleAttempts()
    {
        // A leadership election typically lasts a few seconds, so several consecutive attempts may fail
        // before the client reconnects to the new leader.
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewNotLeaderException(), count: 3);

        await account.Deposit(20);

        Assert.Equal(30, await account.GetConfirmedBalance());
        Assert.Equal(2, (await account.GetEvents()).Count);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task LazyAppend_IsNotLostAfterTransientFailure()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewNotLeaderException());

        await account.DepositWithoutConfirm(20); // lazy write hits the transient failure in the background
        await account.ConfirmPendingEvents();    // confirming must succeed and not lose the pending event

        Assert.Equal(30, await account.GetConfirmedBalance());
        Assert.Equal(2, (await account.GetEvents()).Count);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Load_RecoversFromNotLeaderException()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);
        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewNotLeaderException());

        // Reactivation replays the stream; the transient read failure must not fail the grain call.
        Assert.Equal(10, await account.GetConfirmedBalance());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task GrainRemainsUsableAfterElectionWindow()
    {
        // Even if a write fails during the election, the grain must remain usable afterwards
        // without the caller having to tolerate a failed call.
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), NewUnavailableRpcException(), count: 2);

        await account.Deposit(20);
        Assert.True(await account.Withdraw(5));

        Assert.Equal(25, await account.GetConfirmedBalance());
        Assert.Equal(3, (await account.GetEvents()).Count);
    }

    #endregion

    #region Grain-storage provider ([PersistentState])

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateRead_RecoversFromNotLeaderException()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        await stateGrain.SetValue(123);
        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()), NewNotLeaderException());

        Assert.Equal(123, await stateGrain.GetValue());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateWrite_RecoversFromUnavailableRpcException()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        await stateGrain.SetValue(123);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()), NewUnavailableRpcException());

        await stateGrain.SetValue(456);

        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        Assert.Equal(456, await stateGrain.GetValue());
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task StateClear_RecoversFromNotLeaderException()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        await stateGrain.SetValue(123);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()), NewNotLeaderException());

        await stateGrain.ClearValue();

        Assert.False(await stateGrain.RecordExists());
    }

    #endregion

    #region Projections (catch-up subscription)

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Subscription_ResubscribesAfterBeingDroppedByLeaderChange()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100);
        await account.Deposit(200);

        kurrentClient.AddSubscriptionExceptionToThrowOnceForTesting(NewNotLeaderException());

        var receivedEvents = new List<object>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.Event);
                if (receivedEvents.Count == 2)
                    break;
            }
        }

        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(receivedEvents[0]).Amount);
        Assert.Equal(200, Assert.IsType<AccountEvent.Deposited>(receivedEvents[1]).Amount);
    }

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task Subscription_SurfacesFailureAfterMaxAttempts()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var maxAttempts = new Configuration.KurrentRetryOptions().MaxAttempts;

        kurrentClient.AddSubscriptionExceptionToThrowForTesting(NewNotLeaderException(), count: maxAttempts);

        await Assert.ThrowsAsync<NotLeaderException>(async () =>
        {
            await foreach (var _ in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
            {
            }
        });
    }

    #endregion
}
