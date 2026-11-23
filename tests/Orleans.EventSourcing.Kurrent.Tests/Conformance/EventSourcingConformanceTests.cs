// Scenarios adapted from the Orleans event-sourcing tests (dotnet/orleans, MIT):
// test/Orleans.Runtime.Internal.Tests/GeoClusterTests/BasicLogTestGrainTests.cs,
// test/Orleans.EventSourcing.Tests/EventSourcingTests/LogTestGrainClearTests.cs and CountersGrainTests.cs.
// Each runs against a reentrant and a non-reentrant grain using the Kurrent log-consistency provider.

using Microsoft.Extensions.DependencyInjection;

using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Providers;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.EventSourcing.Kurrent.Tests.Conformance;

public sealed class EventSourcingConformanceClusterFixture : IAsyncLifetime
{
    public InProcessTestCluster Cluster { get; private set; } = null!;

    public IGrainFactory GrainFactory => Cluster.Client;

    public async ValueTask InitializeAsync()
    {
        // A client of its own keeps this cluster's streams apart from other test classes
        var client = new InMemoryKurrentClient();

        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureSilo((_, silo) =>
        {
            silo.Services.AddKeyedSingleton<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, client);
            silo.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = KurrentDB.Client.KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false"));
        });
        builder.Options.ConfigureFileLogging = false;
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
}

public sealed class LogTestGrainConformanceTests(EventSourcingConformanceClusterFixture fixture) : IClassFixture<EventSourcingConformanceClusterFixture>
{
    private const int Timeout = 60_000;

    public static TheoryData<string> GrainClasses =>
    [
        typeof(LogTestGrainNonReentrant).FullName!,
        typeof(LogTestGrainReentrant).FullName!,
    ];

    private ILogTestGrain Grain(string grainClass, long? key = null)
        => fixture.GrainFactory.GetGrain<ILogTestGrain>(key ?? Random.Shared.NextInt64(), grainClass);

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task ThreeCheckers(string grainClass)
    {
        // Global
        async Task Checker1()
        {
            var x = Random.Shared.Next();
            var grain = Grain(grainClass);
            await grain.SetAGlobal(x);
            Assert.Equal(x, await grain.GetAGlobal());
            Assert.Equal(1, await grain.GetConfirmedVersion());
        }

        // Local
        async Task Checker2()
        {
            var x = Random.Shared.Next();
            var grain = Grain(grainClass);
            Assert.Equal(0, await grain.GetConfirmedVersion());
            await grain.SetALocal(x);
            Assert.Equal(x, await grain.GetALocal());
        }

        // Local then global
        async Task Checker3()
        {
            var x = Random.Shared.Next();
            var grain = Grain(grainClass);
            await grain.SetALocal(x);
            Assert.Equal(x, await grain.GetAGlobal());
            Assert.Equal(1, await grain.GetConfirmedVersion());
        }

        await Checker1();
        await Checker2();
        await Checker3();

        var tasks = new List<Task>();
        for (var i = 0; i < 50; i++)
        {
            tasks.Add(Checker1());
            tasks.Add(Checker2());
            tasks.Add(Checker3());
        }

        await Task.WhenAll(tasks);
    }

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task ConditionalUpdatesReportTheVersionTheyWereDecidedOn(string grainClass)
    {
        var grain = Grain(grainClass);

        Assert.Equal(new Tuple<int, bool>(0, true), await grain.SetAConditional(5));
        Assert.Equal(new Tuple<int, bool>(1, true), await grain.SetAConditional(6));
        Assert.Equal(6, await grain.GetAGlobal());
        Assert.Equal(2, await grain.GetConfirmedVersion());
    }

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task LocalEventsAreWrittenByAGlobalRead(string grainClass)
    {
        var grain = Grain(grainClass);

        await grain.AddReservationLocal(1);
        await grain.AddReservationLocal(2);
        await grain.AddReservationLocal(3);
        await grain.RemoveReservationLocal(2);
        await grain.SetBLocal(9);
        await grain.IncrementALocal();

        Assert.Equal(new[] { 1, 3 }, (await grain.GetReservationsGlobal()).Order());
        Assert.Equal(new AB { A = 1, B = 9 }, await grain.GetBothGlobal());
        Assert.Equal(6, await grain.GetConfirmedVersion());
    }

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task TheEventLogIsReturnedInOrder(string grainClass)
    {
        var grain = Grain(grainClass);

        await grain.SetAGlobal(1);
        await grain.IncrementAGlobal();
        await grain.SetBGlobal(2);

        var log = await grain.GetEventLog();
        Assert.Collection(log,
                          e => Assert.Equal(1, Assert.IsType<UpdateA>(e).Val),
                          e => Assert.IsType<IncrementA>(e),
                          e => Assert.Equal(2, Assert.IsType<UpdateB>(e).Val));
    }

    // Upstream writes again after the clear. Kurrent clears by tombstoning the stream, which can never be
    // written to again, so here the final write is expected to fail instead.
    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task ClearDropsTentativeAndConfirmedState(string grainClass)
    {
        var grain = Grain(grainClass);

        // Clearing a log that was never written is a no-op
        await grain.Clear();
        await grain.SetAGlobal(10);
        Assert.Equal(10, await grain.GetAGlobal());
        Assert.Equal(1, await grain.GetConfirmedVersion());

        await grain.SetALocal(99);
        await grain.SetBLocal(77);
        Assert.Equal(new AB { A = 99, B = 77 }, await grain.GetBothLocal());

        await grain.Clear();
        Assert.Equal(0, await grain.GetConfirmedVersion());
        Assert.Equal(default, await grain.GetBothLocal());
        Assert.Equal(default, await grain.GetBothGlobal());

        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => grain.SetAGlobal(7));
        Assert.Contains("permanently deleted", ex.InnerException!.Message, StringComparison.Ordinal);
    }
}

public sealed class CountersGrainConformanceTests(EventSourcingConformanceClusterFixture fixture) : IClassFixture<EventSourcingConformanceClusterFixture>
{
    private const int Timeout = 60_000;

    private static readonly string[] Keys = ["a", "b", "c", "d", "e", "f", "g", "h"];

    public static TheoryData<string> GrainClasses =>
    [
        typeof(CountersGrainNonReentrant).FullName!,
        typeof(CountersGrainReentrant).FullName!,
    ];

    private ICountersGrain Grain(string grainClass)
        => fixture.GrainFactory.GetGrain<ICountersGrain>(Random.Shared.NextInt64(), grainClass);

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public async Task Record(string grainClass)
    {
        var grain = Grain(grainClass);
        Assert.Empty(await grain.GetTentativeState());

        await grain.Add("Alice", 1, false);
        await grain.Add("Alice", 1, false);
        await grain.Add("Alice", 1, false);

        // Visible in the tentative state before they are confirmed
        Assert.Equal(3, await grain.GetTentativeCount("Alice"));

        await grain.Reset(true);
        Assert.Empty(await grain.GetTentativeState());
        Assert.Empty(await grain.GetConfirmedState());
        Assert.Equal(4, (await grain.GetAllEvents()).Count);
    }

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public Task ConcurrentIncrements(string grainClass) => ConcurrentIncrementsRunner(Grain(grainClass), 50, waitForConfirmationOnEach: false);

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(GrainClasses))]
    public Task ConcurrentIncrementsConfirmedEachTime(string grainClass) => ConcurrentIncrementsRunner(Grain(grainClass), 50, waitForConfirmationOnEach: true);

    private static async Task ConcurrentIncrementsRunner(ICountersGrain grain, int count, bool waitForConfirmationOnEach)
    {
        var tasks = new List<Task>();
        for (var i = 0; i < count; i++)
        {
            tasks.Add(grain.Add(Keys[Random.Shared.Next(Keys.Length)], 1, waitForConfirmationOnEach));
        }

        await Task.WhenAll(tasks);

        Assert.Equal(count, (await grain.GetTentativeState()).Values.Sum());

        if (!waitForConfirmationOnEach)
        {
            await grain.ConfirmAllPreviouslyRaisedEvents();
        }

        Assert.Equal(count, (await grain.GetConfirmedState()).Values.Sum());
        Assert.Equal(count, (await grain.GetAllEvents()).Count);

        await grain.Reset(true);
        Assert.Empty(await grain.GetConfirmedState());
    }
}
