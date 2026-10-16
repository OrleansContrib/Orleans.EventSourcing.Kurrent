using System.Text.Json.Serialization;

using KurrentDB.Client;

using Orleans.Core.Internal;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Journaling;
using Orleans.Journaling.Json;
using Orleans.TestingHost;

namespace Orleans.EventSourcing.Kurrent.Journaling.Tests;

public interface ICartGrain : IGrainWithGuidKey
{
    ValueTask SetItem(string itemId, int quantity);

    ValueTask RemoveItem(string itemId);

    ValueTask<Dictionary<string, int>> GetItems();

    ValueTask Clear();
}

public sealed class CartGrain(IDurableStateManager stateManager, IJournaledStateManager journal) : Grain, ICartGrain
{
    private readonly IDurableDictionary<string, int> cart = stateManager.GetOrAddDictionary<string, int>("cart");

    public async ValueTask SetItem(string itemId, int quantity)
    {
        cart[itemId] = quantity;
        await stateManager.WriteStateAsync();
    }

    public async ValueTask RemoveItem(string itemId)
    {
        cart.Remove(itemId);
        await stateManager.WriteStateAsync();
    }

    public ValueTask<Dictionary<string, int>> GetItems() => new(cart.ToDictionary());

    public async ValueTask Clear() => await journal.DeleteStateAsync();
}

public sealed class DurableGrainTests : IAsyncLifetime
{
    private const int TestTimeout = 30_000;
    private const int CompactionEventCountThreshold = 5;

    private InProcessTestCluster cluster = null!;

    public async ValueTask InitializeAsync()
    {
        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureSilo((_, silo) =>
            silo.AddKurrentJournalStorageAsDefault(options =>
                {
                    options.ClientSettings = KurrentDBClientSettings.Create("esdb://kurrentemulator:2113?tls=false");
                    options.CompactionEventCountThreshold = CompactionEventCountThreshold;
                })
                .UseJsonJournalFormat(CartJournalJsonContext.Default));
        builder.Options.ConfigureFileLogging = false;
        cluster = builder.Build();
        await cluster.DeployAsync();
    }

    public async ValueTask DisposeAsync() => await cluster.DisposeAsync();

    [Fact(Timeout = TestTimeout)]
    public async Task StateSurvivesReactivation()
    {
        var cart = cluster.Client.GetGrain<ICartGrain>(Guid.NewGuid());
        await cart.SetItem("apple", 3);
        await cart.SetItem("pear", 1);
        await cart.RemoveItem("pear");

        await cart.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(new Dictionary<string, int> { ["apple"] = 3 }, await cart.GetItems());
    }

    [Fact(Timeout = TestTimeout)]
    public async Task CompactionTruncatesStreamAndPreservesState()
    {
        var cart = cluster.Client.GetGrain<ICartGrain>(Guid.NewGuid());
        for (var i = 0; i < CompactionEventCountThreshold * 3; i++)
        {
            await cart.SetItem($"item-{i % 4}", i);
        }

        var kurrent = InMemoryKurrentClient.Get(Providers.ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);
        var streamName = KurrentJournalStorageOptions.DefaultGetStreamName(JournalId.FromGrainId(cart.GetGrainId()));
        var events = await kurrent.ReadVisibleEventsAsync(streamName);

        // Compaction has run: the oldest visible event is a snapshot, and older events are hidden by $tb.
        Assert.Equal(KurrentJournalEvents.SnapshotEventType, events[0].Event.EventType);
        Assert.True(events.Count <= CompactionEventCountThreshold + 1, $"Expected the stream to be truncated, but {events.Count} events are visible.");
        var metadata = await kurrent.GetStreamMetadata(streamName, TestContext.Current.CancellationToken);
        Assert.Equal(events[0].Event.EventNumber, metadata.Metadata.TruncateBefore);

        await cart.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(new Dictionary<string, int> { ["item-0"] = 12, ["item-1"] = 13, ["item-2"] = 14, ["item-3"] = 11 }, await cart.GetItems());
    }

    [Fact(Timeout = TestTimeout)]
    public async Task ClearedStateCanBeWrittenAgain()
    {
        var cart = cluster.Client.GetGrain<ICartGrain>(Guid.NewGuid());
        await cart.SetItem("apple", 3);
        await cart.Clear();

        await cart.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);
        Assert.Empty(await cart.GetItems());

        await cart.SetItem("pear", 2);
        await cart.AsReference<IGrainManagementExtension>().DeactivateOnIdle(TestContext.Current.CancellationToken);

        Assert.Equal(new Dictionary<string, int> { ["pear"] = 2 }, await cart.GetItems());
    }
}

// JSON journaling needs source-generated metadata for every journaled type: the cart's keys and values, plus the
// string/uint and string/DateTime dictionaries Orleans uses to track state names and retirements.
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(uint))]
[JsonSerializable(typeof(DateTime))]
internal sealed partial class CartJournalJsonContext : JsonSerializerContext;
