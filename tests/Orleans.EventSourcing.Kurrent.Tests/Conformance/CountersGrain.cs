// Adapted from the Orleans counters grain (dotnet/orleans, test/Grains/TestGrains/EventSourcing/CountersGrain.cs,
// CountersGrainVariations.cs and test/Grains/TestGrainInterfaces/EventSourcing/ICountersGrain.cs, MIT). The
// state-storage and log-storage variants are replaced by Kurrent ones, keeping the reentrant and non-reentrant pair.

using Orleans.Concurrency;

namespace Orleans.EventSourcing.Kurrent.Tests.Conformance;

/// <summary>
/// Maintains a number of counters, indexed by a string key.
/// </summary>
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.ICountersGrain")]
public interface ICountersGrain : IGrainWithIntegerKey
{
    [Alias("Add")]
    Task Add(string key, int amount, bool waitForConfirmation);

    [Alias("Reset")]
    Task Reset(bool waitForConfirmation);

    [Alias("GetTentativeCount")]
    Task<int> GetTentativeCount(string key);

    [Alias("GetTentativeState")]
    Task<IReadOnlyDictionary<string, int>> GetTentativeState();

    [Alias("GetConfirmedState")]
    Task<IReadOnlyDictionary<string, int>> GetConfirmedState();

    [Alias("ConfirmAllPreviouslyRaisedEvents")]
    Task ConfirmAllPreviouslyRaisedEvents();

    [Alias("GetAllEvents")]
    Task<IReadOnlyList<object>> GetAllEvents();
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.CountersState")]
public sealed class CountersState
{
    [Id(0)]
    public Dictionary<string, int> Counts { get; set; } = [];

    public void Apply(CounterUpdated e) => Counts[e.Key] = Counts.GetValueOrDefault(e.Key) + e.Amount;

    public void Apply(CountersReset _) => Counts.Clear();
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.CounterUpdated")]
public sealed class CounterUpdated
{
    [Id(0)]
    public string Key { get; set; } = null!;

    [Id(1)]
    public int Amount { get; set; }
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.CountersReset")]
public sealed class CountersReset;

public abstract class CountersGrain : JournaledGrain<CountersState, object>, ICountersGrain
{
    // Load lazily rather than waiting for the current state on activation
    public override Task OnActivateAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task Add(string key, int amount, bool waitForConfirmation)
    {
        RaiseEvent(new CounterUpdated { Key = key, Amount = amount });

        if (waitForConfirmation)
        {
            await ConfirmEvents();
        }
    }

    public async Task Reset(bool waitForConfirmation)
    {
        RaiseEvent(new CountersReset());

        if (waitForConfirmation)
        {
            await ConfirmEvents();
        }
    }

    public Task ConfirmAllPreviouslyRaisedEvents() => ConfirmEvents();

    public Task<int> GetTentativeCount(string key) => Task.FromResult(TentativeState.Counts[key]);

    public Task<IReadOnlyDictionary<string, int>> GetTentativeState() => Task.FromResult<IReadOnlyDictionary<string, int>>(new Dictionary<string, int>(TentativeState.Counts));

    public Task<IReadOnlyDictionary<string, int>> GetConfirmedState() => Task.FromResult<IReadOnlyDictionary<string, int>>(new Dictionary<string, int>(State.Counts));

    public Task<IReadOnlyList<object>> GetAllEvents() => RetrieveConfirmedEvents(0, Version);
}

public sealed class CountersGrainNonReentrant : CountersGrain;

[Reentrant]
public sealed class CountersGrainReentrant : CountersGrain;
