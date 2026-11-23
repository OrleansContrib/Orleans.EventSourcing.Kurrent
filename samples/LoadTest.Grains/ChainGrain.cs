using Orleans.Concurrency;
using Orleans.EventSourcing;

namespace LoadTest.Grains;

/// <summary>
/// An event that names the event it was decided after, so a log can be checked link by link: every
/// event's <see cref="Prev"/> must be the <see cref="Id"/> of the event stored before it.
/// </summary>
[GenerateSerializer]
[Alias("LoadTest.Grains.Linked")]
public sealed record Linked([property: Id(0)] Guid Id, [property: Id(1)] Guid Prev, [property: Id(2)] string Writer);

[GenerateSerializer]
[Alias("LoadTest.Grains.ChainState")]
public sealed record ChainState
{
    [Id(0)]
    public Guid LastId { get; set; }

    public void Apply(Linked linked) => LastId = linked.Id;
}

[GenerateSerializer]
[Alias("LoadTest.Grains.ChainAppendResult")]
public enum ChainAppendResult
{
    Written,
    Rejected,
}

/// <summary>
/// Appends linked events with conditional writes, for the contention scenario: two clusters each run an
/// activation of the same grain, so both write to the same Kurrent stream and reject each other's stale writes.
/// </summary>
[Alias("LoadTest.Grains.IChainGrain")]
public interface IChainGrain : IGrainWithGuidKey
{
    /// <summary>Raises an event linked to the current tentative state and confirms it conditionally.</summary>
    [Alias("Append")]
    Task<ChainAppendResult> Append(Guid id, string writer);

    /// <summary>Reloads the stream and returns every event in it.</summary>
    [Alias("GetChain")]
    Task<IReadOnlyList<Linked>> GetChain();
}

/// <summary>
/// Reentrant, like the Kurrent reminder table grain, so a call's write can be queued behind another
/// call's write that is still in flight. That is the case where a rejected write must stop the writes
/// queued behind it, since they were decided on a tentative state that included it.
/// </summary>
[Reentrant]
public sealed class ChainGrain : JournaledGrain<ChainState, Linked>, IChainGrain
{
    public async Task<ChainAppendResult> Append(Guid id, string writer)
    {
        if (await RaiseConditionalEvent(new Linked(id, TentativeState.LastId, writer)))
        {
            return ChainAppendResult.Written;
        }

        // The other cluster wrote to the stream; catch up before deciding anything else.
        await RefreshNow();
        return ChainAppendResult.Rejected;
    }

    public async Task<IReadOnlyList<Linked>> GetChain()
    {
        await RefreshNow();
        return await RetrieveConfirmedEvents(0, Version);
    }
}
