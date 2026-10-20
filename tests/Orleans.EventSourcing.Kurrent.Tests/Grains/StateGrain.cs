namespace Orleans.EventSourcing.Kurrent.Tests.Grains;

[Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.IStateGrain")]
interface IStateGrain : IGrainWithGuidKey
{
    [Alias("GetValue")]
    ValueTask<int> GetValue(CancellationToken cancellationToken = default);
    [Alias("GetEtag")]
    ValueTask<string?> GetEtag(CancellationToken cancellationToken = default);
    [Alias("SetValue")]
    Task SetValue(int value, CancellationToken cancellationToken = default);
    [Alias("ClearValue")]
    Task ClearValue(CancellationToken cancellationToken = default);
    [Alias("RecordExists")]
    ValueTask<bool> RecordExists(CancellationToken cancellationToken = default);
}

record GrainState
{
    public int Value { get; set; } = 42;
}

internal class StateGrain([PersistentState("test")] IPersistentState<GrainState> state) : Grain, IStateGrain
{
    public async Task ClearValue(CancellationToken cancellationToken) => await state.ClearStateAsync(cancellationToken);

    public ValueTask<string?> GetEtag(CancellationToken cancellationToken)
     => ValueTask.FromResult(state.Etag);

    public ValueTask<int> GetValue(CancellationToken cancellationToken) => ValueTask.FromResult(state.State.Value);

    public ValueTask<bool> RecordExists(CancellationToken cancellationToken) => ValueTask.FromResult(state.RecordExists);

    public async Task SetValue(int value, CancellationToken cancellationToken)
    {
        state.State.Value = value;
        await state.WriteStateAsync(cancellationToken);
    }
}
