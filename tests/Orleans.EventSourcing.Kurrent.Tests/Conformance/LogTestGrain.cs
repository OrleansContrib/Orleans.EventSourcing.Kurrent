// Adapted from the Orleans log-consistency test grain (dotnet/orleans, test/Grains/TestGrains/LogTestGrain.cs
// and test/Grains/TestGrainInterfaces/ILogTestGrain.cs, MIT). The provider variants that need other Orleans
// storage providers are replaced by Kurrent variants.

using Orleans.Concurrency;

namespace Orleans.EventSourcing.Kurrent.Tests.Conformance;

[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.ILogTestGrain")]
public interface ILogTestGrain : IGrainWithIntegerKey
{
    [Alias("GetAGlobal")]
    Task<int> GetAGlobal();

    [Alias("GetALocal")]
    Task<int> GetALocal();

    [Alias("GetBothGlobal")]
    Task<AB> GetBothGlobal();

    [Alias("GetBothLocal")]
    Task<AB> GetBothLocal();

    [Alias("GetReservationsGlobal")]
    Task<int[]> GetReservationsGlobal();

    [Alias("GetConfirmedVersion")]
    Task<int> GetConfirmedVersion();

    [Alias("SetAGlobal")]
    Task SetAGlobal(int a);

    [Alias("SetAConditional")]
    Task<Tuple<int, bool>> SetAConditional(int a);

    [Alias("SetALocal")]
    Task SetALocal(int a);

    [Alias("IncrementALocal")]
    Task IncrementALocal();

    [Alias("IncrementAGlobal")]
    Task IncrementAGlobal();

    [Alias("SetBGlobal")]
    Task SetBGlobal(int b);

    [Alias("SetBLocal")]
    Task SetBLocal(int b);

    [Alias("AddReservationLocal")]
    Task AddReservationLocal(int x);

    [Alias("RemoveReservationLocal")]
    Task RemoveReservationLocal(int x);

    [Alias("GetEventLog")]
    Task<IReadOnlyList<object>> GetEventLog();

    [Alias("SynchronizeGlobalState")]
    Task SynchronizeGlobalState();

    [Alias("Clear")]
    Task Clear();
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.AB")]
public struct AB
{
    [Id(0)]
    public int A;

    [Id(1)]
    public int B;
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.MyGrainState")]
public sealed class MyGrainState
{
    [Id(0)]
    public int A;

    [Id(1)]
    public int B;

    [Id(2)]
    public Dictionary<string, int> Reservations = [];

    public void Apply(UpdateA x) => A = x.Val;

    public void Apply(UpdateB x) => B = x.Val;

    public void Apply(IncrementA _) => A++;

    public void Apply(AddReservation x) => Reservations[x.Val.ToString(System.Globalization.CultureInfo.InvariantCulture)] = x.Val;

    public void Apply(RemoveReservation x) => Reservations.Remove(x.Val.ToString(System.Globalization.CultureInfo.InvariantCulture));
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.UpdateA")]
public sealed class UpdateA
{
    [Id(0)]
    public int Val { get; set; }
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.UpdateB")]
public sealed class UpdateB
{
    [Id(0)]
    public int Val { get; set; }
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.IncrementA")]
public sealed class IncrementA
{
    [Id(0)]
    public int Val { get; set; }
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.AddReservation")]
public sealed class AddReservation
{
    [Id(0)]
    public int Val { get; set; }
}

[GenerateSerializer]
[Alias("Orleans.EventSourcing.Kurrent.Tests.Conformance.RemoveReservation")]
public sealed class RemoveReservation
{
    [Id(0)]
    public int Val { get; set; }
}

/// <summary>
/// Two fields that can be set or incremented and a set of reservations, each through
/// confirmed ("global") or unconfirmed ("local") events.
/// </summary>
public abstract class LogTestGrain : JournaledGrain<MyGrainState, object>, ILogTestGrain
{
    // Do not wait for the initial load
    public override Task OnActivateAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task SetAGlobal(int a)
    {
        RaiseEvent(new UpdateA { Val = a });
        await ConfirmEvents();
    }

    public async Task<Tuple<int, bool>> SetAConditional(int a)
    {
        var version = Version;
        var success = await RaiseConditionalEvent(new UpdateA { Val = a });
        return new Tuple<int, bool>(version, success);
    }

    public Task SetALocal(int a)
    {
        RaiseEvent(new UpdateA { Val = a });
        return Task.CompletedTask;
    }

    public async Task SetBGlobal(int b)
    {
        RaiseEvent(new UpdateB { Val = b });
        await ConfirmEvents();
    }

    public Task SetBLocal(int b)
    {
        RaiseEvent(new UpdateB { Val = b });
        return Task.CompletedTask;
    }

    public async Task IncrementAGlobal()
    {
        RaiseEvent(new IncrementA());
        await ConfirmEvents();
    }

    public Task IncrementALocal()
    {
        RaiseEvent(new IncrementA());
        return Task.CompletedTask;
    }

    public async Task<int> GetAGlobal()
    {
        await RefreshNow();
        return State.A;
    }

    public Task<int> GetALocal() => Task.FromResult(TentativeState.A);

    public async Task<AB> GetBothGlobal()
    {
        await RefreshNow();
        return new AB { A = State.A, B = State.B };
    }

    public Task<AB> GetBothLocal() => Task.FromResult(new AB { A = TentativeState.A, B = TentativeState.B });

    public Task AddReservationLocal(int x)
    {
        RaiseEvent(new AddReservation { Val = x });
        return Task.CompletedTask;
    }

    public Task RemoveReservationLocal(int x)
    {
        RaiseEvent(new RemoveReservation { Val = x });
        return Task.CompletedTask;
    }

    public async Task<int[]> GetReservationsGlobal()
    {
        await RefreshNow();
        return [.. State.Reservations.Values];
    }

    public Task SynchronizeGlobalState() => RefreshNow();

    public Task<int> GetConfirmedVersion() => Task.FromResult(Version);

    public Task Clear() => ClearLogAsync();

    public Task<IReadOnlyList<object>> GetEventLog() => RetrieveConfirmedEvents(0, Version);
}

public sealed class LogTestGrainNonReentrant : LogTestGrain;

[Reentrant]
public sealed class LogTestGrainReentrant : LogTestGrain;
