using System.Text.Json.Serialization;
using Orleans.Journaling;

namespace LoadTest.Grains;

/// <summary>
/// A minimal durable-state grain used to measure write throughput through the Kurrent journaling
/// storage provider. Its state is a fixed number of payload slots plus a write counter, so the state,
/// and therefore every snapshot, stays a bounded size however long the test runs. Each
/// <see cref="Write"/> call overwrites some slots and persists the changes as one journal append.
/// </summary>
[Alias("LoadTest.Grains.IDurableWriterGrain")]
public interface IDurableWriterGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Overwrites <paramref name="updates"/> of the grain's <paramref name="slots"/> payload slots, round-robin, with
    /// <paramref name="payloadBytes"/> bytes each, then writes the state; returns the number of writes persisted.
    /// </summary>
    [Alias("Write")]
    Task<long> Write(int updates, int slots, int payloadBytes);

    [Alias("GetWrites")]
    Task<long> GetWrites();

    /// <summary>Deactivates the grain, so its next call recovers the state from the journal.</summary>
    [Alias("Deactivate")]
    Task Deactivate();
}

public sealed class DurableWriterGrain : DurableGrain, IDurableWriterGrain
{
    private readonly IDurableDictionary<int, byte[]> slots;
    private readonly IDurableValue<long> writes;

    public DurableWriterGrain()
    {
        slots = StateManager.GetOrAddDictionary<int, byte[]>("slots");
        writes = StateManager.GetOrAddValue<long>("writes");
    }

    public async Task<long> Write(int updates, int slotCount, int payloadBytes)
    {
        // Each write starts where the previous one stopped, so every slot is rewritten in turn.
        var next = writes.Value * updates;
        for (var i = 0; i < updates; i++)
        {
            slots[(int)((next + i) % slotCount)] = new byte[payloadBytes];
        }

        writes.Value++;
        await WriteStateAsync();
        return writes.Value;
    }

    public Task<long> GetWrites() => Task.FromResult(writes.Value);

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
}

/// <summary>
/// Source-generated metadata for the JSON journal format (JOURNAL_FORMAT=json): the durable writer's keys and values,
/// plus the string, uint and DateTime values Orleans uses to track state names and retirements.
/// </summary>
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(uint))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(byte[]))]
[JsonSerializable(typeof(DateTime))]
public sealed partial class DurableWriterJournalJsonContext : JsonSerializerContext;
