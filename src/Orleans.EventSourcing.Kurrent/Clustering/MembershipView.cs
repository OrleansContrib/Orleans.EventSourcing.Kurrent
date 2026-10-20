using System.Collections.Immutable;
using System.Globalization;
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Clustering.Events;

namespace Orleans.EventSourcing.Kurrent.Clustering;

// The materialized membership state as of a specific stream position.
// The Orleans table ETag is derived from the domain Version, which only canonical writes (insert/update) advance,
// so heartbeats and defunct-entry cleanup leave it unchanged.
// The StreamCursor is the stream state this view was read at, which KurrentMembershipEventStorage uses
// as the read position and optimistic-concurrency token.
internal sealed record MembershipView
{
    public ImmutableDictionary<SiloAddress, ImmutableMembership> Members { get; init; } = ImmutableDictionary<SiloAddress, ImmutableMembership>.Empty;

    public int Version { get; init; }

    public StreamState StreamCursor { get; init; } = StreamState.NoStream;

    public string TableETag => Version.ToString(CultureInfo.InvariantCulture);

    private TableVersion ToTableVersion() => new(Version, TableETag);

    internal MembershipTableData ReadAll() => new([.. Members.Select(x => x.Value.ToMembershipEntry())], ToTableVersion());

    internal MembershipTableData ReadRow(SiloAddress key)
        => Members.TryGetValue(key, out var entry)
            ? new MembershipTableData(entry.ToMembershipEntry(), ToTableVersion())
            : new MembershipTableData(ToTableVersion());

    internal uint EventsSinceSnapshot { get; init; }

    internal MembershipView Apply(SiloAlive wasAliveEvent)
    => Members.TryGetValue(wasAliveEvent.SiloAddress, out var entry)
           ? this with
           {
               Members = Members.SetItem(wasAliveEvent.SiloAddress, entry with { IAmAliveTime = wasAliveEvent.IAmAliveTime}),
               EventsSinceSnapshot = EventsSinceSnapshot + 1
           }
           : this;

    // A snapshot is a full checkpoint: it replaces the members and version wholesale while keeping the current StreamCursor,
    // which storage overwrites with the actual stream position after the read/write completes.
    internal MembershipView Apply(MembershipTableSnapshot snapshotEvent)
        => this with { Members = snapshotEvent.Members,
            Version = snapshotEvent.TableVersion,
            EventsSinceSnapshot = 0 };

    internal MembershipView Apply(SiloAdded siloAdded)
       => this with
       {
           Members = Members.Add(siloAdded.Membership.SiloAddress, siloAdded.Membership),
           Version = siloAdded.TableVersion,
           EventsSinceSnapshot = EventsSinceSnapshot + 1
       };

    internal MembershipView Apply(SiloStateUpdated siloUpdated)
     => this with
     {
         Members = Members.SetItem(siloUpdated.SiloAddress, Members[siloUpdated.SiloAddress] with { SuspectTimes = siloUpdated.SuspectTimes, Status = siloUpdated.Status, ETag = siloUpdated.ETag}),
         Version = siloUpdated.TableVersion,
         EventsSinceSnapshot = EventsSinceSnapshot + 1
     };

    internal IEnumerable<EventBase> UpdateRow(MembershipEntry entry, string etag, int tableVersion)
    {
        if (!Members.TryGetValue(entry.SiloAddress, out var existingEntry)
            || existingEntry.ETag != etag)
        {
            yield break;
        }

        yield return new SiloStateUpdated(entry.SiloAddress, entry.Status, Guid.NewGuid().ToString(), entry.SuspectTimes?.ToImmutableDictionary(x => x.Item1, x => x.Item2) ?? ImmutableDictionary<SiloAddress, DateTime>.Empty, tableVersion);
    }

    internal IEnumerable<EventBase> InsertRow(MembershipEntry entry, int tableVersion)
    {
        if (Members.ContainsKey(entry.SiloAddress))
        {
            yield break;
        }

        yield return new SiloAdded(ImmutableMembership.FromMembershipEntry(entry), tableVersion);
    }

    internal IEnumerable<SiloAlive> UpdateIAmAlive(MembershipEntry updateEntry)
    {
        if (!Members.TryGetValue(updateEntry.SiloAddress, out var entry)
            || entry.IAmAliveTime > updateEntry.IAmAliveTime)
        {
            yield break;
        }

        yield return new SiloAlive(entry.SiloAddress, updateEntry.IAmAliveTime);
    }

    // Only rows the membership protocol has already declared Dead are eligible; cleanup never decides a silo's fate.
    // A row is defunct once its most recent activity (start, heartbeat or suspicion vote) is strictly before the cutoff.
    internal IEnumerable<EventBase> CleanUpDefunctEntries(DateTimeOffset beforeDate)
    {
        foreach (var item in Members)
        {
            if (item.Value.Status == SiloStatus.Dead &&
                GetLastActivityTime(item.Value) < beforeDate)
            {
                yield return new SiloDefunct(item.Key);
            }
        }
    }

    private static DateTime GetLastActivityTime(ImmutableMembership membership)
    {
        var lastActivityTicks = Math.Max(membership.IAmAliveTime.Ticks, membership.StartTime.Ticks);

        foreach (var suspectTime in membership.SuspectTimes.Values)
        {
            lastActivityTicks = Math.Max(lastActivityTicks, suspectTime.Ticks);
        }

        return new DateTime(lastActivityTicks, DateTimeKind.Utc);
    }

    internal MembershipView Apply(SiloDefunct siloDefunct)
    => this with
    {
        Members = Members.Remove(siloDefunct.SiloAddress),
        EventsSinceSnapshot = EventsSinceSnapshot + 1
    };

    internal MembershipTableSnapshot GetSnapshot()
        => new(Members, Version);
}
