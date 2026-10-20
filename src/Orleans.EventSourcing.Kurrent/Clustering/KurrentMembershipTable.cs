using Orleans.EventSourcing.Kurrent.Clustering.Events;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal sealed class KurrentMembershipTable(KurrentMembershipEventStorage storage) : IMembershipTable
{
    private volatile MembershipView current = storage.InitialView;

    public Task DeleteMembershipTableEntries(string clusterId)
     => DeleteMembershipTableEntriesAsync(clusterId, CancellationToken.None);

    public async Task DeleteMembershipTableEntriesAsync(string clusterId, CancellationToken cancellationToken)
     => current = await storage.Delete(current, clusterId, cancellationToken).ConfigureAwait(false);

    public Task InitializeMembershipTable(bool tryInitTableVersion)
     => InitializeMembershipTableAsync(tryInitTableVersion, CancellationToken.None);

    public Task InitializeMembershipTableAsync(bool tryInitTableVersion, CancellationToken cancellationToken)
     => Task.CompletedTask;

    public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
     => InsertRowAsync(entry, tableVersion, CancellationToken.None);

    public Task<bool> InsertRowAsync(MembershipEntry entry, TableVersion tableVersion, CancellationToken cancellationToken)
     => TryChange(tableVersion.VersionEtag, x => x.InsertRow(entry, tableVersion.Version), cancellationToken);

    public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
     => UpdateRowAsync(entry, etag, tableVersion, CancellationToken.None);

    public Task<bool> UpdateRowAsync(MembershipEntry entry, string etag, TableVersion tableVersion, CancellationToken cancellationToken)
     => TryChange(tableVersion.VersionEtag, x => x.UpdateRow(entry, etag, tableVersion.Version), cancellationToken);

    private async Task<bool> TryChange(string tableVersionEtag, Func<MembershipView, IEnumerable<EventBase>> action, CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current, cancellationToken).ConfigureAwait(false);
        current = refreshed;

        if (tableVersionEtag == refreshed.TableETag &&
            action(refreshed) is { } emittedEvent)
        {
            var updated = await storage.Write(refreshed, emittedEvent, cancellationToken).ConfigureAwait(true);

            if (!ReferenceEquals(updated, refreshed))
            {
                current = updated;
                return true;
            }
        }

        return false;
    }

    public Task<MembershipTableData> ReadAll()
     => ReadAllAsync(CancellationToken.None);

    public async Task<MembershipTableData> ReadAllAsync(CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current, cancellationToken).ConfigureAwait(false);
        current = refreshed;
        return refreshed.ReadAll();
    }

    public Task<MembershipTableData> ReadRow(SiloAddress key)
     => ReadRowAsync(key, CancellationToken.None);

    public async Task<MembershipTableData> ReadRowAsync(SiloAddress key, CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current, cancellationToken).ConfigureAwait(false);
        current = refreshed;
        return refreshed.ReadRow(key);
    }

    public Task UpdateIAmAlive(MembershipEntry entry)
     => UpdateIAmAliveAsync(entry, CancellationToken.None);

    public async Task UpdateIAmAliveAsync(MembershipEntry entry, CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current, cancellationToken).ConfigureAwait(false);
        current = refreshed;

        if (refreshed.UpdateIAmAlive(entry) is { } emittedEvent)
        {
            current = await storage.Write(refreshed, emittedEvent, cancellationToken).ConfigureAwait(true);
        }
    }

    public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
     => CleanupDefunctSiloEntriesAsync(beforeDate, CancellationToken.None);

    public async Task CleanupDefunctSiloEntriesAsync(DateTimeOffset beforeDate, CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current, cancellationToken).ConfigureAwait(false);
        current = await storage.Write(refreshed, refreshed.CleanUpDefunctEntries(beforeDate), cancellationToken).ConfigureAwait(true);
    }
}
