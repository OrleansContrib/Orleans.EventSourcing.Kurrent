using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Clustering;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

/// <summary>
///     Provider-specific behaviour that is stricter than the membership contract verified by <see cref="MembershipTableConformanceTests"/>.
/// </summary>
public sealed class MembershipTableTests
{
    private static KurrentMembershipTable CreateTable()
    {
        var clusterOptions = new ClusterOptions
        {
            ClusterId = "test-cluster",
            ServiceId = Guid.NewGuid().ToString("N")
        };

        // A fresh InMemoryKurrentClient keeps each test isolated.
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            Converters = { new SiloAddressJsonConverter() }
        };

        return new KurrentMembershipTable(new KurrentMembershipEventStorage(new InMemoryKurrentClient(), Options.Create(new KurrentClusteringOptions()), Options.Create(clusterOptions), jsonSerializerOptions));
    }

    private static int nextPort = 11_111;

    private static SiloAddress CreateSiloAddress()
        => SiloAddress.New(IPAddress.Loopback, Interlocked.Increment(ref nextPort), generation: 0);

    private static MembershipEntry CreateEntry(SiloAddress address)
        => new()
        {
            SiloAddress = address,
            Status = SiloStatus.Active,
            SiloName = $"Silo_{address.Endpoint.Port}",
            HostName = "localhost",
            RoleName = "test",
            ProxyPort = address.Endpoint.Port,
            StartTime = DateTime.UtcNow,
            IAmAliveTime = DateTime.UtcNow,
            SuspectTimes = []
        };

    // The contract allows a provider to ignore the row ETag when the table version protects the write; this provider validates it.
    [Fact]
    public async Task UpdateRow_IsRejected_WithStaleEtag()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var table = CreateTable();
        await table.InitializeMembershipTableAsync(true, cancellationToken);

        var address = CreateSiloAddress();
        await table.InsertRowAsync(CreateEntry(address), (await table.ReadAllAsync(cancellationToken)).Version.Next(), cancellationToken);

        var row = await table.ReadRowAsync(address, cancellationToken);
        var (entry, _) = row.Members.Single();
        entry.Status = SiloStatus.Dead;

        Assert.False(await table.UpdateRowAsync(entry, "not-a-real-etag", row.Version.Next(), cancellationToken));
    }

    // The contract allows reads to lag heartbeats; this provider makes a heartbeat visible to the next read.
    [Fact]
    public async Task UpdateIAmAlive_UpdatesLivenessTimestamp()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var table = CreateTable();
        await table.InitializeMembershipTableAsync(true, cancellationToken);

        var address = CreateSiloAddress();
        await table.InsertRowAsync(CreateEntry(address), (await table.ReadAllAsync(cancellationToken)).Version.Next(), cancellationToken);

        var newAliveTime = DateTime.UtcNow.AddMinutes(5);
        var aliveEntry = CreateEntry(address);
        aliveEntry.IAmAliveTime = newAliveTime;

        await table.UpdateIAmAliveAsync(aliveEntry, cancellationToken);

        var data = await table.ReadRowAsync(address, cancellationToken);
        Assert.Equal(newAliveTime, data.Members.Single().Item1.IAmAliveTime);
    }
}
