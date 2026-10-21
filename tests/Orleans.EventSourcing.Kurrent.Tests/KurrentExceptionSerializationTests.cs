using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

/// <summary>
///     Exceptions raised while the Kurrent cluster changes leader can reach callers in other processes
///     (for example through IManagementGrain or a failed grain call), so they must survive Orleans serialization.
/// </summary>
public sealed class KurrentExceptionSerializationTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer(b => b.AddAssembly(typeof(KurrentStorageException).Assembly))
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    [Fact]
    public void NotLeaderExceptionRoundTrips()
    {
        var copy = RoundTrip<NotLeaderException>(new NotLeaderException("localhost", 2313));

        Assert.Equal("localhost", copy.LeaderEndpoint.Host);
        Assert.Equal(2313, copy.LeaderEndpoint.Port);
    }

    [Fact]
    public void DiscoveryExceptionRoundTrips()
    {
        var original = new DiscoveryException(10);
        var copy = RoundTrip<DiscoveryException>(original);

        Assert.Equal(original.Message, copy.Message);
    }

    [Fact]
    public void RpcExceptionRoundTrips()
    {
        var copy = RoundTrip<RpcException>(new RpcException(new Status(StatusCode.Unavailable, "gone")));

        Assert.Equal(StatusCode.Unavailable, copy.StatusCode);
        Assert.Equal("gone", copy.Status.Detail);
    }

    [Fact]
    public void ConvertedNotLeaderExceptionKeepsItsInnerException()
    {
        var converted = KurrentExceptionConverter.ConvertException(new NotLeaderException("localhost", 2313));
        var copy = RoundTrip<InconsistentStateException>(converted);

        var inner = Assert.IsType<NotLeaderException>(copy.InnerException);
        Assert.Equal(2313, inner.LeaderEndpoint.Port);
    }

    private static T RoundTrip<T>(Exception exception) where T : Exception
        => Assert.IsType<T>(Serializer.Deserialize<Exception>(Serializer.SerializeToArray(exception)));
}
