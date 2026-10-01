using Grpc.Core;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class KurrentRetryTests
{
    private static readonly KurrentRetryOptions fastOptions = new()
    {
        MaxAttempts = 3,
        BaseDelay = TimeSpan.FromMilliseconds(1),
        MaxDelay = TimeSpan.FromMilliseconds(2),
    };

    private static NotLeaderException NewNotLeaderException()
        => new("127.0.0.1", 2114, new RpcException(new Status(StatusCode.NotFound, "Leader info available in exception")));

    [Fact]
    public void IsTransient_MatchesOnlyLeaderElectionFailures()
    {
        Assert.True(KurrentRetry.IsTransient(NewNotLeaderException()));
        Assert.True(KurrentRetry.IsTransient(new RpcException(new Status(StatusCode.Unavailable, "unavailable"))));
        Assert.False(KurrentRetry.IsTransient(new RpcException(new Status(StatusCode.PermissionDenied, "denied"))));
        Assert.False(KurrentRetry.IsTransient(new InvalidOperationException()));
    }

    [Fact]
    public async Task ExecuteAsync_RetriesTransientFailureThenSucceeds()
    {
        var calls = 0;

        var result = await KurrentRetry.ExecuteAsync(fastOptions,
                                                     _ => calls++ < 2 ? throw NewNotLeaderException() : new ValueTask<int>(42),
                                                     TestContext.Current.CancellationToken);

        Assert.Equal(42, result);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsAfterMaxAttempts()
    {
        var calls = 0;

        await Assert.ThrowsAsync<NotLeaderException>(async () => await KurrentRetry.ExecuteAsync(fastOptions,
                                                                                                      ValueTask<int> (CancellationToken _) =>
                                                                                                      {
                                                                                                          calls++;
                                                                                                          throw NewNotLeaderException();
                                                                                                      },
                                                                                                      TestContext.Current.CancellationToken));

        Assert.Equal(fastOptions.MaxAttempts, calls);
    }

    [Fact]
    public async Task ExecuteAsync_MaxAttemptsOneDisablesRetry()
    {
        var calls = 0;
        var options = new KurrentRetryOptions { MaxAttempts = 1, BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(1) };

        await Assert.ThrowsAsync<NotLeaderException>(async () => await KurrentRetry.ExecuteAsync(options,
                                                                                                      ValueTask<int> (CancellationToken _) =>
                                                                                                      {
                                                                                                          calls++;
                                                                                                          throw NewNotLeaderException();
                                                                                                      },
                                                                                                      TestContext.Current.CancellationToken));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRetryNonTransientFailure()
    {
        var calls = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await KurrentRetry.ExecuteAsync(fastOptions,
                                                                                                            ValueTask<int> (CancellationToken _) =>
                                                                                                             {
                                                                                                                 calls++;
                                                                                                                 throw new InvalidOperationException();
                                                                                                             },
                                                                                                             TestContext.Current.CancellationToken));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_HonoursCancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await KurrentRetry.ExecuteAsync(fastOptions, _ => new ValueTask<int>(1), cts.Token));
    }

    [Fact]
    public void GetDelay_IsCappedByMaxDelay()
    {
        var options = new KurrentRetryOptions { BaseDelay = TimeSpan.FromMilliseconds(100), MaxDelay = TimeSpan.FromSeconds(1) };

        for (var attempt = 1; attempt <= 20; attempt++)
        {
            Assert.InRange(KurrentRetry.GetDelay(options, attempt), TimeSpan.Zero, options.MaxDelay);
        }
    }

    [Theory]
    [InlineData(0, 100, 1000, false)]
    [InlineData(-1, 100, 1000, false)]
    [InlineData(1, -1, 1000, false)]
    [InlineData(3, 1000, 100, false)]
    [InlineData(1, 100, 100, true)]
    [InlineData(8, 100, 5000, true)]
    public void Validator_ChecksRetryOptions(int maxAttempts, int baseDelayMs, int maxDelayMs, bool valid)
    {
        var options = new KurrentStorageOptions
        {
            ClientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false"),
            Retry = new KurrentRetryOptions
            {
                MaxAttempts = maxAttempts,
                BaseDelay = TimeSpan.FromMilliseconds(baseDelayMs),
                MaxDelay = TimeSpan.FromMilliseconds(maxDelayMs),
            },
        };
        var validator = new KurrentStorageOptionsValidator(options, "test");

        if (valid)
        {
            validator.ValidateConfiguration();
        }
        else
        {
            Assert.Throws<OrleansConfigurationException>(validator.ValidateConfiguration);
        }
    }
}
