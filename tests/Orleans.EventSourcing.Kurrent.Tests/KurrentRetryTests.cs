using System.Diagnostics.Metrics;

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class KurrentRetryTests
{
    private static readonly KurrentMetrics Metrics = new(new ServiceCollection().AddMetrics().BuildServiceProvider().GetRequiredService<IMeterFactory>());

    private static readonly Exception Unavailable = new RpcException(new Status(StatusCode.Unavailable, ""));

    // No jitter worth speaking of: delays are 1-2ms
    private static readonly KurrentRetryOptions Options = new() { BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(2) };

    private static KurrentRetryPolicy Policy(TimeSpan timeout) => new(Options, Metrics, timeout);

    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(50);

    private static readonly TimeSpan LongTimeout = TimeSpan.FromMinutes(5);

    [Fact]
    public async Task WithoutACancellableTokenRetriesStopAtTheTimeout()
    {
        var attempts = 0;
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(TimeSpan.FromMilliseconds(250)), "Test", _ =>
        {
            attempts++;
            throw Unavailable;
        }, CancellationToken.None));

        Assert.True(attempts > 1);
        Assert.Same(Unavailable, ex.InnerException);
    }

    [Fact]
    public async Task WithoutACancellableTokenAnAttemptInFlightIsCancelledAtTheTimeout()
    {
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(ShortTimeout), "Test", async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return true;
        }, CancellationToken.None));

        Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);
    }

    [Fact]
    public async Task TheTimeoutAlsoCoversCancellationReportedByGrpc()
    {
        await Assert.ThrowsAsync<TimeoutException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(ShortTimeout), "Test", async token =>
        {
            await token.WhenCanceled();
            throw new RpcException(new Status(StatusCode.Cancelled, ""));
        }, CancellationToken.None));
    }

    [Fact]
    public async Task ACallerThatCanCancelKeepsRetryingPastTheTimeout()
    {
        using var cancellation = new CancellationTokenSource();
        var attempts = 0;

        var result = await KurrentRetry.ExecuteAsync(Policy(ShortTimeout), "Test", async _ =>
        {
            if (++attempts == 1)
            {
                await Task.Delay(ShortTimeout * 3, TestContext.Current.CancellationToken);
            }

            return attempts > 3 ? true : throw Unavailable;
        }, cancellation.Token);

        Assert.True(result);
        Assert.Equal(4, attempts);
    }

    [Fact]
    public async Task ACallerThatCanCancelStopsRetriesWithCancellation()
    {
        using var cancellation = new CancellationTokenSource(ShortTimeout);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(LongTimeout), "Test", _ => throw Unavailable, cancellation.Token));
    }

    [Fact]
    public async Task DisposalSurfacesAsCancellationNotTimeout()
    {
        using var dispose = new CancellationTokenSource(ShortTimeout);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(LongTimeout), "Test", _ => throw Unavailable, CancellationToken.None, dispose.Token));
    }

    [Fact]
    public async Task DisposalDoesNotCountAsACallerThatCanCancel()
    {
        using var dispose = new CancellationTokenSource();

        await Assert.ThrowsAsync<TimeoutException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(ShortTimeout), "Test", _ => throw Unavailable, CancellationToken.None, dispose.Token));
    }

    [Fact]
    public async Task AStreamingReadIsNotCancelledByTheTimeout()
    {
        var result = await KurrentRetry.ExecuteStreamingReadAsync(Policy(ShortTimeout), "Test", async token =>
        {
            await Task.Delay(ShortTimeout * 3, token);
            return true;
        }, CancellationToken.None);

        Assert.True(result);
    }

    [Fact]
    public async Task AStreamingReadIsNotRetriedAfterTheTimeout()
    {
        var attempts = 0;

        // A zero timeout is due before the attempt even starts, so its timer cannot fire after the attempt's own delay
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => KurrentRetry.ExecuteStreamingReadAsync<bool>(Policy(TimeSpan.Zero), "Test", async token =>
        {
            attempts++;
            await Task.Delay(ShortTimeout * 3, token);
            throw Unavailable;
        }, CancellationToken.None));

        Assert.Equal(1, attempts);
        Assert.Same(Unavailable, ex.InnerException);
    }

    [Fact]
    public async Task DisposalStopsAStreamingRead()
    {
        using var dispose = new CancellationTokenSource(ShortTimeout);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => KurrentRetry.ExecuteStreamingReadAsync<bool>(Policy(LongTimeout), "Test", async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return true;
        }, dispose.Token));
    }

    [Fact]
    public async Task NonTransientFailuresAreNotRetried()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<RpcException>(() => KurrentRetry.ExecuteAsync<bool>(Policy(LongTimeout), "Test", _ =>
        {
            attempts++;
            throw new RpcException(new Status(StatusCode.PermissionDenied, ""));
        }, CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(5000)]
    public void TheDelayStaysWithinMaxDelayHoweverManyAttemptsThereHaveBeen(int attempt)
        => Assert.InRange(KurrentRetry.GetDelay(Options, attempt), TimeSpan.Zero, Options.MaxDelay);

    [Fact]
    public void PolicyTimesOutAfterTheOrleansResponseTimeout()
    {
        var services = new ServiceCollection().AddMetrics().AddSingleton<KurrentMetrics>();
        services.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = TimeSpan.FromSeconds(12));
        using var serviceProvider = services.BuildServiceProvider();

        Assert.Equal(TimeSpan.FromSeconds(12), KurrentRetryPolicy.Create(serviceProvider, new KurrentStorageOptions()).Timeout);
    }

    [Fact]
    public void AResponseTimeoutTooLongForATimerMeansNoTimeout()
    {
        var services = new ServiceCollection().AddMetrics().AddSingleton<KurrentMetrics>();
        services.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = TimeSpan.MaxValue);
        using var serviceProvider = services.BuildServiceProvider();

        Assert.Equal(Timeout.InfiniteTimeSpan, KurrentRetryPolicy.Create(serviceProvider, new KurrentStorageOptions()).Timeout);
    }
}

internal static class CancellationTokenExtensions
{
    public static Task WhenCanceled(this CancellationToken token)
    {
        var completion = new TaskCompletionSource();
        _ = token.Register(completion.SetResult);
        return completion.Task;
    }
}
