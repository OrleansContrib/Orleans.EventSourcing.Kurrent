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

    private static readonly Exception DeadlineExceeded = new RpcException(new Status(StatusCode.DeadlineExceeded, ""));

    // No jitter worth speaking of: delays are 1-2ms
    private static readonly KurrentRetryOptions Options = new() { MaxAttempts = 10, BaseDelay = TimeSpan.FromMilliseconds(1), MaxDelay = TimeSpan.FromMilliseconds(2) };

    [Theory]
    [InlineData(0, true)]
    [InlineData(19.9, true)] // 19.9s + ~2ms delay + 10s deadline still ends before 30s
    [InlineData(20, false)] // a full 10s attempt would end after 30s
    public void ARetryMustFinishWithinTheBudgetEvenIfItRunsToTheDeadline(double elapsedSeconds, bool expected)
    {
        var policy = new KurrentRetryPolicy(Options, Metrics, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10));

        Assert.Equal(expected, KurrentRetry.ShouldRetry(policy, 1, DeadlineExceeded, TimeSpan.FromSeconds(elapsedSeconds), CancellationToken.None, out _));
    }

    [Fact]
    public void WithoutAClientDeadlineOnlyTheDelayHasToFit()
    {
        var policy = new KurrentRetryPolicy(Options, Metrics, TimeSpan.FromSeconds(30), AttemptTimeout: null);

        Assert.True(KurrentRetry.ShouldRetry(policy, 1, DeadlineExceeded, TimeSpan.FromSeconds(29.9), CancellationToken.None, out _));
        Assert.False(KurrentRetry.ShouldRetry(policy, 1, DeadlineExceeded, TimeSpan.FromSeconds(30), CancellationToken.None, out _));
    }

    [Fact]
    public void AttemptsCancellationAndNonTransientFailuresStillStopRetries()
    {
        var policy = new KurrentRetryPolicy(Options, Metrics, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10));

        Assert.False(KurrentRetry.ShouldRetry(policy, Options.MaxAttempts, DeadlineExceeded, TimeSpan.Zero, CancellationToken.None, out _));
        Assert.False(KurrentRetry.ShouldRetry(policy, 1, DeadlineExceeded, TimeSpan.Zero, new CancellationToken(canceled: true), out _));
        Assert.False(KurrentRetry.ShouldRetry(policy, 1, new RpcException(new Status(StatusCode.PermissionDenied, "")), TimeSpan.Zero, CancellationToken.None, out _));
    }

    [Fact]
    public void PolicyDefaultsToTheOrleansResponseTimeoutAndTheClientDeadline()
    {
        var services = new ServiceCollection().AddMetrics().AddSingleton<KurrentMetrics>();
        services.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = TimeSpan.FromSeconds(12));
        using var serviceProvider = services.BuildServiceProvider();

        var clientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");
        clientSettings.DefaultDeadline = TimeSpan.FromSeconds(4);

        var policy = KurrentRetryPolicy.Create(serviceProvider, new KurrentStorageOptions { ClientSettings = clientSettings });
        Assert.Equal(TimeSpan.FromSeconds(12), policy.MaxDuration);
        Assert.Equal(TimeSpan.FromSeconds(4), policy.AttemptTimeout);

        var configured = KurrentRetryPolicy.Create(serviceProvider, new KurrentStorageOptions { ClientSettings = clientSettings, Retry = new KurrentRetryOptions { MaxDuration = TimeSpan.FromSeconds(7) } });
        Assert.Equal(TimeSpan.FromSeconds(7), configured.MaxDuration);
    }
}
