#if NET10_0_OR_GREATER
using LoadTest.Client;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class LoadTestSupportTests
{
    [Fact]
    public void Parse_UsesDocumentedDefaults()
    {
        var options = LoadTestOptions.Parse([]);

        Assert.Equal(10_000, options.Grains);
        Assert.Equal(32, options.Workers);
        Assert.Equal(90, options.DurationSeconds);
        Assert.Equal(0.1, options.ReminderFraction);
        Assert.Equal(0, options.ObserveRemindersSeconds);
    }

    [Fact]
    public void Parse_AcceptsShortRun_WhenPersistentRemindersAreDisabled()
    {
        var options = LoadTestOptions.Parse(["--duration", "1", "--reminder-fraction", "0"]);

        Assert.Equal(1, options.DurationSeconds);
        Assert.Equal(0, options.ReminderFraction);
    }

    [Theory]
    [InlineData("--grains", "0")]
    [InlineData("--workers", "-1")]
    [InlineData("--duration", "0")]
    [InlineData("--reminder-fraction", "-0.1")]
    [InlineData("--reminder-fraction", "1.1")]
    [InlineData("--observe-reminders", "-1")]
    public void Parse_RejectsOutOfRangeValues(string option, string value)
        => Assert.Throws<ArgumentOutOfRangeException>(() => LoadTestOptions.Parse([option, value]));

    [Fact]
    public void Parse_RejectsUnknownOption()
        => Assert.Throws<ArgumentException>(() => LoadTestOptions.Parse(["--unknown", "1"]));

    [Fact]
    public void Parse_RejectsMissingValue()
        => Assert.Throws<ArgumentException>(() => LoadTestOptions.Parse(["--grains"]));

    [Fact]
    public void Parse_RequiresReminderObservationDuration()
        => Assert.Throws<ArgumentOutOfRangeException>(() => LoadTestOptions.Parse(["--duration", "89"]));

    [Fact]
    public void Merge_KeepsExactAggregatesAndBoundedSamples()
    {
        var first = new WorkerStats(1);
        var second = new WorkerStats(2);

        for (var i = 1; i <= 5_000; i++)
        {
            first.Record(OpType.ReminderChurn, TimeSpan.FromMilliseconds(i));
            second.Record(OpType.ReminderChurn, TimeSpan.FromMilliseconds(i + 5_000));
        }

        var summary = WorkerStats.Merge([first, second])[OpType.ReminderChurn];

        Assert.Equal(10_000, summary.Count);
        Assert.Equal(5_000.5, summary.MeanMilliseconds);
        Assert.Equal(10_000, summary.MaxMilliseconds);
        Assert.Equal(4_096, summary.SortedSamples.Length);
        Assert.InRange(summary.Percentile(0.50), 4_500, 5_500);
    }

    [Fact]
    public void RecordError_KeepsExactCountAndBoundedDiagnostics()
    {
        var statistics = new WorkerStats(1);

        for (var i = 0; i < 10; i++)
        {
            statistics.RecordError(new InvalidOperationException($"failure {i}"));
        }

        Assert.Equal(10, statistics.Errors);
        Assert.Equal(5, statistics.SampleErrors.Count);
    }
}
#endif
