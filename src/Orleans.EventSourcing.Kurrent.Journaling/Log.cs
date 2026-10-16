using Microsoft.Extensions.Logging;

namespace Orleans.EventSourcing.Kurrent.Journaling;

internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Compaction of journal stream {StreamName} could not set truncate-before to {TruncateBefore}; older events remain until the next compaction")]
    public static partial void TruncateFailed(this ILogger logger, string streamName, ulong truncateBefore, Exception ex);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "Compaction of journal stream {StreamName} retrying after its metadata changed concurrently (attempt {Attempt})")]
    public static partial void CompactionMetadataConflict(this ILogger logger, string streamName, int attempt, Exception ex);
}
