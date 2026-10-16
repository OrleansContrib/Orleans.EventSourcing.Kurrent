using KurrentDB.Client;

using Orleans.Journaling;

namespace Orleans.EventSourcing.Kurrent.Journaling;

/// <summary>
///     Options for the KurrentDB Orleans Journaling storage provider.
/// </summary>
public sealed class KurrentJournalStorageOptions
{
    /// <summary>
    ///     The prefix of the default stream name. Everything before the first <c>-</c> is the KurrentDB category,
    ///     so all journals share the <c>orleansjournal</c> category.
    /// </summary>
    public const string DefaultStreamPrefix = "orleansjournal-";

    /// <summary>
    ///     The default value of <see cref="MaxEventSize"/>: 512 KiB.
    /// </summary>
    public const int DefaultMaxEventSize = 512 * 1024;

    /// <summary>
    ///     The default value of <see cref="CompactionEventCountThreshold"/>.
    /// </summary>
    public const int DefaultCompactionEventCountThreshold = 1_000;

    /// <summary>
    ///     The default value of <see cref="CompactionSizeThreshold"/>: 4 MiB.
    /// </summary>
    public const long DefaultCompactionSizeThreshold = 4L * 1024 * 1024;

    /// <summary>
    ///     The default value of <see cref="ReadPageSize"/>.
    /// </summary>
    public const int DefaultReadPageSize = 500;

    /// <summary>
    ///     The default value of <see cref="MaxMetadataConflictRetries"/>.
    /// </summary>
    public const int DefaultMaxMetadataConflictRetries = 3;

    /// <summary>
    ///     The Kurrent client settings.
    /// </summary>
    [Redact]
    public KurrentDBClientSettings ClientSettings { get; set; } = null!;

    /// <summary>
    ///     Maps a journal to the KurrentDB stream that stores it. Defaults to <see cref="DefaultGetStreamName"/>.
    /// </summary>
    /// <remarks>
    ///     The mapping must be stable: changing it for existing journals makes their data unreachable.
    ///     The returned name must not start with <c>$</c>, which KurrentDB reserves for system streams.
    /// </remarks>
    public Func<JournalId, string> GetStreamName { get; set; } = DefaultGetStreamName;

    /// <summary>
    ///     The maximum number of journal bytes written to a single KurrentDB event. Larger writes are split across
    ///     several events which are appended in one atomic operation.
    /// </summary>
    /// <remarks>
    ///     Every write must still fit within the server's <c>MaxAppendSize</c> (1 MiB by default), which limits
    ///     the total size of one append. Snapshots larger than that cannot be written.
    /// </remarks>
    public int MaxEventSize { get; set; } = DefaultMaxEventSize;

    /// <summary>
    ///     The number of events appended since the last snapshot at which compaction is requested.
    /// </summary>
    public int CompactionEventCountThreshold { get; set; } = DefaultCompactionEventCountThreshold;

    /// <summary>
    ///     The number of journal bytes appended since the last snapshot at which compaction is requested.
    /// </summary>
    /// <remarks>
    ///     Recovery reads and buffers everything written since the last snapshot, so this also bounds recovery memory.
    /// </remarks>
    public long CompactionSizeThreshold { get; set; } = DefaultCompactionSizeThreshold;

    /// <summary>
    ///     How compaction truncates the stream. Defaults to <see cref="AtomicCompactionMode.Auto"/>.
    /// </summary>
    public AtomicCompactionMode AtomicCompaction { get; set; } = AtomicCompactionMode.Auto;

    /// <summary>
    ///     The number of events requested per page when reading a journal.
    /// </summary>
    public int ReadPageSize { get; set; } = DefaultReadPageSize;

    /// <summary>
    ///     How many times an atomic compaction is retried when the stream's metadata changes concurrently,
    ///     for example because an operator edited it.
    /// </summary>
    public int MaxMetadataConflictRetries { get; set; } = DefaultMaxMetadataConflictRetries;

    /// <summary>
    ///     The default journal to stream mapping: <see cref="DefaultStreamPrefix"/> followed by <see cref="JournalId.Value"/>.
    /// </summary>
    public static string DefaultGetStreamName(JournalId journalId) => $"{DefaultStreamPrefix}{journalId.Value}";
}

/// <summary>
///     Controls whether compaction writes the snapshot and the stream's truncation point in one atomic operation.
/// </summary>
/// <remarks>
///     Compaction appends a snapshot and then sets the stream's <c>$tb</c> (truncate before) metadata so older
///     events are hidden and later scavenged. Reads are correct either way, because recovery starts from the
///     latest snapshot; atomic compaction only removes the window in which a failure leaves old events in place.
/// </remarks>
public enum AtomicCompactionMode
{
    /// <summary>
    ///     Use an atomic multi-stream append when the connected server supports it (KurrentDB 25.1+), and otherwise
    ///     append the snapshot and then update the metadata as a separate, best-effort step.
    /// </summary>
    Auto,

    /// <summary>
    ///     Always use an atomic multi-stream append; compaction fails if the server does not support it.
    /// </summary>
    Required,

    /// <summary>
    ///     Never use multi-stream appends; append the snapshot and then update the metadata as a separate step.
    /// </summary>
    Disabled,
}
